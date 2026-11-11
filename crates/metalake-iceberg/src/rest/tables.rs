//! Tables (ICE-1), on the ref a client reads.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, State as Shared};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use metalake::{Error, Table};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::errors::Failure;
use super::namespaces::SEPARATOR;

use crate::apply::commit::Target;
use crate::apply::update::{Change, asserted_uuid, change, check_uuid};
use crate::apply::{Refused, commit, create, ddl};
use crate::caller::Caller;
use crate::load::load;
use crate::project::metadata::staged;
use crate::state::State;
use crate::vend::{Access, refresh_path};
use crate::wire::commit::{CommitTableRequest, CommitTransactionRequest, CreateTableRequest, RenameTableRequest};
use crate::wire::credentials::{LoadCredentialsResponse, StorageCredential};
use crate::wire::metadata::LoadTableResult;

/// Whether a request asks for credentials for a table's files (REST-11).
fn asks(headers: &HeaderMap) -> bool {
    headers
        .get_all("x-iceberg-access-delegation")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|mode| mode.trim().eq_ignore_ascii_case("vended-credentials"))
}

/// The credentials for a table at `location`, named `name` in `namespace` as its route names them (REST-11): none when
/// they are not asked for or its files are local; a writer's when its token says so; refused when it bears none.
async fn credentials_for(
    state: &State,
    caller: Option<Caller>,
    asked: bool,
    location: &str,
    namespace: &str,
    name: &str,
    renewable: bool,
) -> Result<Vec<StorageCredential>, Failure> {
    if !asked || !state.vendor().serves(location) {
        return Ok(Vec::new());
    }
    let access = match caller.unwrap_or(Caller::Anonymous) {
        Caller::Writer => Access::ReadWrite,
        Caller::Anonymous => return Err(Failure::credentials_refused()),
    };
    let refresh = renewable.then(|| refresh_path(namespace, name));
    state
        .vendor()
        .vend(location, access, refresh.as_deref())
        .await
        .map_err(Failure::not_minted)
}

pub(super) fn levels(namespace: &str) -> Vec<&str> {
    namespace.split(SEPARATOR).collect()
}

/// `GET /v1/namespaces/{namespace}/tables`.
pub(crate) async fn list(Shared(state): Shared<State>, Path(namespace): Path<String>) -> Result<Json<Value>, Failure> {
    let wanted = levels(&namespace);
    let names = state.main().namespaces().await?;
    if !names.iter().any(|n| n.namespace.iter().eq(&wanted)) {
        return Err(Failure::no_namespace(&wanted.join(".")));
    }
    let tables = state.main().tables().await?;
    let here = tables.iter().filter(|t| t.namespace.iter().eq(&wanted));
    let identifiers: Vec<Value> = here
        .map(|t| json!({"namespace": t.namespace, "name": t.name}))
        .collect();
    Ok(Json(json!({"identifiers": identifiers})))
}

/// A table as a client names it: its namespace's levels and its name, joined with dots.
fn qualified<S: AsRef<str>>(namespace: &[S], name: &str) -> String {
    let levels: Vec<&str> = namespace.iter().map(AsRef::as_ref).collect();
    format!("{}.{name}", levels.join("."))
}

/// The table a client names on `main`, or Iceberg's `NoSuchTableException` naming it.
async fn found(state: &State, namespace: &[&str], name: &str) -> Result<Table, Failure> {
    state.main().table(namespace, name).await.map_err(|e| match e {
        Error::NotFound(_) => Failure::no_table(&qualified(namespace, name)),
        other => other.into(),
    })
}

pub(super) fn decoded<T: DeserializeOwned>(body: &Bytes) -> Result<T, Failure> {
    let malformed = |e: serde_json::Error| Refused::Invalid(format!("a malformed request: {e}"));
    Ok(serde_json::from_slice(body).map_err(malformed)?)
}

/// `POST /v1/namespaces/{namespace}/tables`: `createTable` (REST-7). A staged one writes nothing: what its client commits
/// later brings the definition back whole.
pub(crate) async fn create(
    Shared(state): Shared<State>,
    Path(namespace): Path<String>,
    caller: Option<Extension<Caller>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let (caller, asked) = (caller.map(|Extension(c)| c), asks(&headers));
    let wanted = levels(&namespace);
    let request: CreateTableRequest = decoded(&body)?;
    let name = request.name.clone();
    let namespaces = state.main().namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace.iter().eq(&wanted)) {
        return Err(Failure::no_namespace(&wanted.join(".")));
    }
    if state.main().table(&wanted, &name).await.is_ok() {
        return Err(Failure::already_exists(&qualified(&wanted, &name)));
    }
    let new = create::requested(&state, &wanted, request)?;
    if new.stage {
        let described = create::described(&new.definition);
        let metadata = staged(new.table_id.to_string(), described);
        // A staged table's files are written before its commit: its client is given credentials for them now. They
        // name no endpoint to refresh them, as `loadCredentials` finds only a table committed.
        let storage_credentials =
            credentials_for(&state, caller, asked, &metadata.location, &namespace, &name, false).await?;
        return Ok(Json(LoadTableResult {
            metadata_location: None,
            metadata,
            config: BTreeMap::new(),
            storage_credentials,
        }));
    }
    // Minted before the commit: a create that cannot be given the credentials it asked for leaves no table behind, so
    // that the client's retry finds none.
    let location = new.definition["create"]["location"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let storage_credentials = credentials_for(&state, caller, asked, &location, &namespace, &name, true).await?;
    let table = create::create(&state, &wanted, &name, new).await.map_err(|e| {
        let failure = Failure::from(e);
        if failure.is_conflict() {
            Failure::already_exists(&qualified(&wanted, &name))
        } else {
            failure
        }
    })?;
    let mut loaded = load(&state, &table).await?;
    loaded.storage_credentials = storage_credentials;
    Ok(Json(loaded))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}`.
pub(crate) async fn get(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
    caller: Option<Extension<Caller>>,
    headers: HeaderMap,
) -> Result<Json<LoadTableResult>, Failure> {
    let table = found(&state, &levels(&namespace), &name).await?;
    let mut loaded = load(&state, &table).await?;
    let caller = caller.map(|Extension(c)| c);
    loaded.storage_credentials = credentials_for(
        &state,
        caller,
        asks(&headers),
        table.location(),
        &namespace,
        &name,
        true,
    )
    .await?;
    Ok(Json(loaded))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}/credentials`: `loadCredentials`, fresh credentials for a table's files,
/// as a client asks when its own expire (REST-11). Asking is what it is: none for a table whose files are local.
pub(crate) async fn credentials(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
    caller: Option<Extension<Caller>>,
) -> Result<Json<LoadCredentialsResponse>, Failure> {
    let table = found(&state, &levels(&namespace), &name).await?;
    let caller = caller.map(|Extension(c)| c);
    let storage_credentials = credentials_for(&state, caller, true, table.location(), &namespace, &name, true).await?;
    Ok(Json(LoadCredentialsResponse { storage_credentials }))
}

/// What a commit asks of one table. Refused, it answers first that the table is not the one the client asserts, if so
/// (REST-3): reading the table is left to that case alone.
async fn checked(
    state: &State,
    namespace: &[String],
    name: &str,
    request: CommitTableRequest,
) -> Result<Change, Failure> {
    let asserted = asserted_uuid(&request);
    let refused = match change(request) {
        Ok(change) => return Ok(change),
        Err(refused) => refused,
    };
    if asserted.is_some() {
        let levels: Vec<&str> = namespace.iter().map(String::as_str).collect();
        if let Ok(table) = state.main().table(&levels, name).await {
            check_uuid(asserted.as_deref(), &table.id().to_string())?;
        }
    }
    Err(refused.into())
}

/// `POST /v1/namespaces/{namespace}/tables/{table}`: `updateTable`, one commit (ICE-6).
pub(crate) async fn update(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let request: CommitTableRequest = decoded(&body)?;
    let namespace: Vec<String> = levels(&namespace).iter().map(|l| l.to_string()).collect();
    let change = checked(&state, &namespace, &name, request).await?;
    let target = Target {
        namespace,
        name,
        change,
    };
    let tables = commit::apply(&state, std::slice::from_ref(&target)).await?;
    Ok(Json(load(&state, &tables[0]).await?))
}

/// `POST /v1/transactions/commit`: one commit of several tables, on one ref (REST-9).
pub(crate) async fn commit_transaction(Shared(state): Shared<State>, body: Bytes) -> Result<StatusCode, Failure> {
    let request: CommitTransactionRequest = decoded(&body)?;
    let mut targets = Vec::with_capacity(request.table_changes.len());
    for mut entry in request.table_changes {
        let identifier = entry.identifier.take();
        let identifier = identifier.ok_or_else(|| Refused::Invalid("a table change names no table".into()))?;
        let change = checked(&state, &identifier.namespace, &identifier.name, entry).await?;
        targets.push(Target {
            namespace: identifier.namespace,
            name: identifier.name,
            change,
        });
    }
    commit::apply(&state, &targets).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `HEAD /v1/namespaces/{namespace}/tables/{table}`, which generates nothing, unlike a load.
pub(crate) async fn exists(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    found(&state, &levels(&namespace), &name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /v1/namespaces/{namespace}/tables/{table}`. `purgeRequested` changes nothing (REST-8), so it is not read.
pub(crate) async fn drop(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    let table = found(&state, &levels(&namespace), &name).await?;
    ddl::drop_table(&state, &table).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename`.
pub(crate) async fn rename(Shared(state): Shared<State>, body: Bytes) -> Result<StatusCode, Failure> {
    let request: RenameTableRequest = decoded(&body)?;
    let source: Vec<&str> = request.source.namespace.iter().map(String::as_str).collect();
    let table = found(&state, &source, &request.source.name).await?;
    let destination = &request.destination;
    let namespaces = state.main().namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace == destination.namespace) {
        return Err(Failure::no_namespace(&destination.namespace.join(".")));
    }
    ddl::rename_table(&state, &table, &destination.namespace, &destination.name)
        .await
        .map_err(|e| match Failure::from(e) {
            failure if failure.is_conflict() => {
                Failure::already_exists(&qualified(&destination.namespace, &destination.name))
            }
            failure => failure,
        })?;
    Ok(StatusCode::NO_CONTENT)
}
