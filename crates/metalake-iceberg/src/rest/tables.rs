//! Tables (ICE-1), on the ref a client reads.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, State as Shared};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use metalake::path::{DEFAULT_CATALOG, MAIN};
use metalake::{Error, Ref, Table};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::errors::Failure;
use super::namespaces::SEPARATOR;
use super::prefix::{Catalog, On, prefix_of};

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

/// The endpoint a client renews a table's credentials at: under the prefix of the ref and the catalog it is connected
/// to, so that it renews them for the same table (I6, I7).
fn renewal(on: &Ref, catalog: &Catalog, namespace: &str, name: &str) -> String {
    match (on.name(), catalog.0.as_str()) {
        (MAIN, DEFAULT_CATALOG) => refresh_path(namespace, name),
        (other, catalog) => format!(
            "v1/{}/{}",
            prefix_of(other, catalog),
            &refresh_path(namespace, name)["v1/".len()..]
        ),
    }
}

/// The credentials for a table at `location` (REST-11): none when they are not asked for or its files are local; a
/// writer's when its token says so; refused when it bears none. `refresh` is the endpoint that renews them, if any.
async fn credentials_for(
    state: &State,
    caller: Option<Caller>,
    asked: bool,
    location: &str,
    refresh: Option<String>,
) -> Result<Vec<StorageCredential>, Failure> {
    if !asked || !state.vendor().serves(location) {
        return Ok(Vec::new());
    }
    let access = match caller.unwrap_or(Caller::Anonymous) {
        Caller::Writer => Access::ReadWrite,
        Caller::Anonymous => return Err(Failure::credentials_refused()),
    };
    state
        .vendor()
        .vend(location, access, refresh.as_deref())
        .await
        .map_err(Failure::not_minted)
}

pub(super) fn levels(namespace: &str) -> Vec<&str> {
    namespace.split(SEPARATOR).collect()
}

/// The lake's namespace of one a client names on a route: under the catalog it sees (I7).
pub(super) fn inside(catalog: &Catalog, namespace: &str) -> Vec<String> {
    catalog.inside(&levels(namespace))
}

/// A namespace of the lake as the client named it: past its catalog, which the client does not say (I7). What a client
/// is told names it as it asked.
pub(super) fn shown<'a>(namespace: &'a [&'a str]) -> &'a [&'a str] {
    namespace.get(1..).unwrap_or(namespace)
}

/// Levels, borrowed.
pub(super) fn strs(levels: &[String]) -> Vec<&str> {
    levels.iter().map(String::as_str).collect()
}

/// `GET /v1/namespaces/{namespace}/tables`.
pub(crate) async fn list(On(on, catalog): On, Path(namespace): Path<String>) -> Result<Json<Value>, Failure> {
    let full = inside(&catalog, &namespace);
    let wanted = strs(&full);
    let names = on.namespaces().await?;
    if !names.iter().any(|n| n.namespace.iter().eq(&wanted)) {
        return Err(Failure::no_namespace(&levels(&namespace).join(".")));
    }
    let tables = on.tables().await?;
    let here = tables.iter().filter(|t| t.namespace.iter().eq(&wanted));
    let identifiers: Vec<Value> = here
        .map(|t| json!({"namespace": catalog.outside(&t.namespace).unwrap_or(&t.namespace), "name": t.name}))
        .collect();
    Ok(Json(json!({"identifiers": identifiers})))
}

/// A table as a client names it: its namespace's levels and its name, joined with dots.
fn qualified<S: AsRef<str>>(namespace: &[S], name: &str) -> String {
    let levels: Vec<&str> = namespace.iter().map(AsRef::as_ref).collect();
    format!("{}.{name}", levels.join("."))
}

/// The table a client names on the ref, or Iceberg's `NoSuchTableException` naming it.
async fn found(on: &Ref, namespace: &[&str], name: &str) -> Result<Table, Failure> {
    on.table(namespace, name).await.map_err(|e| match e {
        Error::NotFound(_) => Failure::no_table(&qualified(shown(namespace), name)),
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
    On(on, catalog): On,
    Path(namespace): Path<String>,
    caller: Option<Extension<Caller>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let (caller, asked) = (caller.map(|Extension(c)| c), asks(&headers));
    let full = inside(&catalog, &namespace);
    let wanted = strs(&full);
    let request: CreateTableRequest = decoded(&body)?;
    let name = request.name.clone();
    let namespaces = on.namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace.iter().eq(&wanted)) {
        return Err(Failure::no_namespace(&shown(&wanted).join(".")));
    }
    if on.table(&wanted, &name).await.is_ok() {
        return Err(Failure::already_exists(&qualified(shown(&wanted), &name)));
    }
    let new = create::requested(&state, &wanted, request)?;
    if new.stage {
        let described = create::described(&new.definition);
        let metadata = staged(new.table_id.to_string(), described);
        // A staged table's files are written before its commit: its client is given credentials for them now. They
        // name no endpoint to refresh them, as `loadCredentials` finds only a table committed.
        let storage_credentials = credentials_for(&state, caller, asked, &metadata.location, None).await?;
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
    let storage_credentials = credentials_for(
        &state,
        caller,
        asked,
        &location,
        Some(renewal(&on, &catalog, &namespace, &name)),
    )
    .await?;
    let table = create::create(&state, &on, &wanted, &name, new).await.map_err(|e| {
        let failure = Failure::from(e);
        if failure.is_conflict() {
            Failure::already_exists(&qualified(shown(&wanted), &name))
        } else {
            failure
        }
    })?;
    let mut loaded = load(&state, &on, &table).await?;
    loaded.storage_credentials = storage_credentials;
    Ok(Json(loaded))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}`.
pub(crate) async fn get(
    Shared(state): Shared<State>,
    On(on, catalog): On,
    Path((namespace, name)): Path<(String, String)>,
    caller: Option<Extension<Caller>>,
    headers: HeaderMap,
) -> Result<Json<LoadTableResult>, Failure> {
    let full = inside(&catalog, &namespace);
    let table = found(&on, &strs(&full), &name).await?;
    let mut loaded = load(&state, &on, &table).await?;
    let caller = caller.map(|Extension(c)| c);
    loaded.storage_credentials = credentials_for(
        &state,
        caller,
        asks(&headers),
        table.location(),
        Some(renewal(&on, &catalog, &namespace, &name)),
    )
    .await?;
    Ok(Json(loaded))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}/credentials`: `loadCredentials`, fresh credentials for a table's files,
/// as a client asks when its own expire (REST-11). Asking is what it is: none for a table whose files are local.
pub(crate) async fn credentials(
    Shared(state): Shared<State>,
    On(on, catalog): On,
    Path((namespace, name)): Path<(String, String)>,
    caller: Option<Extension<Caller>>,
) -> Result<Json<LoadCredentialsResponse>, Failure> {
    let full = inside(&catalog, &namespace);
    let table = found(&on, &strs(&full), &name).await?;
    let caller = caller.map(|Extension(c)| c);
    let storage_credentials = credentials_for(
        &state,
        caller,
        true,
        table.location(),
        Some(renewal(&on, &catalog, &namespace, &name)),
    )
    .await?;
    Ok(Json(LoadCredentialsResponse { storage_credentials }))
}

/// What a commit asks of one table. Refused, it answers first that the table is not the one the client asserts, if so
/// (REST-3): reading the table is left to that case alone.
async fn checked(on: &Ref, namespace: &[String], name: &str, request: CommitTableRequest) -> Result<Change, Failure> {
    let asserted = asserted_uuid(&request);
    let refused = match change(request) {
        Ok(change) => return Ok(change),
        Err(refused) => refused,
    };
    if asserted.is_some() {
        let levels: Vec<&str> = namespace.iter().map(String::as_str).collect();
        if let Ok(table) = on.table(&levels, name).await {
            check_uuid(asserted.as_deref(), &table.id().to_string())?;
        }
    }
    Err(refused.into())
}

/// `POST /v1/namespaces/{namespace}/tables/{table}`: `updateTable`, one commit (ICE-6).
pub(crate) async fn update(
    Shared(state): Shared<State>,
    On(on, catalog): On,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let request: CommitTableRequest = decoded(&body)?;
    let namespace: Vec<String> = inside(&catalog, &namespace);
    let change = checked(&on, &namespace, &name, request).await?;
    let target = Target {
        namespace,
        name,
        change,
    };
    let tables = commit::apply(&state, &on, std::slice::from_ref(&target)).await?;
    Ok(Json(load(&state, &on, &tables[0]).await?))
}

/// `POST /v1/transactions/commit`: one commit of several tables, on one ref (REST-9).
pub(crate) async fn commit_transaction(
    Shared(state): Shared<State>,
    On(on, catalog): On,
    body: Bytes,
) -> Result<StatusCode, Failure> {
    let request: CommitTransactionRequest = decoded(&body)?;
    let mut targets = Vec::with_capacity(request.table_changes.len());
    for mut entry in request.table_changes {
        let identifier = entry.identifier.take();
        let identifier = identifier.ok_or_else(|| Refused::Invalid("a table change names no table".into()))?;
        let namespace = catalog.inside(&identifier.namespace);
        let change = checked(&on, &namespace, &identifier.name, entry).await?;
        targets.push(Target {
            namespace,
            name: identifier.name,
            change,
        });
    }
    commit::apply(&state, &on, &targets).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `HEAD /v1/namespaces/{namespace}/tables/{table}`, which generates nothing, unlike a load.
pub(crate) async fn exists(
    On(on, catalog): On,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    found(&on, &strs(&inside(&catalog, &namespace)), &name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /v1/namespaces/{namespace}/tables/{table}`. `purgeRequested` changes nothing (REST-8), so it is not read.
pub(crate) async fn drop(
    On(on, catalog): On,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    let table = found(&on, &strs(&inside(&catalog, &namespace)), &name).await?;
    ddl::drop_table(&on, &table).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename`.
pub(crate) async fn rename(On(on, catalog): On, body: Bytes) -> Result<StatusCode, Failure> {
    let request: RenameTableRequest = decoded(&body)?;
    let source = catalog.inside(&request.source.namespace);
    let table = found(&on, &strs(&source), &request.source.name).await?;
    let destination = &request.destination;
    let into = catalog.inside(&destination.namespace);
    let namespaces = on.namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace == into) {
        return Err(Failure::no_namespace(&destination.namespace.join(".")));
    }
    ddl::rename_table(&on, &table, &into, &destination.name)
        .await
        .map_err(|e| match Failure::from(e) {
            failure if failure.is_conflict() => {
                Failure::already_exists(&qualified(&destination.namespace, &destination.name))
            }
            failure => failure,
        })?;
    Ok(StatusCode::NO_CONTENT)
}
