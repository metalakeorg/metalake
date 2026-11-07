//! Tables (ICE-1), on the ref a client reads.

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State as Shared};
use axum::http::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::errors::Failure;
use super::namespaces::SEPARATOR;
use crate::apply::commit::Target;
use crate::apply::update::change;
use crate::apply::{Refused, commit, create, ddl};
use crate::load::load;
use crate::project::metadata::staged;
use crate::state::State;
use crate::wire::commit::{CommitTableRequest, CommitTransactionRequest, CreateTableRequest, RenameTableRequest};
use crate::wire::metadata::LoadTableResult;

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

pub(super) fn decoded<T: DeserializeOwned>(body: &Bytes) -> Result<T, Failure> {
    let malformed = |e: serde_json::Error| Refused::Invalid(format!("a malformed request: {e}"));
    Ok(serde_json::from_slice(body).map_err(malformed)?)
}

/// `POST /v1/namespaces/{namespace}/tables`: `createTable` (REST-7). A staged one writes nothing: what its client commits
/// later brings the definition back whole.
pub(crate) async fn create(
    Shared(state): Shared<State>,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let wanted = levels(&namespace);
    eprintln!("TRACE create: {}", String::from_utf8_lossy(&body));
    let request: CreateTableRequest = decoded(&body)?;
    let name = request.name.clone();
    let namespaces = state.main().namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace.iter().eq(&wanted)) {
        return Err(Failure::no_namespace(&wanted.join(".")));
    }
    if state.main().table(&wanted, &name).await.is_ok() {
        return Err(Failure::already_exists(&name));
    }
    let new = create::requested(&state, &wanted, request)?;
    if new.stage {
        let described = create::described(&new.definition);
        return Ok(Json(LoadTableResult {
            metadata_location: None,
            metadata: staged(new.table_id.to_string(), described),
            config: BTreeMap::new(),
        }));
    }
    let table = create::create(&state, &wanted, &name, new).await.map_err(|e| {
        let failure = Failure::from(e);
        if failure.is_conflict() {
            Failure::already_exists(&name)
        } else {
            failure
        }
    })?;
    Ok(Json(load(&state, &table).await?))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}`.
pub(crate) async fn get(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<LoadTableResult>, Failure> {
    let wanted = levels(&namespace);
    let table = state.main().table(&wanted, &name).await?;
    Ok(Json(load(&state, &table).await?))
}

/// `POST /v1/namespaces/{namespace}/tables/{table}`: `updateTable`, one commit (ICE-6).
pub(crate) async fn update(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    eprintln!("TRACE update {name}: {}", String::from_utf8_lossy(&body));
    let request: CommitTableRequest = decoded(&body)?;
    let target = Target {
        namespace: levels(&namespace).iter().map(|l| l.to_string()).collect(),
        name,
        change: change(request)?,
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
        targets.push(Target {
            namespace: identifier.namespace,
            name: identifier.name,
            change: change(entry)?,
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
    state.main().table(&levels(&namespace), &name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /v1/namespaces/{namespace}/tables/{table}`. `purgeRequested` changes nothing (REST-8), so it is not read.
pub(crate) async fn drop(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    let table = state.main().table(&levels(&namespace), &name).await?;
    ddl::drop_table(&state, &table).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename`.
pub(crate) async fn rename(Shared(state): Shared<State>, body: Bytes) -> Result<StatusCode, Failure> {
    let request: RenameTableRequest = decoded(&body)?;
    let source: Vec<&str> = request.source.namespace.iter().map(String::as_str).collect();
    let table = state.main().table(&source, &request.source.name).await?;
    let destination = &request.destination;
    let namespaces = state.main().namespaces().await?;
    if !namespaces.iter().any(|n| n.namespace == destination.namespace) {
        return Err(Failure::no_namespace(&destination.namespace.join(".")));
    }
    ddl::rename_table(&state, &table, &destination.namespace, &destination.name)
        .await
        .map_err(|e| match Failure::from(e) {
            failure if failure.is_conflict() => Failure::already_exists(&destination.name),
            failure => failure,
        })?;
    Ok(StatusCode::NO_CONTENT)
}
