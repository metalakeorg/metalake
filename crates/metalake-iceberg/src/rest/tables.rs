//! Tables (ICE-1), on the ref a client reads.

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State as Shared};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::errors::Failure;
use super::namespaces::SEPARATOR;
use crate::apply::update::change;
use crate::apply::{Refused, commit, create};
use crate::load::load;
use crate::project::metadata::staged;
use crate::state::State;
use crate::wire::commit::{CommitTableRequest, CreateTableRequest};
use crate::wire::metadata::LoadTableResult;

fn levels(namespace: &str) -> Vec<&str> {
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

fn decoded<T: DeserializeOwned>(body: &Bytes) -> Result<T, Failure> {
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

/// `POST /v1/namespaces/{namespace}/tables/{table}`: `updateTable`, one commit (ICE-6). The body is decoded here since
/// Iceberg expects a 400 for a malformed request, and axum's extractor answers 422.
pub(crate) async fn update(
    Shared(state): Shared<State>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<LoadTableResult>, Failure> {
    let request: CommitTableRequest = decoded(&body)?;
    let change = change(request)?;
    let table = commit::apply(&state, &levels(&namespace), &name, &change).await?;
    Ok(Json(load(&state, &table).await?))
}
