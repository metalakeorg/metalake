//! Tables (ICE-1), on the ref a client reads.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State as Shared};
use serde_json::{Value, json};

use super::errors::Failure;
use super::namespaces::SEPARATOR;
use crate::apply::update::change;
use crate::apply::{Refused, commit};
use crate::load::load;
use crate::state::State;
use crate::wire::commit::CommitTableRequest;
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
    let request: CommitTableRequest =
        serde_json::from_slice(&body).map_err(|e| Refused::Invalid(format!("a malformed commit: {e}")))?;
    let change = change(request)?;
    let table = commit::apply(&state, &levels(&namespace), &name, &change).await?;
    Ok(Json(load(&state, &table).await?))
}
