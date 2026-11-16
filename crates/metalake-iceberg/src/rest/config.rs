//! `GET /v1/config` (REST-1): what a client learns on connecting, above all which routes exist, so that it calls no other.

use axum::Json;
use axum::extract::{Query, State as Shared};
use metalake::Error;
use metalake::path::{DEFAULT_CATALOG, MAIN};
use serde::Deserialize;
use serde_json::{Value, json};

use super::errors::Failure;
use super::prefix::{prefix_of, session_prefix};
use crate::state::State;

/// What this server serves, as the OpenAPI spells a route. `{prefix}` is a ref's, when the client asks for one.
const ENDPOINTS: [&str; 15] = [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/transactions/commit",
];

#[derive(Deserialize)]
pub(crate) struct Asked {
    warehouse: Option<String>,
}

/// What a warehouse starts with that asks for a session rather than a ref: `session:<id>`.
pub(crate) const SESSION: &str = "session:";

/// Whether a warehouse a client asks for is a place, as Iceberg's Java catalogs mostly mean it (`s3://b/w`, `/tmp/w`),
/// rather than a ref's name: such a client is on `main`, as before refs were served.
fn is_a_location(warehouse: &str) -> bool {
    warehouse.contains("://") || warehouse.starts_with('/')
}

/// What a client is told to connect to the warehouse it asks for (I7): `catalog`, `catalog@ref` or
/// `catalog@session:<id>`, by its prefix in `overrides`, so that the client's own configuration does not override it
/// (I6). None asked for, or a location, is the catalog `default` on `main`, under no prefix; a catalog the ref has not,
/// or a ref the lake has not, is refused rather than read as another.
pub(crate) async fn config(Shared(state): Shared<State>, Query(asked): Query<Asked>) -> Result<Json<Value>, Failure> {
    let asked = asked
        .warehouse
        .as_deref()
        .filter(|w| !w.is_empty() && !is_a_location(w));
    let overrides = match asked {
        None => json!({}),
        Some(warehouse) => {
            let (catalog, on) = match warehouse.split_once('@') {
                Some((catalog, on)) => (catalog, on),
                // A session asked for as before catalogs (I6): in the catalog `default`.
                None if warehouse.starts_with(SESSION) => ("", warehouse),
                None => (warehouse, MAIN),
            };
            let catalog = if catalog.is_empty() { DEFAULT_CATALOG } else { catalog };
            // A session, which follows the ref it says it is on; one that has said none is not known here yet.
            if let Some(id) = on.strip_prefix(SESSION) {
                let Some(name) = state.session(id) else {
                    return Err(Failure::no_session(id, axum::http::StatusCode::BAD_REQUEST));
                };
                has_catalog(&state, &name, catalog).await?;
                json!({"prefix": session_prefix(id, catalog)})
            } else {
                has_catalog(&state, on, catalog).await?;
                match (on, catalog) {
                    (MAIN, DEFAULT_CATALOG) => json!({}),
                    _ => json!({"prefix": prefix_of(on, catalog)}),
                }
            }
        }
    };
    Ok(Json(
        json!({"defaults": {}, "overrides": overrides, "endpoints": ENDPOINTS}),
    ))
}

/// Refuses a ref the lake has not, or a catalog the ref has not.
async fn has_catalog(state: &State, on: &str, catalog: &str) -> Result<(), Failure> {
    let found = match state.named(on).await {
        Ok(found) => found,
        Err(Error::NotFound(_)) => return Err(Failure::no_warehouse(on)),
        Err(e) => return Err(e.into()),
    };
    let namespaces = found.namespaces().await?;
    if namespaces.iter().any(|n| n.namespace.len() == 1 && n.namespace[0] == catalog) {
        return Ok(());
    }
    Err(Failure::no_catalog(catalog, on))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest1_config_endpoints_are_spelled_with_their_prefix() {
        assert!(ENDPOINTS.iter().all(|e| e.contains(" /v1/{prefix}/")));
    }
}
