//! `GET /v1/config` (REST-1): what a client learns on connecting, above all which routes exist, so that it calls no other.

use axum::Json;
use axum::extract::{Query, State as Shared};
use metalake::Error;
use serde::Deserialize;
use serde_json::{Value, json};

use super::errors::Failure;
use super::prefix::prefix_of;
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

/// Whether a warehouse a client asks for is a place, as Iceberg's Java catalogs mostly mean it (`s3://b/w`, `/tmp/w`),
/// rather than a ref's name: such a client is on `main`, as before refs were served.
fn is_a_location(warehouse: &str) -> bool {
    warehouse.contains("://") || warehouse.starts_with('/')
}

/// What a client is told to connect to the warehouse it asks for: the ref of that name, by its prefix in `overrides`,
/// so that the client's own configuration does not override it (I6). None asked for, or a location that names no
/// ref, `main`, under no prefix; any other name that is no ref is refused, rather than read as `main`.
pub(crate) async fn config(Shared(state): Shared<State>, Query(asked): Query<Asked>) -> Result<Json<Value>, Failure> {
    let overrides = match asked.warehouse.as_deref().filter(|w| !w.is_empty()) {
        None => json!({}),
        Some(name) => match state.named(name).await {
            Ok(_) => json!({"prefix": prefix_of(name)}),
            Err(Error::NotFound(_)) if is_a_location(name) => json!({}),
            Err(Error::NotFound(_)) => return Err(Failure::no_warehouse(name)),
            Err(e) => return Err(e.into()),
        },
    };
    Ok(Json(
        json!({"defaults": {}, "overrides": overrides, "endpoints": ENDPOINTS}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest1_config_endpoints_are_spelled_with_their_prefix() {
        assert!(ENDPOINTS.iter().all(|e| e.contains(" /v1/{prefix}/")));
    }
}
