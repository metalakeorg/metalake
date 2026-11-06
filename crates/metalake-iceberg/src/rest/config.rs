//! `GET /v1/config` (REST-1): what a client learns on connecting, above all which routes exist, so that it calls no other.

use axum::Json;
use serde_json::{Value, json};

/// What this server serves, as the OpenAPI spells a route. `{prefix}` is part of the spelling, and this server uses none.
const ENDPOINTS: [&str; 3] = [
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
];

pub(crate) async fn config() -> Json<Value> {
    Json(json!({"defaults": {}, "overrides": {}, "endpoints": ENDPOINTS}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rest1_config_endpoints() {
        let Json(body) = config().await;
        assert_eq!(body["endpoints"], json!(ENDPOINTS));
        assert_eq!(body["defaults"], json!({}));
        assert_eq!(body["overrides"], json!({}));
    }
}
