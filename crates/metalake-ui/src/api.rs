//! The UI's API, under `/api` (ADR 0007 §4.1). The guard's token and origin layers wrap this whole router, its fallback
//! included, so that no path under `/api`, of today or to come, answers without them; and a path that does not exist
//! answers 401 to a request without the token, which so learns nothing of which paths do.

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::get;

/// The version of the API's contract, which a frontend of another build compares with its own.
pub const VERSION: u32 = 1;

/// The paths under `/api`.
pub(crate) fn router() -> Router {
    Router::new().route("/v1/version", get(version)).fallback(no_route)
}

/// `GET /api/v1/version`: the API's version.
async fn version() -> Response {
    json(StatusCode::OK, format!(r#"{{"api":{VERSION}}}"#))
}

async fn no_route() -> Response {
    json(
        StatusCode::NOT_FOUND,
        r#"{"kind":"usage","message":"no such path in MetaLake's UI API"}"#.to_owned(),
    )
}

fn json(status: StatusCode, body: String) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
