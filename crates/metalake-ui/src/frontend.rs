//! What the router answers for every path that is not the API's (ADR 0007 §4.3): the frontend's files, and its page for
//! any route of the frontend's own router.
//!
//! | Request | Answer |
//! |---|---|
//! | under `/api` | 404 in JSON: the API's own paths are its router's, and none falls back to the page |
//! | not `GET` or `HEAD` | 405 |
//! | the frontend not built | 503, a page that says how to build it, for a route; 404 for a file |
//! | a route: no `.` in its last segment | the page |
//! | a file that is there | the file, with its type |
//! | any other | 404 |

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;

use crate::assets::{File, Files, INDEX};

/// The page served when the frontend was not built before the crate was compiled.
const UNBUILT: &str = include_str!("unbuilt.html");

/// The answer to a path under `/api` that no route of the API has, in the shape of the client's errors.
const NO_ROUTE: &str = r#"{"kind":"usage","message":"no such path in MetaLake's UI API"}"#;

/// The path the API's routes are under.
const API: &str = "/api";

/// Answers `uri` from `files`.
pub(crate) async fn serve(State(files): State<Arc<dyn Files>>, method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path == API || path.starts_with("/api/") {
        return answer(StatusCode::NOT_FOUND, "application/json", NO_ROUTE.as_bytes().to_vec());
    }
    if method != Method::GET && method != Method::HEAD {
        let mut response = answer(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain; charset=utf-8",
            b"".to_vec(),
        );
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }
    let relative = path.trim_start_matches('/');
    // Never a path out of `web/dist/`, though only the files embedded can be named.
    if relative.split('/').any(|segment| segment == ".." || segment == ".") {
        return not_found();
    }
    let route = !relative.rsplit('/').next().is_some_and(|last| last.contains('.'));
    if route {
        return match files.get(INDEX) {
            Some(page) => file(page),
            None => answer(
                StatusCode::SERVICE_UNAVAILABLE,
                "text/html; charset=utf-8",
                UNBUILT.as_bytes().to_vec(),
            ),
        };
    }
    match files.get(relative) {
        Some(found) => file(found),
        None => not_found(),
    }
}

/// A file, with its type; UTF-8 named where the type is text.
fn file(file: File) -> Response {
    let media_type = match file.media_type.as_ref() {
        text if text.starts_with("text/") && !text.contains("charset") => format!("{text}; charset=utf-8"),
        other => other.to_owned(),
    };
    answer(StatusCode::OK, &media_type, file.bytes.into_owned())
}

fn not_found() -> Response {
    answer(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        b"not found".to_vec(),
    )
}

fn answer(status: StatusCode, media_type: &str, body: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    if let Ok(value) = HeaderValue::from_str(media_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}
