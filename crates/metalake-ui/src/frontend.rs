//! What the router answers for every path that is not the API's (ADR 0007 §4.3): the frontend's files, and its page for
//! any route of the frontend's own router.
//!
//! | Request | Answer |
//! |---|---|
//! | under `/api` | 404 in JSON: the API's own paths are its router's, and none falls back to the page |
//! | not `GET` or `HEAD` | 405 |
//! | the frontend not built | 503, a page that says how to build it, for a route; 404 for a file |
//! | a route: no `.` in its last segment | the page |
//! | a file that is there, but a compressed copy by its own name | the file |
//! | any other | 404 |
//!
//! A file is answered with its type; cached forever under `assets/`, whose names Vite makes of their hashes, and
//! revalidated otherwise; as the compressed copy `pnpm build` wrote beside it that the request accepts best; with an
//! entity tag of what is sent, and 304 when the request already holds it. `HEAD` is `GET` with no body.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;

use crate::assets::{File, Files, INDEX};

/// The page served when the frontend was not built before the crate was compiled.
const UNBUILT: &str = include_str!("unbuilt.html");

/// The answer to a path under `/api` that no route of the API has, in the shape of the client's errors.
const NO_ROUTE: &str = r#"{"kind":"usage","message":"no such path in MetaLake's UI API"}"#;

/// The path the API's routes are under.
const API: &str = "/api";

/// Where Vite writes the files it names by their hashes.
const HASHED: &str = "assets/";

/// How long a client may keep a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cache {
    /// A file named by its hash: what the name names never changes.
    Forever,
    /// Kept, but asked again before each use: the page, whose content changes with each build under one name.
    Revalidate,
    /// Not kept: an error, or the page that says the frontend is not built.
    Never,
}

impl Cache {
    fn value(self) -> HeaderValue {
        HeaderValue::from_static(match self {
            Cache::Forever => "public, max-age=31536000, immutable",
            Cache::Revalidate => "no-cache",
            Cache::Never => "no-store",
        })
    }
}

/// A content coding of a file, in the order the router prefers them when a request accepts them equally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Coding {
    Brotli,
    Gzip,
    Identity,
}

impl Coding {
    const ALL: [Coding; 3] = [Coding::Brotli, Coding::Gzip, Coding::Identity];

    /// The suffix of the copy `pnpm build` writes in this coding.
    fn suffix(self) -> &'static str {
        match self {
            Coding::Brotli => ".br",
            Coding::Gzip => ".gz",
            Coding::Identity => "",
        }
    }

    /// Its `Content-Encoding`; none for the file itself.
    fn name(self) -> Option<&'static str> {
        match self {
            Coding::Brotli => Some("br"),
            Coding::Gzip => Some("gzip"),
            Coding::Identity => None,
        }
    }
}

/// Answers `uri` from `files`.
pub(crate) async fn serve(
    State(files): State<Arc<dyn Files>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let mut response = respond(files.as_ref(), &method, uri.path(), &headers);
    if method == Method::HEAD {
        // The headers of a `GET`, `Content-Length` included, with no body.
        *response.body_mut() = Body::empty();
    }
    response
}

fn respond(files: &dyn Files, method: &Method, path: &str, headers: &HeaderMap) -> Response {
    if path == API || path.starts_with("/api/") {
        return answer(
            StatusCode::NOT_FOUND,
            "application/json",
            NO_ROUTE.as_bytes(),
            Cache::Never,
        );
    }
    if method != Method::GET && method != Method::HEAD {
        let mut response = answer(StatusCode::METHOD_NOT_ALLOWED, TEXT, b"", Cache::Never);
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
    let last = relative.rsplit('/').next().unwrap_or_default();
    if !last.contains('.') {
        return match files.get(INDEX) {
            Some(page) => found(files, INDEX, page, Cache::Revalidate, headers),
            None => answer(StatusCode::SERVICE_UNAVAILABLE, HTML, UNBUILT.as_bytes(), Cache::Never),
        };
    }
    // A compressed copy is answered for its file, never by its own name.
    if Coding::ALL
        .iter()
        .any(|coding| coding.name().is_some() && relative.ends_with(coding.suffix()))
    {
        return not_found();
    }
    let cache = if relative.starts_with(HASHED) {
        Cache::Forever
    } else {
        Cache::Revalidate
    };
    match files.get(relative) {
        Some(file) => found(files, relative, file, cache, headers),
        None => not_found(),
    }
}

const TEXT: &str = "text/plain; charset=utf-8";
const HTML: &str = "text/html; charset=utf-8";

/// The file at `path`, in the coding the request accepts best of those it has, or 304 when the request holds it.
fn found(files: &dyn Files, path: &str, file: File, cache: Cache, headers: &HeaderMap) -> Response {
    let accepted = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok());
    let (coding, sent) = codings(accepted)
        .into_iter()
        .find_map(|coding| match coding {
            Coding::Identity => Some((coding, file.clone())),
            _ => files
                .get(&format!("{path}{}", coding.suffix()))
                .map(|copy| (coding, copy)),
        })
        .unwrap_or((Coding::Identity, file.clone()));
    let tag = entity_tag(&sent.sha256);
    let held = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|given| matches(given, &tag));

    let mut response = if held {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
    } else {
        answer(StatusCode::OK, &media_type(&file), &sent.bytes, cache)
    };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, cache.value());
    // A cache keeps one copy per coding asked for.
    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    if let Ok(value) = HeaderValue::from_str(&tag) {
        headers.insert(header::ETAG, value);
    }
    if let (Some(name), false) = (coding.name(), held) {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static(name));
    }
    response
}

/// The codings `accept_encoding` accepts, the most preferred first (RFC 9110 §12.5.3). With no header, the file itself:
/// a client that says nothing may read no other. With none acceptable, the file itself too, rather than 406, which no
/// browser asks for.
pub(crate) fn codings(accept_encoding: Option<&str>) -> Vec<Coding> {
    let Some(accept) = accept_encoding else {
        return vec![Coding::Identity];
    };
    let mut named: [Option<f32>; 3] = [None; 3];
    let mut any = None;
    for item in accept.split(',') {
        let mut parts = item.split(';');
        let name = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
        let weight = parts
            .find_map(|parameter| {
                let (key, value) = parameter.split_once('=')?;
                key.trim()
                    .eq_ignore_ascii_case("q")
                    .then(|| value.trim().parse::<f32>().ok())?
            })
            .unwrap_or(1.0);
        match name.as_str() {
            "br" => named[0] = Some(weight),
            "gzip" | "x-gzip" => named[1] = Some(weight),
            "identity" => named[2] = Some(weight),
            "*" => any = Some(weight),
            _ => {}
        }
    }
    let weight = |index: usize| match (named[index], any) {
        (Some(weight), _) => weight,
        (None, Some(weight)) => weight,
        // The file itself is acceptable unless the request says otherwise; a coding it does not name is not.
        (None, None) => {
            if index == 2 {
                1.0
            } else {
                0.0
            }
        }
    };
    let mut accepted: Vec<(Coding, f32)> = Coding::ALL
        .iter()
        .enumerate()
        .map(|(index, coding)| (*coding, weight(index)))
        .filter(|(_, weight)| *weight > 0.0)
        .collect();
    // Stable, so codings weighed alike keep the router's order.
    accepted.sort_by(|a, b| b.1.total_cmp(&a.1));
    if accepted.is_empty() {
        return vec![Coding::Identity];
    }
    accepted.into_iter().map(|(coding, _)| coding).collect()
}

/// A strong entity tag of the bytes whose SHA-256 is `sha256`.
fn entity_tag(sha256: &[u8; 32]) -> String {
    let mut tag = String::with_capacity(66);
    tag.push('"');
    for byte in sha256 {
        tag.push_str(&format!("{byte:02x}"));
    }
    tag.push('"');
    tag
}

/// Whether `if_none_match` names `tag`, by the weak comparison `If-None-Match` uses (RFC 9110 §13.1.2).
pub(crate) fn matches(if_none_match: &str, tag: &str) -> bool {
    let tag = tag.trim_start_matches("W/");
    if_none_match.trim() == "*"
        || if_none_match
            .split(',')
            .any(|given| given.trim().trim_start_matches("W/") == tag)
}

/// A file's type, UTF-8 named where it is text.
fn media_type(file: &File) -> String {
    match file.media_type.as_ref() {
        text if (text.starts_with("text/") || text == "application/javascript") && !text.contains("charset") => {
            format!("{text}; charset=utf-8")
        }
        other => other.to_owned(),
    }
}

fn not_found() -> Response {
    answer(StatusCode::NOT_FOUND, TEXT, b"not found", Cache::Never)
}

fn answer(status: StatusCode, media_type: &str, body: &[u8], cache: Cache) -> Response {
    let mut response = Response::new(Body::from(body.to_vec()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(media_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(body.len()));
    headers.insert(header::CACHE_CONTROL, cache.value());
    response
}
