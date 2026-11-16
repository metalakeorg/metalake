//! The ref a request is on (I6). A lake's branches and tags are the whole lake's; Iceberg's REST protocol names a
//! catalog of its own by a prefix of the routes, `/v1/{prefix}/…`, which `GET /v1/config` hands a client for the
//! `warehouse` it asks for. Here the warehouse is a ref's name, and the prefix `refs/<name, percent-encoded>`, which no
//! route without a prefix starts with: a request under it is on that ref, one without a prefix on `main`.
//!
//! The prefix is the server's to choose and the client's to use as given: DuckDB, PyIceberg and Iceberg's Java client
//! join it to their routes without encoding it again. A ref's name may hold any character, `/` and `%` among them, so it
//! is encoded whole, and decoded once here.

use axum::extract::{FromRequestParts, Request, State as Shared};
use axum::http::Uri;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use metalake::{Error, Ref};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use super::errors::Failure;
use crate::state::State;

/// What a prefix starts with.
const REFS: &str = "refs";

/// Every character of a name but RFC 3986's unreserved ones is encoded, so that the name is one segment of a path.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// The prefix of the routes on the ref named `name`.
pub(crate) fn prefix_of(name: &str) -> String {
    format!("{REFS}/{}", utf8_percent_encode(name, SEGMENT))
}

/// A request's path under a prefix: the ref's name, and the path without the prefix, its query kept.
fn unprefixed(uri: &Uri) -> Option<(String, Uri)> {
    let under = uri.path().strip_prefix(&format!("/v1/{REFS}/"))?;
    let (encoded, rest) = under.split_once('/')?;
    if encoded.is_empty() || rest.is_empty() {
        return None;
    }
    let name = percent_decode_str(encoded).decode_utf8().ok()?.into_owned();
    let path = match uri.query() {
        Some(query) => format!("/v1/{rest}?{query}"),
        None => format!("/v1/{rest}"),
    };
    Some((name, path.parse().ok()?))
}

/// The ref a request is on, as the request's extension: the one its prefix names, or `main` when it has none.
#[derive(Debug, Clone)]
#[allow(
    dead_code,
    reason = "the routes read it from I6's step 3, which reads on the request's ref"
)]
pub(crate) struct On(pub(crate) Ref);

impl FromRequestParts<State> for On {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &State) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<On>()
            .cloned()
            .unwrap_or_else(|| On(state.main().clone())))
    }
}

/// Before the routes: a request under a prefix is on the ref it names, and is routed as one without it. A ref the lake
/// has not is refused here, before anything reads.
pub(crate) async fn route(Shared(state): Shared<State>, mut request: Request, next: Next) -> Response {
    let Some((name, path)) = unprefixed(request.uri()) else {
        return next.run(request).await;
    };
    let on = match state.named(&name).await {
        Ok(on) => on,
        Err(Error::NotFound(_)) => return Failure::no_ref(&name).into_response(),
        Err(e) => return Failure::from(e).into_response(),
    };
    request.extensions_mut().insert(On(on));
    *request.uri_mut() = path;
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Any name a ref may take goes and comes back whole, as one segment of a path.
    #[test]
    fn a_refs_name_is_one_segment_of_its_prefix() {
        for name in [
            "main",
            "dev",
            "feature/x",
            "with space",
            "acentó",
            "a.b",
            "a@b",
            "a%2Fb",
            "-x_y~z",
        ] {
            let prefix = prefix_of(name);
            assert_eq!(prefix.matches('/').count(), 1, "{prefix}");
            let uri: Uri = format!("/v1/{prefix}/namespaces/db/tables?pageToken=").parse().unwrap();
            let (back, path) = unprefixed(&uri).unwrap();
            assert_eq!(back, name);
            assert_eq!(path, "/v1/namespaces/db/tables?pageToken=");
        }
        assert_eq!(prefix_of("feature/x"), "refs/feature%2Fx");
        assert_eq!(prefix_of("a%2Fb"), "refs/a%252Fb");
    }

    #[test]
    fn a_path_without_a_prefix_is_left_as_it_is() {
        for path in [
            "/v1/config",
            "/v1/namespaces",
            "/v1/namespaces/refs/tables",
            "/v1/tables/rename",
            "/v1/refs/dev",
            "/v1/refs//namespaces",
            "/metalake/shutdown",
        ] {
            assert!(unprefixed(&path.parse().unwrap()).is_none(), "{path}");
        }
    }
}
