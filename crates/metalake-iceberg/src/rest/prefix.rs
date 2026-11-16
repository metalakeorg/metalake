//! The ref a request is on (I6). A lake's branches and tags are the whole lake's; Iceberg's REST protocol names a
//! catalog of its own by a prefix of the routes, `/v1/{prefix}/…`, which `GET /v1/config` hands a client for the
//! `warehouse` it asks for. A prefix is one of two, neither of which a route without a prefix starts with:
//!
//! - `refs/<name>`: that ref, whatever happens after, as `metalake iceberg connect --ref` connects an engine;
//! - `sessions/<id>`: the ref a session is on *at each request*, as `git`'s `HEAD` is the branch checked out. A session
//!   says its ref to the server, and again each time it moves, as `lake.on` and a notebook's selector do; a client
//!   connected to it follows, with no new connection.
//!
//! Either may go on with `/catalogs/<name>`: the catalog of the lake the client sees, a namespace's first level (I7).
//! Its namespaces are then the ones in it, from the schema down, as an engine names them: `lake.demo.t`. Without it, the
//! catalog is `default`.
//!
//! A request with no prefix is on `main`, in the catalog `default`. The prefix is the server's to choose and the client's to use as given:
//! DuckDB, PyIceberg and Iceberg's Java client join it to their routes without encoding it again. A ref's name, or a
//! session's id, may hold any character, `/` and `%` among them, so it is encoded whole, and decoded once here.

use axum::extract::{FromRequestParts, Request, State as Shared};
use axum::http::request::Parts;
use axum::http::{StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use metalake::path::DEFAULT_CATALOG;
use metalake::{Error, Ref};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use super::errors::Failure;
use crate::state::State;

/// What the prefix of a ref starts with.
const REFS: &str = "refs";
/// What the prefix of a session starts with.
const SESSIONS: &str = "sessions";
/// What names the catalog, after a ref's or a session's prefix.
const CATALOGS: &str = "catalogs";

/// Every character of a name but RFC 3986's unreserved ones is encoded, so that the name is one segment of a path.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// The prefix of the routes on the ref named `name`, in `catalog`.
pub(crate) fn prefix_of(name: &str, catalog: &str) -> String {
    format!("{REFS}/{}{}", utf8_percent_encode(name, SEGMENT), in_catalog(catalog))
}

/// The prefix of the routes on whatever ref session `id` is on, in `catalog`.
pub(crate) fn session_prefix(id: &str, catalog: &str) -> String {
    format!("{SESSIONS}/{}{}", utf8_percent_encode(id, SEGMENT), in_catalog(catalog))
}

/// What a prefix ends with for `catalog`: nothing for `default`.
fn in_catalog(catalog: &str) -> String {
    match catalog == DEFAULT_CATALOG {
        true => String::new(),
        false => format!("/{CATALOGS}/{}", utf8_percent_encode(catalog, SEGMENT)),
    }
}

/// The catalog of the lake a request sees (I7): the first level of every namespace it names, which it does not say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Catalog(pub(crate) String);

impl Default for Catalog {
    fn default() -> Self {
        Self(DEFAULT_CATALOG.to_string())
    }
}

impl Catalog {
    /// The lake's namespace of one the client names: under this catalog.
    pub(crate) fn inside<S: AsRef<str>>(&self, levels: &[S]) -> Vec<String> {
        std::iter::once(self.0.clone())
            .chain(levels.iter().map(|l| l.as_ref().to_string()))
            .collect()
    }

    /// A namespace of the lake as the client names it, if it is in this catalog and below it.
    pub(crate) fn outside<'a>(&self, namespace: &'a [String]) -> Option<&'a [String]> {
        match namespace.split_first() {
            Some((first, rest)) if *first == self.0 && !rest.is_empty() => Some(rest),
            _ => None,
        }
    }
}

/// What a prefix names.
#[derive(Debug, PartialEq, Eq)]
enum Named {
    Ref(String),
    Session(String),
}

/// One segment of a path, decoded.
fn decoded(encoded: &str) -> Option<String> {
    if encoded.is_empty() {
        return None;
    }
    Some(percent_decode_str(encoded).decode_utf8().ok()?.into_owned())
}

/// A request's path under a prefix: what the prefix names, its catalog, and the path without the prefix, its query
/// kept.
fn unprefixed(uri: &Uri) -> Option<(Named, Catalog, Uri)> {
    let under = uri.path().strip_prefix("/v1/")?;
    let (kind, under) = under.split_once('/')?;
    let (encoded, mut rest) = under.split_once('/')?;
    let name = decoded(encoded)?;
    let named = match kind {
        REFS => Named::Ref(name),
        SESSIONS => Named::Session(name),
        _ => return None,
    };
    let mut catalog = Catalog::default();
    if let Some(after) = rest.strip_prefix(&format!("{CATALOGS}/")) {
        let (encoded, after) = after.split_once('/')?;
        catalog = Catalog(decoded(encoded)?);
        rest = after;
    }
    if rest.is_empty() {
        return None;
    }
    let path = match uri.query() {
        Some(query) => format!("/v1/{rest}?{query}"),
        None => format!("/v1/{rest}"),
    };
    Some((named, catalog, path.parse().ok()?))
}

/// The ref a request is on and the catalog it sees, as the request's extension: the ones its prefix names, or `main`
/// and `default` when it has none.
#[derive(Debug, Clone)]
pub(crate) struct On(pub(crate) Ref, pub(crate) Catalog);

impl FromRequestParts<State> for On {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &State) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<On>()
            .cloned()
            .unwrap_or_else(|| On(state.main().clone(), Catalog::default())))
    }
}

/// Before the routes: a request under a prefix is on the ref it names, or on the one its session is on now, and is
/// routed as one without it. A ref the lake has not, or a session that has said none, is refused here, before anything
/// reads.
pub(crate) async fn route(Shared(state): Shared<State>, mut request: Request, next: Next) -> Response {
    let Some((named, catalog, path)) = unprefixed(request.uri()) else {
        return next.run(request).await;
    };
    let name = match named {
        Named::Ref(name) => name,
        Named::Session(id) => match state.session(&id) {
            Some(name) => name,
            None => return Failure::no_session(&id, StatusCode::NOT_FOUND).into_response(),
        },
    };
    let on = match state.named(&name).await {
        Ok(on) => on,
        Err(Error::NotFound(_)) => return Failure::no_ref(&name).into_response(),
        Err(e) => return Failure::from(e).into_response(),
    };
    request.extensions_mut().insert(On(on, catalog));
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
            let prefix = prefix_of(name, DEFAULT_CATALOG);
            assert_eq!(prefix.matches('/').count(), 1, "{prefix}");
            let uri: Uri = format!("/v1/{prefix}/namespaces/db/tables?pageToken=").parse().unwrap();
            let (back, catalog, path) = unprefixed(&uri).unwrap();
            assert_eq!(back, Named::Ref(name.to_string()));
            assert_eq!(catalog, Catalog::default());
            assert_eq!(path, "/v1/namespaces/db/tables?pageToken=");
        }
        assert_eq!(prefix_of("feature/x", DEFAULT_CATALOG), "refs/feature%2Fx");
        assert_eq!(prefix_of("a%2Fb", DEFAULT_CATALOG), "refs/a%252Fb");
    }

    /// A catalog other than `default` goes on the prefix, any name it may take whole (I7).
    #[test]
    fn i7_a_catalog_is_a_segment_after_the_ref() {
        for catalog in ["ventas", "a.b", "with space", "x/y"] {
            let prefix = prefix_of("dev", catalog);
            let uri: Uri = format!("/v1/{prefix}/namespaces/eu/tables").parse().unwrap();
            let (named, seen, path) = unprefixed(&uri).unwrap();
            assert_eq!((named, seen), (Named::Ref("dev".into()), Catalog(catalog.into())));
            assert_eq!(path, "/v1/namespaces/eu/tables");
        }
        let uri: Uri = format!("/v1/{}/namespaces", session_prefix("k1", "ventas")).parse().unwrap();
        let (named, seen, _) = unprefixed(&uri).unwrap();
        assert_eq!((named, seen), (Named::Session("k1".into()), Catalog("ventas".into())));
        let ventas = Catalog("ventas".into());
        assert_eq!(ventas.inside(&["eu"]), ["ventas", "eu"]);
        let lake = vec!["ventas".to_string(), "eu".to_string()];
        assert_eq!(ventas.outside(&lake), Some(&lake[1..]));
        assert_eq!(Catalog::default().outside(&lake), None);
        assert_eq!(ventas.outside(&lake[..1]), None, "the catalog itself is no schema");
    }

    #[test]
    fn a_sessions_id_is_one_segment_of_its_prefix() {
        let id = "9f0c/kernel 1";
        let uri: Uri = format!("/v1/{}/namespaces", session_prefix(id, DEFAULT_CATALOG)).parse().unwrap();
        let (named, _, path) = unprefixed(&uri).unwrap();
        assert_eq!(named, Named::Session(id.to_string()));
        assert_eq!(path, "/v1/namespaces");
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
            "/v1/sessions/x",
            "/v1/other/x/namespaces",
            "/metalake/shutdown",
        ] {
            assert!(unprefixed(&path.parse().unwrap()).is_none(), "{path}");
        }
    }
}
