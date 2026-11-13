//! The UI's API, under `/api` (ADR 0007 §4.1). The guard decides the token and the `Origin` by a path's prefix, over the
//! whole router, so that no path under `/api`, of today or to come, answers without them, whichever route it reaches;
//! and a path that does not exist answers 401 to a request without the token, which so learns nothing of which do.
//!
//! The router translates and adds no rule: each path asks the [`Backend`], which over a lake is metalake-client, the
//! logic every face calls; its answer is written as JSON, a failure as the client's [`Kind`] and message. The types
//! answered are the API's own, described in its OpenAPI spec ([`openapi`]), from which the frontend's are generated.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

/// The version of the API's contract, which a frontend of another build compares with its own.
pub const VERSION: u32 = 1;

/// What the API answers with, or fails with. Boxed, so that a backend is an object the router holds whatever lake, or
/// none, is behind it.
pub type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, Fault>> + Send + 'a>>;

/// The lake's marks as they move, for as long as it is read; one that fails ends it.
pub type Marks = Pin<Box<dyn Stream<Item = Result<Mark, Fault>> + Send>>;

/// What the API asks of a lake. Over a lake it is metalake-client's `Lake` (the feature `lake`); a test gives its own.
pub trait Backend: Send + Sync + 'static {
    /// What the lake is.
    fn status(&self) -> Answer<'_, Status>;
    /// Every branch and tag.
    fn refs(&self) -> Answer<'_, Vec<Ref>>;
    /// The tables a ref sees at its head.
    fn tables<'a>(&'a self, on: &'a str) -> Answer<'a, Vec<Table>>;
    /// The lake's mark now, then each that differs from the last, as the lake changes.
    fn watch(&self) -> Answer<'_, Marks>;
}

/// A backend with no lake behind it: every path that reads one says so. What the `serve` example and the router's tests
/// serve, where nothing of MetaLake is linked.
#[derive(Debug, Clone, Copy, Default)]
pub struct Detached;

impl Backend for Detached {
    fn status(&self) -> Answer<'_, Status> {
        Box::pin(async { Err(Detached::fault()) })
    }

    fn refs(&self) -> Answer<'_, Vec<Ref>> {
        Box::pin(async { Err(Detached::fault()) })
    }

    fn tables<'a>(&'a self, _: &'a str) -> Answer<'a, Vec<Table>> {
        Box::pin(async { Err(Detached::fault()) })
    }

    fn watch(&self) -> Answer<'_, Marks> {
        Box::pin(async { Err(Detached::fault()) })
    }
}

impl Detached {
    fn fault() -> Fault {
        Fault::new(
            Kind::Usage,
            "this UI was started without a lake: run `metalake ui` in one",
        )
    }
}

/// The API's version.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Version {
    /// [`VERSION`]: a frontend built for another refuses this server.
    pub api: u32,
}

/// What the lake is, as `metalake status` says it.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Status {
    /// The catalog's Postgres, without its password; absent for a lake with its own.
    pub postgres: Option<String>,
    /// The directory of the lake's own Postgres; absent for a lake on another.
    pub local: Option<String>,
    /// The catalog's schema in that Postgres.
    pub schema: String,
    /// Where the lake's files are.
    pub warehouse: Option<String>,
    pub format_version: String,
    pub writer_version: String,
    pub protocol_version: Option<String>,
    /// Every branch and tag.
    pub refs: Vec<Named>,
}

/// A ref, by its name and its kind.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Named {
    pub name: String,
    /// `branch` or `tag`.
    pub kind: String,
}

/// A branch or a tag.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Ref {
    pub name: String,
    /// `branch` or `tag`.
    pub kind: String,
    /// The ref it was made from, if any: `main` has none.
    pub parent: Option<String>,
    /// The sequence of its parent it was made at.
    pub fork_seq: Option<i64>,
    /// The sequence it was made at.
    pub created_seq: i64,
    /// Whether it may be deleted.
    pub protected: bool,
}

/// A table of the catalog, by its namespace's parts and its name.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Table {
    pub namespace: Vec<String>,
    pub name: String,
}

/// The lake's state, as far as telling that it changed needs: when either moves, what is shown of it is read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub struct Mark {
    /// Its latest commit's sequence: a commit, a merge and a new ref each move it.
    pub seq: i64,
    /// How many refs were deleted.
    pub deleted: i64,
}

/// What a failure means to whoever called, as the command line's exit codes and Python's exceptions say it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// The request was wrong: what it names does not exist, or cannot be asked so.
    Usage,
    Failed,
    /// A write that conflicts with another was refused; nothing changed.
    Conflict,
    /// The connection was lost, and a write that was sent may or may not have taken effect.
    Unknown,
}

/// A failure, as the command line's `--format json` says it.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Fault {
    pub kind: Kind,
    pub message: String,
    /// The specification's rule a refused write broke.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    /// Postgres's SQLSTATE.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Whether the same request may succeed if made again.
    pub retryable: bool,
}

impl Fault {
    pub fn new(kind: Kind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            rule: None,
            code: None,
            retryable: false,
        }
    }

    /// The answer's status: a wrong request is the client's, a failure the server's, a lost connection its Postgres's.
    fn status(&self) -> StatusCode {
        match self.kind {
            Kind::Usage => StatusCode::BAD_REQUEST,
            Kind::Failed => StatusCode::INTERNAL_SERVER_ERROR,
            Kind::Conflict => StatusCode::CONFLICT,
            Kind::Unknown => StatusCode::BAD_GATEWAY,
        }
    }
}

/// The paths under `/api`, over `backend`.
pub(crate) fn router(backend: Arc<dyn Backend>) -> Router {
    Router::new()
        .route("/v1/version", get(version))
        .route("/v1/status", get(status))
        .route("/v1/refs", get(refs))
        .route("/v1/refs/{ref}/tables", get(tables))
        .route("/v1/events", get(events))
        .fallback(no_route)
        .with_state(backend)
}

/// The API's version.
#[utoipa::path(get, path = "/api/v1/version", tag = "api", responses(
    (status = 200, description = "The API's version", body = Version),
    (status = 401, description = "Without the run's token", body = Fault),
))]
async fn version() -> Response {
    answer(Ok(Version { api: VERSION }))
}

/// What the lake is: its catalog, its warehouse, its versions and its refs.
#[utoipa::path(get, path = "/api/v1/status", tag = "lake", responses(
    (status = 200, description = "What the lake is", body = Status),
    (status = 401, description = "Without the run's token", body = Fault),
    (status = 500, description = "The lake could not be read", body = Fault),
    (status = 502, description = "The connection to its Postgres was lost", body = Fault),
))]
async fn status(State(backend): State<Arc<dyn Backend>>) -> Response {
    answer(backend.status().await)
}

/// Every branch and tag.
#[utoipa::path(get, path = "/api/v1/refs", tag = "catalog", responses(
    (status = 200, description = "Every branch and tag", body = [Ref]),
    (status = 401, description = "Without the run's token", body = Fault),
    (status = 500, description = "The lake could not be read", body = Fault),
))]
async fn refs(State(backend): State<Arc<dyn Backend>>) -> Response {
    answer(backend.refs().await)
}

/// The tables a ref sees at its head.
#[utoipa::path(get, path = "/api/v1/refs/{ref}/tables", tag = "catalog",
    params(("ref" = String, Path, description = "The branch or tag, percent-encoded")),
    responses(
        (status = 200, description = "Its tables", body = [Table]),
        (status = 400, description = "No such ref", body = Fault),
        (status = 401, description = "Without the run's token", body = Fault),
        (status = 500, description = "The lake could not be read", body = Fault),
    ))]
async fn tables(State(backend): State<Arc<dyn Backend>>, Path(on): Path<String>) -> Response {
    answer(backend.tables(&on).await)
}

/// The lake's marks as it changes, as server-sent events: a `mark` now, then one each time the lake's differs, for as
/// long as the request is open. A `fault` ends them; the frontend asks again.
#[utoipa::path(get, path = "/api/v1/events", tag = "catalog", responses(
    (status = 200, description = "Events `mark`, and a last `fault` if one comes", content_type = "text/event-stream", body = Mark),
    (status = 401, description = "Without the run's token", body = Fault),
    (status = 500, description = "The lake could not be watched", body = Fault),
))]
async fn events(State(backend): State<Arc<dyn Backend>>) -> Response {
    let marks = match backend.watch().await {
        Ok(marks) => marks,
        Err(fault) => return answer::<()>(Err(fault)),
    };
    let events = marks.map(|mark| {
        let event = match mark {
            Ok(mark) => Event::default().event("mark").data(to_json(&mark)),
            Err(fault) => Event::default().event("fault").data(to_json(&fault)),
        };
        Ok::<_, Infallible>(event)
    });
    let mut response = Sse::new(events).keep_alive(KeepAlive::default()).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("the API's types are JSON")
}

async fn no_route() -> Response {
    let fault = Fault::new(Kind::Usage, "no such path in MetaLake's UI API");
    json(
        StatusCode::NOT_FOUND,
        serde_json::to_string(&fault).expect("a fault is JSON"),
    )
}

/// The API's spec.
#[derive(OpenApi)]
#[openapi(
    info(title = "MetaLake UI", version = "1", description = "The API of `metalake ui` (ADR 0007 §4.1)"),
    paths(version, status, refs, tables, events),
    modifiers(&Token),
    security(("token" = []))
)]
struct Spec;

/// The run's token, as every path under `/api` takes it.
struct Token;

impl Modify for Token {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "token",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
    }
}

/// The API's OpenAPI spec, as `web/openapi.json` holds it.
pub fn openapi() -> String {
    let mut spec = Spec::openapi().to_pretty_json().expect("the spec is JSON");
    spec.push('\n');
    spec
}

/// `value` as JSON, or the failure with its status.
fn answer<T: Serialize>(value: Result<T, Fault>) -> Response {
    let (status, body) = match value {
        Ok(value) => (StatusCode::OK, serde_json::to_string(&value)),
        Err(fault) => (fault.status(), serde_json::to_string(&fault)),
    };
    json(status, body.expect("the API's types are JSON"))
}

fn json(status: StatusCode, body: String) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
