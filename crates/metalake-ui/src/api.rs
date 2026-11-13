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
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

use crate::notebook::Notebook;

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
    /// Runs a text's statements, each committed before the next, and answers the first rows of the last.
    fn sql(&self, statement: Statement) -> Answer<'_, Rows>;
}

/// How many rows a statement answers when the request says no other: what a page shows without holding too much.
pub const LIMIT: usize = 10_000;
/// The most a request may ask for.
pub const MOST: usize = 100_000;

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

    fn sql(&self, _: Statement) -> Answer<'_, Rows> {
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
    /// How many commits it has among its latest sequences: one that lands after a later one moves it.
    pub recent: i64,
    /// How many refs were deleted.
    pub deleted: i64,
}

/// What to run: a text of statements, in order, each committed before the next, as `metalake sql` runs one.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct Statement {
    pub sql: String,
    /// The branch a name that says no ref reads and writes on: `default.t` is `dev.default.t` on `dev`. By default,
    /// `main`.
    #[serde(default)]
    pub branch: Option<String>,
    /// How many rows of the last statement to answer, at most [`MOST`]; by default [`LIMIT`].
    #[serde(default)]
    pub limit: Option<usize>,
}

impl Statement {
    /// The rows to answer, as asked within the bounds.
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(LIMIT).clamp(1, MOST)
    }
}

/// The first rows a statement answered, as an Arrow IPC stream: their schema, then their batches.
#[derive(Debug, Clone)]
pub struct Rows {
    pub arrow: Vec<u8>,
    /// How many rows it holds.
    pub rows: usize,
    /// Whether the statement answered more than it holds.
    pub truncated: bool,
}

/// Whether this `metalake` can give a Notebook (ADR 0008).
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = Notebook)]
pub struct NotebookState {
    /// Whether it runs in a Python: a `metalake` pip installed does, one built alone does not.
    pub python: bool,
    /// Whether JupyterLab can be imported by that Python.
    pub jupyterlab: bool,
    /// The line that installs what the Notebook needs.
    pub install: String,
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

/// The paths under `/api`, over `backend`; and the Notebook's, over `notebook`.
pub(crate) fn router(backend: Arc<dyn Backend>, notebook: Arc<Notebook>) -> Router {
    let notebooks = Router::new()
        .route("/v1/notebook", get(notebook_state))
        .with_state(notebook);
    Router::new()
        .route("/v1/version", get(version))
        .route("/v1/status", get(status))
        .route("/v1/refs", get(refs))
        .route("/v1/refs/{ref}/tables", get(tables))
        .route("/v1/events", get(events))
        .route("/v1/sql", post(sql))
        .fallback(no_route)
        .with_state(backend)
        .merge(notebooks)
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

/// Runs statements on the lake, as `metalake sql` and Python's `lake.sql` do, through the same session: a read at the
/// point it is run, a write committed. The rows of the last statement come as an Arrow IPC stream, so that each value
/// keeps its type, and at most as many as asked: a statement that answered more says so. A request the browser drops
/// drops the statement, which stops reading.
#[utoipa::path(post, path = "/api/v1/sql", tag = "sql",
    request_body(content = Statement, content_type = "application/json"),
    responses(
        (status = 200, description = "The first rows of the last statement", content_type = "application/vnd.apache.arrow.stream",
            headers(
                ("metalake-rows" = u64, description = "How many rows it holds"),
                ("metalake-truncated" = bool, description = "Whether the statement answered more"),
            )),
        (status = 400, description = "A statement that is wrong, or refused", body = Fault),
        (status = 401, description = "Without the run's token", body = Fault),
        (status = 403, description = "From another origin than the UI's own", body = Fault),
        (status = 409, description = "A write that conflicts with another; nothing changed", body = Fault),
        (status = 500, description = "The statement failed", body = Fault),
    ))]
async fn sql(State(backend): State<Arc<dyn Backend>>, body: Bytes) -> Response {
    let statement: Statement = match serde_json::from_slice(&body) {
        Ok(statement) => statement,
        Err(e) => return answer::<()>(Err(Fault::new(Kind::Usage, format!("a request to run is JSON: {e}")))),
    };
    if statement.sql.trim().is_empty() {
        return answer::<()>(Err(Fault::new(Kind::Usage, "no statement to run")));
    }
    let rows = match backend.sql(statement).await {
        Ok(rows) => rows,
        Err(fault) => return answer::<()>(Err(fault)),
    };
    let mut response = Response::new(Body::from(rows.arrow));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("metalake-rows", HeaderValue::from(rows.rows));
    headers.insert(
        "metalake-truncated",
        HeaderValue::from_static(if rows.truncated { "true" } else { "false" }),
    );
    response
}

fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("the API's types are JSON")
}

/// Whether this `metalake` can give a Notebook: a Python, and JupyterLab in it; and the line that installs it. Looked
/// at each time, so that an install while the UI runs is seen.
#[utoipa::path(get, path = "/api/v1/notebook", tag = "notebook", responses(
    (status = 200, description = "What the Notebook can do here", body = NotebookState),
    (status = 401, description = "Without the run's token", body = Fault),
))]
async fn notebook_state(State(notebook): State<Arc<Notebook>>) -> Response {
    answer(Ok(NotebookState {
        python: notebook.python().is_some(),
        jupyterlab: notebook.jupyterlab().await,
        install: crate::notebook::INSTALL.to_string(),
    }))
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
    paths(version, status, refs, tables, events, sql, notebook_state),
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
