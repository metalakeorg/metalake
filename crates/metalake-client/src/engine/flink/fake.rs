//! A Flink SQL Gateway that answers as the tests say, on a port of its own: what it hears is kept to be asked.
//!
//! `select …` in a batch session answers `NOT_READY`, then `n` = 1, then 2, then `EOS`; in a streaming session, a page
//! of a query that never ends. `fail` is refused with a Java trace; `busy` is answered 503 once; `slow` never answers its
//! fetch. Anything else is answered `OK`. A session the test said is gone is answered as one the Gateway reaped.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use serde_json::{Value, json};

/// What the fake heard.
#[derive(Debug, Default)]
pub(super) struct Heard {
    /// Each session opened, with its properties.
    pub opened: Vec<(String, Value)>,
    /// Each statement, with its session.
    pub statements: Vec<(String, String)>,
    pub closed_operations: Vec<String>,
    pub closed_sessions: Vec<String>,
    /// Sessions answered as the Gateway answers one it reaped.
    pub gone: HashSet<String>,
    /// The `authorization` each call bore.
    pub tokens: Vec<Option<String>>,
    batch: HashMap<String, bool>,
    operations: HashMap<String, (String, String, u32)>,
    busy: bool,
    next: u32,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Fake {
    pub heard: Arc<Mutex<Heard>>,
}

type Answer = (StatusCode, String);

fn bore(fake: &Fake, headers: &HeaderMap) {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    fake.heard.lock().unwrap().tokens.push(token);
}

fn gone(session: &str) -> Answer {
    let trace = format!(
        "<Exception on server side:\norg.apache.flink.table.gateway.api.utils.SqlGatewayException: \
         Session '{session}' does not exist.\nEnd of exception on server side>"
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({"errors": ["Internal server error.", trace]}).to_string(),
    )
}

async fn open(State(fake): State<Fake>, headers: HeaderMap, body: String) -> Answer {
    bore(&fake, &headers);
    let body: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let properties = body["properties"].clone();
    let mut heard = fake.heard.lock().unwrap();
    heard.next += 1;
    let session = format!("s{}", heard.next);
    let batch = properties["execution.runtime-mode"] == "batch";
    heard.batch.insert(session.clone(), batch);
    heard.opened.push((session.clone(), properties));
    (StatusCode::OK, json!({"sessionHandle": session}).to_string())
}

async fn close_session(State(fake): State<Fake>, Path(session): Path<String>) -> Answer {
    fake.heard.lock().unwrap().closed_sessions.push(session);
    (StatusCode::OK, json!({"status": "CLOSED"}).to_string())
}

async fn submit(State(fake): State<Fake>, Path(session): Path<String>, headers: HeaderMap, body: String) -> Answer {
    bore(&fake, &headers);
    let statement = serde_json::from_str::<Value>(&body).unwrap_or(Value::Null)["statement"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let mut heard = fake.heard.lock().unwrap();
    if heard.gone.contains(&session) {
        return gone(&session);
    }
    if statement == "busy" && !heard.busy {
        heard.busy = true;
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    }
    heard.statements.push((session.clone(), statement.clone()));
    let lowered = statement.to_lowercase().replace(' ', "");
    if lowered == "set'execution.runtime-mode'='batch'" {
        heard.batch.insert(session.clone(), true);
    }
    heard.next += 1;
    let operation = format!("o{}", heard.next);
    heard.operations.insert(operation.clone(), (session, statement, 0));
    (StatusCode::OK, json!({"operationHandle": operation}).to_string())
}

fn page(base: &str, rows: Value, next: Option<String>, query: bool) -> Answer {
    let mut answer = json!({
        "resultType": "PAYLOAD",
        "isQueryResult": query,
        "resultKind": "SUCCESS_WITH_CONTENT",
        "results": {"columns": [], "rowFormat": "JSON", "data": rows},
    });
    if query {
        answer["results"]["columns"] = json!([{"name": "n", "logicalType": {"type": "BIGINT", "nullable": false}}]);
    } else {
        answer["results"]["columns"] =
            json!([{"name": "result", "logicalType": {"type": "VARCHAR", "nullable": true}}]);
    }
    if let Some(next) = next {
        answer["nextResultUri"] = json!(format!("{base}{next}"));
    }
    (StatusCode::OK, answer.to_string())
}

async fn fetch(State(fake): State<Fake>, Path((session, operation, token)): Path<(String, String, u32)>) -> Answer {
    let base = format!("/v3/sessions/{session}/operations/{operation}/result");
    let (statement, batch, asked) = {
        let mut heard = fake.heard.lock().unwrap();
        if heard.gone.contains(&session) {
            return gone(&session);
        }
        let batch = heard.batch.get(&session).copied().unwrap_or(false);
        let Some(op) = heard.operations.get_mut(&operation) else {
            return (
                StatusCode::NOT_FOUND,
                json!({"errors": ["no such operation"]}).to_string(),
            );
        };
        op.2 += 1;
        (op.1.clone(), batch, op.2)
    };
    let next = |t: u32| Some(format!("{base}/{t}?rowFormat=JSON"));
    match statement.as_str() {
        "fail" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"errors": ["Internal server error.", "<Exception on server side:\n\
                org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.\n\
                Caused by: org.apache.flink.table.api.ValidationException: Object 'nope' not found\n\
                End of exception on server side>"]})
            .to_string(),
        ),
        "slow" => {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            (StatusCode::OK, String::new())
        }
        query if query.starts_with("select") => {
            if !batch {
                return page("", json!([{"kind": "INSERT", "fields": [1]}]), next(token + 1), true);
            }
            match (token, asked) {
                (0, 1) => (
                    StatusCode::OK,
                    json!({"resultType": "NOT_READY", "nextResultUri": format!("{base}/0?rowFormat=JSON")}).to_string(),
                ),
                (0, _) => page("", json!([{"kind": "INSERT", "fields": [1]}]), next(1), true),
                (1, _) => page("", json!([{"kind": "INSERT", "fields": [2]}]), next(2), true),
                _ => (
                    StatusCode::OK,
                    json!({"resultType": "EOS", "isQueryResult": true}).to_string(),
                ),
            }
        }
        _ => match token {
            0 => page("", json!([{"kind": "INSERT", "fields": ["OK"]}]), next(1), false),
            _ => (StatusCode::OK, json!({"resultType": "EOS"}).to_string()),
        },
    }
}

async fn close_operation(State(fake): State<Fake>, Path((_, operation)): Path<(String, String)>) -> Answer {
    fake.heard.lock().unwrap().closed_operations.push(operation);
    (StatusCode::OK, json!({"status": "CLOSED"}).to_string())
}

/// The fake, served on a port of its own: its url.
pub(super) async fn serve(fake: Fake) -> String {
    let app = Router::new()
        .route("/v3/sessions", post(open))
        .route("/v3/sessions/{session}", delete(close_session))
        .route("/v3/sessions/{session}/statements", post(submit))
        .route(
            "/v3/sessions/{session}/operations/{operation}/result/{token}",
            get(fetch),
        )
        .route(
            "/v3/sessions/{session}/operations/{operation}/close",
            delete(close_operation),
        )
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}
