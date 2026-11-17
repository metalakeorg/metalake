//! A statement sent to Flink through its SQL Gateway (REST v3): a session opened, the statement submitted, its result
//! fetched page by page (`NOT_READY`, `PAYLOAD`, `EOS`), the operation closed. Statements on a ref run in a Gateway
//! session of that ref, which the lake's catalogs are created on under their own names, each asking the lake's Iceberg
//! server for itself on that ref (`catalog@ref`): what `USING` sends reads the names MetaLake does, on the session's
//! branch. A session the Gateway reaped (ten idle minutes) is opened again, its catalogs created again, and the
//! statement sent once more; what `SET` had set on it is gone, and that is said. A query in streaming is refused once
//! Flink says it is one: it would not end, and `USING` answers a statement's rows when it ended.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde_json::json;

use super::{EngineConfig, RuntimeMode};
use crate::{Error, Result};

#[cfg(test)]
mod fake;
#[cfg(test)]
mod protocol_tests;
mod wire;

use wire::{Column, Fetched, Opened, ResultType, Row, Submitted};

/// How long one fetch may wait: the Gateway holds a fetch until it has rows, so a quiet query is asked again then.
const FETCH: Duration = Duration::from_secs(60);
/// How many times a call the Gateway cannot answer now (503) is asked again, waiting longer each time.
const AGAIN: u32 = 5;
const FIRST: Duration = Duration::from_millis(100);
/// The wait between asks of a result that is not ready, which the Gateway answers at once.
const NOT_READY: Duration = Duration::from_millis(100);

/// A catalog of the lake as Flink is given it: its name, and the `CREATE CATALOG` that makes it.
pub(crate) type CatalogStatement = (String, String);

/// A Flink the lake names, ready to be sent statements: clones share its sessions, closed when the last is dropped.
#[derive(Debug, Clone)]
pub struct Flink {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    url: String,
    http: reqwest::Client,
    mode: Option<RuntimeMode>,
    lake_url: Option<String>,
    /// The Gateway session of each ref statements ran on, by the ref's name.
    sessions: Mutex<HashMap<String, Held>>,
    /// Held while a session is opened and its catalogs made, so that two statements at once open one session and
    /// make each catalog once.
    making: tokio::sync::Mutex<()>,
}

/// A Gateway session, whether it runs streaming, and the lake's catalogs created on it, each by its statement.
#[derive(Debug, Clone)]
struct Held {
    handle: String,
    streaming: bool,
    catalogs: HashMap<String, String>,
}

/// Why a statement was not run: the Gateway has no such session any more, or anything else.
enum Failed {
    SessionGone,
    Error(Error),
}

impl From<Error> for Failed {
    fn from(e: Error) -> Self {
        Self::Error(e)
    }
}

impl Flink {
    /// The engine `config` names, its secret read from the environment now: a variable named and unset is said here,
    /// before anything is sent.
    pub fn new(name: &str, config: &EngineConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if let Some(variable) = &config.token_env {
            let token = std::env::var(variable)
                .map_err(|_| Error::usage(format!("the engine `{name}` signs in by ${variable}, which is not set")))?;
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Error::usage(format!("the engine `{name}`: its token is not a header's value")))?;
            headers.insert(AUTHORIZATION, value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(FETCH)
            .build()
            .map_err(|e| Error::failed(format!("a client of Flink: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                url: config.url.trim_end_matches('/').to_string(),
                http,
                mode: config.mode,
                lake_url: config.lake_url.clone(),
                sessions: Mutex::new(HashMap::new()),
                making: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// Where the engine reaches the lake's Iceberg server, when it names it.
    pub(crate) fn lake_url(&self) -> Option<&str> {
        self.inner.lake_url.as_deref()
    }

    /// Runs one statement on `main`, no catalog of the lake created: Flink's own, as a check asks.
    pub async fn sql(&self, text: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        self.sql_on(crate::MAIN, &[], text).await
    }

    /// Runs one statement in the Gateway session of the ref `on`, `catalogs` created on it first if they are not, and
    /// answers its rows: in streaming, each row's kind first. A statement Flink refuses fails with Flink's own words.
    pub async fn sql_on(
        &self,
        on: &str,
        catalogs: &[CatalogStatement],
        text: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        match self.attempt(on, catalogs, text).await {
            Ok(rows) => Ok(rows),
            Err(Failed::Error(e)) => Err(e),
            Err(Failed::SessionGone) => {
                self.forget(on);
                eprintln!(
                    "metalake: flink at {} had closed this session's, idle; it is opened again, and what SET had \
                     set on it is not",
                    self.inner.url
                );
                match self.attempt(on, catalogs, text).await {
                    Ok(rows) => Ok(rows),
                    Err(Failed::Error(e)) => Err(e),
                    Err(Failed::SessionGone) => Err(Error::failed(format!(
                        "flink at {}: a session opened is gone at once",
                        self.inner.url
                    ))),
                }
            }
        }
    }

    async fn attempt(
        &self,
        on: &str,
        catalogs: &[CatalogStatement],
        text: &str,
    ) -> std::result::Result<(SchemaRef, Vec<RecordBatch>), Failed> {
        let session = {
            let _making = self.inner.making.lock().await;
            let mut session = self.session(on).await?;
            // A catalog is made once on a session; one whose statement changed since, as when the lake's server
            // started again at another address, is made on a session opened anew.
            let changed = catalogs
                .iter()
                .any(|(catalog, made)| session.catalogs.get(catalog).is_some_and(|was| was != made));
            if changed {
                self.close_session(on);
                session = self.session(on).await?;
            }
            for (catalog, made) in catalogs {
                if !session.catalogs.contains_key(catalog) {
                    self.execute(&session.handle, made, false).await?;
                    self.mark(on, catalog, made);
                }
            }
            session
        };
        let answered = self.execute(&session.handle, text, session.streaming).await?;
        // A mode set in a `USING` block holds for what follows; in batch an INSERT answers once its job ended.
        if let Some(mode) = mode_set(text) {
            let sync = if mode == RuntimeMode::Batch { "true" } else { "false" };
            self.execute(&session.handle, &format!("SET 'table.dml-sync' = '{sync}'"), false)
                .await?;
            self.mode_is(on, mode);
        }
        Ok(answered)
    }

    /// The session of `on`, opened if there is none.
    async fn session(&self, on: &str) -> std::result::Result<Held, Failed> {
        if let Some(held) = self.held(on) {
            return Ok(held);
        }
        let mode = self.inner.mode.unwrap_or(RuntimeMode::Streaming);
        let mut properties = serde_json::Map::new();
        if let Some(mode) = self.inner.mode {
            properties.insert("execution.runtime-mode".into(), json!(mode.name()));
        }
        if mode == RuntimeMode::Batch {
            properties.insert("table.dml-sync".into(), json!("true"));
        }
        let opened: Opened = self
            .call(
                reqwest::Method::POST,
                "/v3/sessions",
                Some(json!({ "properties": properties })),
            )
            .await?;
        let held = Held {
            handle: opened.session_handle,
            streaming: mode == RuntimeMode::Streaming,
            catalogs: HashMap::new(),
        };
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        Ok(sessions.entry(on.to_string()).or_insert(held).clone())
    }

    fn held(&self, on: &str) -> Option<Held> {
        let sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.get(on).cloned()
    }

    fn mark(&self, on: &str, catalog: &str, made: &str) {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(held) = sessions.get_mut(on) {
            held.catalogs.insert(catalog.to_string(), made.to_string());
        }
    }

    fn mode_is(&self, on: &str, mode: RuntimeMode) {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(held) = sessions.get_mut(on) {
            held.streaming = mode == RuntimeMode::Streaming;
        }
    }

    /// Forgets the session of `on`: one the Gateway closed already.
    fn forget(&self, on: &str) -> Option<Held> {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.remove(on)
    }

    /// Forgets the session of `on` and closes it in the Gateway.
    pub(crate) fn close_session(&self, on: &str) {
        if let Some(held) = self.forget(on) {
            close(&self.inner.http, &self.inner.url, &held.handle);
        }
    }

    /// Submits `text` in session `session`, fetches its result to its end, and closes the operation; dropped before,
    /// the operation is closed all the same, which ends the job of a query.
    async fn execute(
        &self,
        session: &str,
        text: &str,
        streaming: bool,
    ) -> std::result::Result<(SchemaRef, Vec<RecordBatch>), Failed> {
        let submitted: Submitted = self
            .call(
                reqwest::Method::POST,
                &format!("/v3/sessions/{session}/statements"),
                Some(json!({ "statement": text })),
            )
            .await?;
        let operation = format!("/v3/sessions/{session}/operations/{}", submitted.operation_handle);
        let mut running = Running {
            http: self.inner.http.clone(),
            url: format!("{}{operation}/close", self.inner.url),
            done: false,
        };
        let mut next = Some(format!("{operation}/result/0?rowFormat=JSON"));
        let mut columns: Option<Vec<Column>> = None;
        let mut rows: Vec<Row> = Vec::new();
        let mut query = false;
        while let Some(uri) = next.take() {
            let fetched: Fetched = match self.call(reqwest::Method::GET, &uri, None).await {
                Ok(fetched) => fetched,
                // A fetch the Gateway held past the client's wait: the same page is asked again.
                Err(Failed::Error(e)) if e.retryable => {
                    next = Some(uri);
                    continue;
                }
                Err(failed) => return Err(failed),
            };
            query |= fetched.is_query_result;
            // A query in streaming does not end: its rows would never all be there to answer. Its operation is closed
            // as this returns, which ends its job; rows answered as they come are a step of their own.
            if query && streaming {
                return Err(Error::usage(format!(
                    "flink at {}: a SELECT in streaming does not end, and USING answers a statement's rows once it                      ended: run it in batch, `SET 'execution.runtime-mode' = 'batch'` in the USING block or                      `mode = \"batch\"` in the engine's entry",
                    self.inner.url
                ))
                .into());
            }
            if let Some(results) = fetched.results {
                if columns.is_none() && !results.columns.is_empty() {
                    columns = Some(results.columns);
                }
                rows.extend(results.data);
            }
            match fetched.result_type {
                ResultType::Eos => break,
                ResultType::NotReady => tokio::time::sleep(NOT_READY).await,
                ResultType::Payload => {}
            }
            next = fetched.next_result_uri;
        }
        running.done = true;
        let _ = self
            .inner
            .http
            .delete(format!("{}{operation}/close", self.inner.url))
            .send()
            .await;
        let columns = columns.unwrap_or_default();
        if columns.is_empty() {
            return Ok((Arc::new(Schema::empty()), Vec::new()));
        }
        Ok(wire::arrow_of(&columns, &rows, streaming && query)?)
    }

    /// One call to the Gateway, its JSON answer read as `T`: 503 asked again, waiting longer each time; 401 and 403
    /// said as this side not let in; a session the Gateway does not have, said apart; anything else, Flink's cause.
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> std::result::Result<T, Failed> {
        let url = format!("{}{path}", self.inner.url);
        let mut wait = FIRST;
        for tried in 0..=AGAIN {
            let mut request = self.inner.http.request(method.clone(), &url);
            if let Some(body) = &body {
                request = request.json(body);
            }
            let answer = match request.send().await {
                Ok(answer) => answer,
                Err(e) if e.is_timeout() => {
                    let mut error = Error::failed(format!("flink at {}: no answer in time", self.inner.url));
                    error.retryable = true;
                    return Err(error.into());
                }
                Err(e) => {
                    return Err(Error::failed(format!("flink at {} is not reached: {e}", self.inner.url)).into());
                }
            };
            let status = answer.status();
            if status == StatusCode::SERVICE_UNAVAILABLE && tried < AGAIN {
                tokio::time::sleep(wait).await;
                wait *= 4;
                continue;
            }
            let text = answer
                .text()
                .await
                .map_err(|e| Error::failed(format!("flink at {}: {e}", self.inner.url)))?;
            if status.is_success() {
                return serde_json::from_str(&text).map_err(|e| {
                    Error::failed(format!(
                        "flink at {}: an answer not as the Gateway writes one: {e}",
                        self.inner.url
                    ))
                    .into()
                });
            }
            if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                return Err(Error::usage(format!(
                    "flink at {} does not let this side in: {status}",
                    self.inner.url
                ))
                .into());
            }
            let cause = wire::cause(&text);
            if cause.contains("Session '") && cause.contains("does not exist") {
                return Err(Failed::SessionGone);
            }
            return Err(Error::failed(format!("flink at {}: {cause}", self.inner.url)).into());
        }
        Err(Error::failed(format!("flink at {} is not available", self.inner.url)).into())
    }
}

/// The mode a `SET 'execution.runtime-mode' = '…'` sets, if `text` is one.
fn mode_set(text: &str) -> Option<RuntimeMode> {
    let words: String = uncommented(text)
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase();
    let rest = words.strip_prefix("set'execution.runtime-mode'=")?;
    let value = rest.trim_end_matches(';').trim_matches('\'');
    RuntimeMode::parse(value).ok()
}

/// `text` without its SQL comments, `-- …` to the line's end and `/* … */`, outside quotes.
fn uncommented(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    while let Some(c) = chars.next() {
        if quoted {
            kept.push(c);
            quoted = c != '\'';
            continue;
        }
        match (c, chars.peek()) {
            ('\'', _) => {
                quoted = true;
                kept.push(c);
            }
            ('-', Some('-')) => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        kept.push('\n');
                        break;
                    }
                }
            }
            ('/', Some('*')) => {
                chars.next();
                let mut last = ' ';
                for c in chars.by_ref() {
                    if last == '*' && c == '/' {
                        break;
                    }
                    last = c;
                }
                kept.push(' ');
            }
            _ => kept.push(c),
        }
    }
    kept
}

/// Closes a Gateway session, in the background.
fn close(http: &reqwest::Client, url: &str, session: &str) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let request = http.delete(format!("{url}/v3/sessions/{session}"));
    runtime.spawn(async move {
        let _ = request.send().await;
    });
}

/// An operation running in the Gateway, closed if its result is not all taken: closing a query's ends its job.
struct Running {
    http: reqwest::Client,
    url: String,
    done: bool,
}

impl Drop for Running {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let request = self.http.delete(&self.url);
        runtime.spawn(async move {
            let _ = request.send().await;
        });
    }
}

impl Drop for Inner {
    /// Each session this side opened is closed.
    fn drop(&mut self) {
        let sessions = self.sessions.get_mut().map(std::mem::take).unwrap_or_default();
        for held in sessions.values() {
            close(&self.http, &self.url, &held.handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mode_set_is_read_as_flink_writes_it() {
        assert_eq!(
            mode_set("SET 'execution.runtime-mode' = 'batch'"),
            Some(RuntimeMode::Batch)
        );
        assert_eq!(
            mode_set("set 'execution.runtime-mode'='streaming';"),
            Some(RuntimeMode::Streaming)
        );
        assert_eq!(mode_set("SET 'table.dml-sync' = 'true'"), None);
        assert_eq!(mode_set("select 1"), None);
        assert_eq!(
            mode_set("-- for the rest\nSET /* the mode */ 'execution.runtime-mode' = 'batch'"),
            Some(RuntimeMode::Batch)
        );
        assert_eq!(mode_set("-- SET 'execution.runtime-mode' = 'batch'\nselect 1"), None);
    }

    #[test]
    fn a_token_variable_unset_is_said_before_anything_is_sent() {
        let config = EngineConfig {
            kind: super::super::EngineKind::Flink,
            url: "http://127.0.0.1:8083".into(),
            user: None,
            token_env: Some("METALAKE_TEST_FLINK_TOKEN_UNSET".into()),
            password_env: None,
            catalog: None,
            lake_url: None,
            mode: None,
        };
        let error = Flink::new("f", &config).unwrap_err().to_string();
        assert!(error.contains("$METALAKE_TEST_FLINK_TOKEN_UNSET"), "{error}");
    }
}
