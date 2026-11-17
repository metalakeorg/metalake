//! A statement sent to Trino by its client protocol (`POST /v1/statement`, then each `nextUri` until there is none),
//! with the lake's catalog as the session's, so that `demo.t` is the lake's `demo.t`. Its rows come back as JSON, each
//! column with its Trino type, and are made Arrow: each type Arrow has exactly, from its text; any other, as its text.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::Value;

use super::EngineConfig;
use crate::{Error, Result};

/// How long a statement may wait for the next of its pages before it is given up: Trino answers each within a second
/// while it runs, so this is for a coordinator that is gone.
const PAGE: Duration = Duration::from_secs(300);
/// How many times a page Trino asks to be asked again for (502, 503, 504) is asked again.
const AGAIN: u32 = 5;

/// A Trino the lake names, ready to be sent statements.
#[derive(Debug, Clone)]
pub struct Trino {
    url: String,
    catalog: String,
    headers: HeaderMap,
    http: reqwest::Client,
}

/// A page of a statement's answer.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    next_uri: Option<String>,
    columns: Option<Vec<Column>>,
    data: Option<Vec<Vec<Value>>>,
    error: Option<Failed>,
}

#[derive(Debug, Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    trino_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Failed {
    message: String,
    error_name: Option<String>,
}

impl Trino {
    /// The engine `config` names, its secret read from the environment now: a variable named and unset is said here,
    /// before anything is sent.
    pub fn new(name: &str, config: &EngineConfig) -> Result<Self> {
        let secret = |variable: &str| {
            std::env::var(variable)
                .map_err(|_| Error::usage(format!("the engine `{name}` signs in by ${variable}, which is not set")))
        };
        let mut headers = HeaderMap::new();
        let mut set = |key: &'static str, value: &str| -> Result<()> {
            let value = HeaderValue::from_str(value)
                .map_err(|_| Error::usage(format!("the engine `{name}`: {key} cannot be `{value}`")))?;
            headers.insert(key, value);
            Ok(())
        };
        let user = config.user.as_deref().unwrap_or("metalake");
        set("x-trino-user", user)?;
        set("x-trino-source", "metalake")?;
        set("x-trino-catalog", config.catalog())?;
        match (&config.token_env, &config.password_env) {
            (Some(token), _) => {
                let token = secret(token)?;
                set(AUTHORIZATION.as_str(), &format!("Bearer {token}"))?;
            }
            (None, Some(password)) => {
                use base64::Engine;
                let password = secret(password)?;
                let basic = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
                set(AUTHORIZATION.as_str(), &format!("Basic {basic}"))?;
            }
            (None, None) => {}
        }
        let http = reqwest::Client::builder()
            .timeout(PAGE)
            .build()
            .map_err(|e| Error::failed(format!("a client of Trino: {e}")))?;
        Ok(Self {
            url: config.url.trim_end_matches('/').to_string(),
            catalog: config.catalog().to_string(),
            headers,
            http,
        })
    }

    /// The lake's catalog in this Trino.
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Runs one statement, and answers its rows. A statement Trino refuses fails with Trino's own words.
    pub async fn sql(&self, text: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let mut page = self
            .page(
                self.http
                    .post(format!("{}/v1/statement", self.url))
                    .body(text.to_string()),
            )
            .await?;
        let mut columns: Option<Vec<Column>> = None;
        let mut rows: Vec<Vec<Value>> = Vec::new();
        loop {
            if let Some(failed) = page.error {
                let name = failed.error_name.map(|n| format!("{n}: ")).unwrap_or_default();
                return Err(Error::failed(format!("trino: {name}{}", failed.message)));
            }
            if columns.is_none() {
                columns = page.columns;
            }
            rows.extend(page.data.unwrap_or_default());
            match page.next_uri {
                Some(next) => page = self.page(self.http.get(next)).await?,
                None => break,
            }
        }
        arrow_of(&columns.unwrap_or_default(), &rows)
    }

    /// One page, asked again while Trino says it is busy.
    async fn page(&self, request: reqwest::RequestBuilder) -> Result<Page> {
        let reach = |e: reqwest::Error| Error::failed(format!("trino at {}: {e}", self.url));
        for again in 0..=AGAIN {
            let asked = request
                .try_clone()
                .expect("a statement's body is text")
                .headers(self.headers.clone())
                .send()
                .await
                .map_err(reach)?;
            let status = asked.status();
            if matches!(status.as_u16(), 502..=504) && again < AGAIN {
                tokio::time::sleep(Duration::from_millis(100 << again)).await;
                continue;
            }
            if status.as_u16() == 401 {
                return Err(Error::usage(format!("trino at {} refuses who asks: 401", self.url)));
            }
            if !status.is_success() {
                let said = asked.text().await.unwrap_or_default();
                return Err(Error::failed(format!("trino at {}: {status}: {said}", self.url)));
            }
            return asked.json::<Page>().await.map_err(reach);
        }
        unreachable!("the last time is answered")
    }
}

/// The Arrow type a Trino type is, when Arrow has it exactly; `None` for one shown as its text.
fn arrow_type(trino_type: &str) -> Option<DataType> {
    let base = trino_type.split('(').next().unwrap_or(trino_type).trim();
    let precision = || {
        let inside = trino_type.split_once('(')?.1.trim_end_matches(')');
        let mut parts = inside.split(',').map(|p| p.trim().parse::<i32>());
        Some((parts.next()?.ok()?, parts.next().and_then(|p| p.ok()).unwrap_or(0)))
    };
    Some(match base {
        "boolean" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "decimal" => {
            let (p, s) = precision()?;
            DataType::Decimal128(u8::try_from(p).ok()?, i8::try_from(s).ok()?)
        }
        "timestamp" if !trino_type.contains("with time zone") => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => return None,
    })
}

/// A value as text: a string as it is; a number or a boolean as written; anything else, as its JSON.
fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        other => Some(other.to_string()),
    }
}

/// Trino's rows, one batch: each column of a type Arrow has exactly in it, cast from its text; a value that will not
/// cast leaves its column as text, rather than lose it.
fn arrow_of(columns: &[Column], rows: &[Vec<Value>]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (at, column) in columns.iter().enumerate() {
        let texts: StringArray = rows.iter().map(|row| row.get(at).and_then(text)).collect();
        let texts: ArrayRef = Arc::new(texts);
        let typed = arrow_type(&column.trino_type).and_then(|to| cast_with_options(&texts, &to, &strict).ok());
        let array = typed.unwrap_or(texts);
        fields.push(Field::new(&column.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    if columns.is_empty() {
        return Ok((schema, Vec::new()));
    }
    Ok((schema.clone(), vec![RecordBatch::try_new(schema, arrays)?]))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::{Date32Type, Int64Type};
    use serde_json::json;

    use super::*;

    fn column(name: &str, trino_type: &str) -> Column {
        Column {
            name: name.into(),
            trino_type: trino_type.into(),
        }
    }

    #[test]
    fn each_type_arrow_has_is_made_arrow_and_any_other_text() {
        let columns = [
            column("n", "bigint"),
            column("s", "varchar(10)"),
            column("d", "date"),
            column("x", "decimal(10,2)"),
            column("at", "timestamp(3)"),
            column("tz", "timestamp(3) with time zone"),
            column("a", "array(integer)"),
            column("f", "double"),
        ];
        let rows = vec![
            vec![
                json!(9007199254740993_i64),
                json!("a"),
                json!("2026-11-15"),
                json!("12.34"),
                json!("2026-11-15 10:00:00.123"),
                json!("2026-11-15 10:00:00.123 UTC"),
                json!([1, 2]),
                json!("NaN"),
            ],
            vec![Value::Null; 8],
        ];
        let (schema, batches) = arrow_of(&columns, &rows).unwrap();
        let types: Vec<_> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(
            types,
            [
                DataType::Int64,
                DataType::Utf8,
                DataType::Date32,
                DataType::Decimal128(10, 2),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Utf8,
                DataType::Utf8,
                DataType::Float64,
            ]
        );
        let batch = &batches[0];
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 9007199254740993);
        assert_eq!(batch.column(2).as_primitive::<Date32Type>().value(0), 20772);
        assert_eq!(batch.column(6).as_string::<i32>().value(0), "[1,2]");
        assert!(
            batch
                .column(7)
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                .value(0)
                .is_nan()
        );
        assert!((0..8).all(|c| batch.column(c).is_null(1)));
    }

    #[test]
    fn a_value_that_will_not_cast_keeps_its_column_as_text() {
        let (schema, _) = arrow_of(&[column("n", "bigint")], &[vec![json!("not a number")]]).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    mod protocol {
        use std::sync::{Arc, Mutex};

        use axum::Router;
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::routing::{get, post};
        use serde_json::json;

        use super::super::*;
        use crate::{EngineConfig, EngineKind};

        /// A Trino that answers as Trino does: the statement asked, then a page that says the next, then the rows. It
        /// keeps the headers of each request; `fail` it refuses, `busy` is answered 503 once, `nobody` 401.
        #[derive(Clone, Default)]
        struct Fake {
            base: Arc<Mutex<String>>,
            heard: Arc<Mutex<Vec<HeaderMap>>>,
            busy: Arc<Mutex<bool>>,
        }

        async fn statement(State(fake): State<Fake>, headers: HeaderMap, body: String) -> (StatusCode, String) {
            fake.heard.lock().unwrap().push(headers);
            let base = fake.base.lock().unwrap().clone();
            match body.as_str() {
                "nobody" => (StatusCode::UNAUTHORIZED, String::new()),
                "show schemas from elsewhere" => (
                    StatusCode::OK,
                    json!({"id": "q", "error": {"message": "Catalog 'elsewhere' not found", "errorName": "CATALOG_NOT_FOUND"}})
                        .to_string(),
                ),
                "fail" => (
                    StatusCode::OK,
                    json!({"id": "q", "error": {"message": "line 1:1: mismatched input", "errorName": "SYNTAX_ERROR"}})
                        .to_string(),
                ),
                _ => (
                    StatusCode::OK,
                    json!({"id": "q", "nextUri": format!("{base}/v1/statement/q/1"), "stats": {"state": "QUEUED"}})
                        .to_string(),
                ),
            }
        }

        async fn page(State(fake): State<Fake>, headers: HeaderMap) -> (StatusCode, String) {
            fake.heard.lock().unwrap().push(headers);
            let mut busy = fake.busy.lock().unwrap();
            if !*busy {
                *busy = true;
                return (StatusCode::SERVICE_UNAVAILABLE, String::new());
            }
            let rows = json!({
                "id": "q",
                "columns": [{"name": "region", "type": "varchar"}, {"name": "n", "type": "bigint"}],
                "data": [["eu", 3], ["us", 5]],
                "stats": {"state": "FINISHED"}
            });
            (StatusCode::OK, rows.to_string())
        }

        async fn serve() -> (String, Fake) {
            let fake = Fake::default();
            let app = Router::new()
                .route("/v1/statement", post(statement))
                .route("/v1/statement/q/1", get(page))
                .with_state(fake.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            *fake.base.lock().unwrap() = base.clone();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (base, fake)
        }

        fn config(url: &str) -> EngineConfig {
            EngineConfig {
                kind: EngineKind::Trino,
                url: format!("{url}/"),
                user: Some("ana".into()),
                token_env: Some("METALAKE_TEST_TRINO_TOKEN".into()),
                password_env: None,
                catalog: Some("lake".into()),
                lake_url: None,
                mode: None,
            }
        }

        #[tokio::test]
        async fn a_statement_follows_its_pages_and_answers_arrow() {
            let (url, fake) = serve().await;
            // SAFETY: set before any thread of this test reads it; no other test reads this variable.
            unsafe { std::env::set_var("METALAKE_TEST_TRINO_TOKEN", "s3cret") };
            let trino = Trino::new("trino_prod", &config(&url)).unwrap();
            let (schema, batches) = trino
                .sql("select region, count(*) from demo.t group by 1")
                .await
                .unwrap();
            assert_eq!(schema.field(1).data_type(), &DataType::Int64);
            assert_eq!(batches[0].num_rows(), 2);

            let heard = fake.heard.lock().unwrap();
            assert_eq!(heard.len(), 3, "the statement, a 503 asked again, and its page");
            for headers in heard.iter() {
                assert_eq!(headers["x-trino-user"], "ana");
                assert_eq!(headers["x-trino-catalog"], "lake");
                assert_eq!(headers["authorization"], "Bearer s3cret");
            }
        }

        #[tokio::test]
        async fn what_trino_refuses_is_said_in_its_words() {
            let (url, _) = serve().await;
            let mut config = config(&url);
            config.token_env = None;
            let trino = Trino::new("trino_prod", &config).unwrap();
            let refused = trino.sql("fail").await.unwrap_err().to_string();
            assert!(
                refused.contains("SYNTAX_ERROR: line 1:1: mismatched input"),
                "{refused}"
            );
            let nobody = trino.sql("nobody").await.unwrap_err().to_string();
            assert!(nobody.contains("401"), "{nobody}");
        }

        #[tokio::test]
        async fn a_secret_named_and_unset_is_said_before_anything_is_sent() {
            let mut config = config("http://127.0.0.1:1");
            config.token_env = Some("METALAKE_TEST_UNSET_TOKEN".into());
            let said = Trino::new("trino_prod", &config).unwrap_err().to_string();
            assert!(said.contains("$METALAKE_TEST_UNSET_TOKEN, which is not set"), "{said}");
        }

        #[tokio::test]
        async fn a_check_says_which_link_holds() {
            let (url, _) = serve().await;
            let mut config = config(&url);
            config.token_env = None;
            let remote = crate::Remote::new("trino_prod", &config).unwrap();
            let checked = remote.check().await;
            assert!(checked.holds(), "{checked:?}");

            config.catalog = Some("elsewhere".into());
            let checked = crate::Remote::new("trino_prod", &config).unwrap().check().await;
            assert!(checked.engine.is_ok());
            assert!(checked.lake.unwrap_err().contains("CATALOG_NOT_FOUND"));

            config.url = "http://127.0.0.1:1".into();
            let checked = crate::Remote::new("trino_prod", &config).unwrap().check().await;
            assert!(checked.engine.is_err());
            assert!(checked.lake.clone().unwrap_err().contains("not asked"));
            let (_, rows) = checked.rows("trino_prod").unwrap();
            assert_eq!(rows.num_rows(), 2);
        }
    }
}
