//! Flink's client against a Gateway that answers as the tests say ([`super::fake`]).

use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};

use super::fake::{Fake, serve};
use super::{CatalogStatement, Flink};
use crate::engine::{EngineConfig, EngineKind, RuntimeMode};

fn config(url: &str, mode: Option<RuntimeMode>, token_env: Option<&str>) -> EngineConfig {
    EngineConfig {
        kind: EngineKind::Flink,
        url: url.into(),
        user: None,
        token_env: token_env.map(str::to_string),
        password_env: None,
        catalog: None,
        lake_url: None,
        mode,
    }
}

async fn flink(mode: Option<RuntimeMode>) -> (Flink, Fake) {
    let fake = Fake::default();
    let url = serve(fake.clone()).await;
    (Flink::new("f", &config(&url, mode, None)).unwrap(), fake)
}

fn numbers(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let n = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..n.len()).map(|i| n.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

fn catalog(name: &str) -> CatalogStatement {
    (
        name.to_string(),
        format!("CREATE CATALOG `{name}` WITH ('type' = 'iceberg')"),
    )
}

/// Waits a little for what the client sends in the background, as a close.
async fn until(fake: &Fake, what: impl Fn(&super::fake::Heard) -> bool) {
    for _ in 0..50 {
        if what(&fake.heard.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_query_in_batch_follows_its_pages_to_their_end() {
    let (flink, fake) = flink(Some(RuntimeMode::Batch)).await;
    let (schema, batches) = flink.sql("select n from t").await.unwrap();
    assert_eq!(schema.fields().len(), 1, "no kind in batch");
    assert_eq!(numbers(&batches), [1, 2]);
    let heard = fake.heard.lock().unwrap();
    let properties = &heard.opened[0].1;
    assert_eq!(properties["execution.runtime-mode"], "batch");
    assert_eq!(properties["table.dml-sync"], "true");
    assert_eq!(heard.closed_operations.len(), 1, "the operation closed at its end");
}

#[tokio::test]
async fn a_query_in_streaming_is_refused_and_its_operation_closed() {
    let (flink, fake) = flink(None).await;
    let error = flink.sql("select n from t").await.unwrap_err();
    assert!(matches!(error.kind, crate::Kind::Usage));
    assert!(
        error.to_string().contains("'execution.runtime-mode' = 'batch'"),
        "{error}"
    );
    until(&fake, |h| !h.closed_operations.is_empty()).await;
    let heard = fake.heard.lock().unwrap();
    assert_eq!(heard.closed_operations.len(), 1);
    assert!(
        heard.opened[0].1.get("execution.runtime-mode").is_none(),
        "Flink's own default"
    );
}

#[tokio::test]
async fn a_mode_set_in_the_block_holds_for_what_follows() {
    let (flink, fake) = flink(None).await;
    flink.sql("SET 'execution.runtime-mode' = 'batch'").await.unwrap();
    let (_, batches) = flink.sql("select n from t").await.unwrap();
    assert_eq!(numbers(&batches), [1, 2]);
    let heard = fake.heard.lock().unwrap();
    let said: Vec<&str> = heard.statements.iter().map(|(_, s)| s.as_str()).collect();
    assert_eq!(
        said,
        [
            "SET 'execution.runtime-mode' = 'batch'",
            "SET 'table.dml-sync' = 'true'",
            "select n from t"
        ]
    );
}

#[tokio::test]
async fn a_refs_catalogs_are_created_once_on_a_session_of_its_own() {
    let (flink, fake) = flink(Some(RuntimeMode::Batch)).await;
    let catalogs = [catalog("default"), catalog("demo")];
    flink.sql_on("main", &catalogs, "select n").await.unwrap();
    flink.sql_on("main", &catalogs, "select n").await.unwrap();
    flink.sql_on("dev", &catalogs[..1], "select n").await.unwrap();
    let heard = fake.heard.lock().unwrap();
    assert_eq!(heard.opened.len(), 2, "a session for main, one for dev");
    let (main, dev) = (heard.opened[0].0.as_str(), heard.opened[1].0.as_str());
    let said: Vec<(&str, &str)> = heard.statements.iter().map(|(s, t)| (s.as_str(), t.as_str())).collect();
    assert_eq!(
        said,
        [
            (main, "CREATE CATALOG `default` WITH ('type' = 'iceberg')"),
            (main, "CREATE CATALOG `demo` WITH ('type' = 'iceberg')"),
            (main, "select n"),
            (main, "select n"),
            (dev, "CREATE CATALOG `default` WITH ('type' = 'iceberg')"),
            (dev, "select n"),
        ]
    );
}

#[tokio::test]
async fn a_catalog_made_otherwise_is_made_on_a_session_opened_anew() {
    let (flink, fake) = flink(Some(RuntimeMode::Batch)).await;
    let moved = (
        "default".to_string(),
        "CREATE CATALOG `default` WITH ('uri' = 'http://elsewhere')".to_string(),
    );
    flink.sql_on("main", &[catalog("default")], "select n").await.unwrap();
    flink.sql_on("main", &[moved], "select n").await.unwrap();
    until(&fake, |h| !h.closed_sessions.is_empty()).await;
    let heard = fake.heard.lock().unwrap();
    assert_eq!(heard.opened.len(), 2);
    assert_eq!(heard.closed_sessions, [heard.opened[0].0.clone()], "the first closed");
}

#[tokio::test]
async fn a_reaped_session_is_opened_again_with_its_catalogs() {
    let (flink, fake) = flink(Some(RuntimeMode::Batch)).await;
    flink.sql_on("main", &[catalog("default")], "select n").await.unwrap();
    let first = fake.heard.lock().unwrap().opened[0].0.clone();
    fake.heard.lock().unwrap().gone.insert(first);
    let (_, batches) = flink.sql_on("main", &[catalog("default")], "select n").await.unwrap();
    assert_eq!(numbers(&batches), [1, 2]);
    let heard = fake.heard.lock().unwrap();
    assert_eq!(heard.opened.len(), 2);
    let made = heard
        .statements
        .iter()
        .filter(|(_, s)| s.starts_with("CREATE CATALOG"))
        .count();
    assert_eq!(made, 2, "the catalog made again on the new session");
}

#[tokio::test]
async fn a_refusal_says_flinks_deepest_cause_and_a_busy_gateway_is_asked_again() {
    let (flink, _) = flink(Some(RuntimeMode::Batch)).await;
    let error = flink.sql("fail").await.unwrap_err().to_string();
    assert!(
        error.ends_with("ValidationException: Object 'nope' not found"),
        "{error}"
    );
    flink.sql("busy").await.unwrap();
}

#[tokio::test]
async fn a_statement_dropped_unanswered_has_its_operation_closed() {
    let (flink, fake) = flink(Some(RuntimeMode::Batch)).await;
    let waited = tokio::time::timeout(Duration::from_millis(300), flink.sql("slow")).await;
    assert!(waited.is_err(), "it never answers");
    until(&fake, |h| !h.closed_operations.is_empty()).await;
    assert_eq!(fake.heard.lock().unwrap().closed_operations.len(), 1);
}

#[tokio::test]
async fn each_call_bears_the_token() {
    let fake = Fake::default();
    let url = serve(fake.clone()).await;
    // SAFETY: a variable no other test reads.
    unsafe { std::env::set_var("METALAKE_TEST_FLINK_TOKEN", "the-token") };
    let config = config(&url, Some(RuntimeMode::Batch), Some("METALAKE_TEST_FLINK_TOKEN"));
    let flink = Flink::new("f", &config).unwrap();
    flink.sql("select n").await.unwrap();
    let heard = fake.heard.lock().unwrap();
    assert!(!heard.tokens.is_empty());
    assert!(
        heard.tokens.iter().all(|t| t.as_deref() == Some("Bearer the-token")),
        "{:?}",
        heard.tokens
    );
}
