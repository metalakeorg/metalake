//! C14: a continuous pipeline through the client, as each face will drive it: created by name and query alone, kept
//! up to date with its source through inserts and deletes, suspended with a checkpoint, resumed with what came
//! meanwhile, and dropped. It runs Arroyo, so it needs its binary or Docker with its image: it runs where
//! `METALAKE_PIPELINE_TEST` is set (the end-to-end workflow), and passes trivially elsewhere.

use std::time::Duration;

use datafusion::arrow::util::pretty::pretty_format_batches;
use futures_util::TryStreamExt;
use metalake_client::{Lake, PipelineOptions, PipelineState, Session};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

async fn sql(session: &Session, text: &str) -> String {
    let prepared = session.run(text).await.unwrap_or_else(|e| panic!("{text}: {e}"));
    let batches: Vec<_> = prepared.read().unwrap().try_collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

const WANT: &str = "select customer, count(*) as orders, sum(amount) as amount from shop.orders \
                    group by customer order by customer";
const GOT: &str = "select customer, orders, amount from shop.totals order by customer";

/// Until the totals the pipeline keeps are what the orders give.
async fn settled(session: &Session, what: &str) {
    let want = sql(session, WANT).await;
    let mut got = String::new();
    for _ in 0..90 {
        session.refresh().await.unwrap();
        got = sql(session, GOT).await;
        if got == want {
            println!("{what}: settled");
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!("{what}: the totals never came to the orders'\nwant:\n{want}\ngot:\n{got}");
}

#[tokio::test]
async fn c14_pipeline_lifecycle() {
    if std::env::var_os("METALAKE_PIPELINE_TEST").is_none() {
        return;
    }
    let schema = format!("pipeline_{}", std::process::id());
    let dir = std::env::temp_dir().join(&schema);
    let warehouse = dir.join("data");
    let lake = Lake::create(
        &url(),
        Some(&warehouse.to_string_lossy()),
        Some(&schema),
        Some(&dir),
        Some(4),
    )
    .await
    .expect("a lake");
    let session = Session::new(&lake).await.unwrap();
    for statement in [
        "create schema main.shop",
        "create table main.shop.orders (id bigint, customer varchar, amount double)",
        "create table main.shop.totals (customer varchar, orders bigint, amount double)",
        "insert into main.shop.orders values (1, 'ana', 10), (2, 'ana', 20), (3, 'bo', 5), (4, 'cy', 7)",
    ] {
        sql(&session, statement).await;
    }

    let query = "INSERT INTO shop.totals SELECT customer, count(*) AS orders, sum(amount) AS amount \
                 FROM shop.orders GROUP BY customer";
    let created = lake
        .create_pipeline("totals", query, PipelineOptions::default())
        .await
        .unwrap();
    assert_eq!(created.state, PipelineState::Running, "{:?}", created.state);
    assert_eq!(created.writes.len(), 1);
    settled(&session, "snapshot").await;

    // A name is taken once, and a table with commits is not written by a new pipeline unless emptied.
    let again = lake
        .create_pipeline("totals", query, PipelineOptions::default())
        .await
        .unwrap_err();
    assert!(again.message.contains("exists"), "{}", again.message);
    let other = lake
        .create_pipeline("other", query, PipelineOptions::default())
        .await
        .unwrap_err();
    assert!(other.message.contains("has rows"), "{}", other.message);

    sql(
        &session,
        "insert into main.shop.orders values (5, 'bo', 15), (6, 'dee', 30)",
    )
    .await;
    settled(&session, "inserts").await;
    sql(&session, "delete from main.shop.orders where customer = 'cy'").await;
    settled(&session, "a customer gone").await;

    // Suspended with a checkpoint; what comes meanwhile comes once it is resumed.
    let suspended = lake.suspend_pipeline("totals").await.unwrap();
    assert_eq!(suspended.state, PipelineState::Suspended);
    sql(
        &session,
        "insert into main.shop.orders values (7, 'ana', 1), (8, 'eve', 2)",
    )
    .await;
    let resumed = lake.resume_pipeline("totals").await.unwrap();
    assert_eq!(resumed.state, PipelineState::Running, "{:?}", resumed.state);
    settled(&session, "resumed from its checkpoint").await;
    sql(&session, "delete from main.shop.orders where customer = 'dee'").await;
    settled(&session, "after resuming").await;

    let listed = lake.pipelines().await.unwrap();
    assert_eq!(listed.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), ["totals"]);
    assert!(listed[0].reached[0].1.is_some(), "it committed into its table");

    lake.drop_pipeline("totals").await.unwrap();
    assert!(lake.pipelines().await.unwrap().is_empty());
    assert!(
        lake.pipeline("totals")
            .await
            .unwrap_err()
            .message
            .contains("no pipeline")
    );
}
