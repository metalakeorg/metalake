//! A run of several statements over a lake (`POST /api/v1/sql/run`'s backend): one session for all of them, each
//! answered as it ends, the first that fails the last that runs. On the Postgres of `sql/test.sh`:
//!
//!     CRATE=metalake-ui bash sql/test.sh --test run

#![cfg(feature = "lake")]

use std::sync::atomic::{AtomicU32, Ordering};

use futures_util::StreamExt;
use metalake_client::Lake;
use metalake_ui::api::{Backend, Statement, StepAnswer, StepOutcome};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-ui bash sql/test.sh")
}

/// A lake on the tests' Postgres, in a directory of its own, with the namespace `db`.
async fn lake() -> (Lake, std::path::PathBuf) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!("ui_run_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
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
    let main = lake.catalog.branch("main").await.unwrap();
    main.transaction().create_namespace(&["db"]).commit().await.unwrap();
    (lake, dir)
}

fn statement(sql: &str, limit: Option<usize>) -> Statement {
    Statement {
        sql: sql.into(),
        branch: None,
        limit,
    }
}

async fn run(lake: &Lake, sql: &str, limit: Option<usize>) -> Vec<StepAnswer> {
    let answers = lake
        .run(statement(sql, limit))
        .await
        .unwrap_or_else(|f| panic!("{sql}: {}", f.message));
    answers.collect().await
}

async fn done(lake: Lake, dir: std::path::PathBuf) {
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn each_statement_is_answered_and_the_first_failure_ends_the_run() {
    let (lake, dir) = lake().await;
    let answers = run(
        &lake,
        "create table db.t (a int);\ninsert into db.t values (1), (2);\nselect a from db.t order by a;\nselect nope;\nselect 1",
        None,
    )
    .await;
    let texts: Vec<&str> = answers.iter().map(|a| a.sql.as_str()).collect();
    assert_eq!(
        texts,
        [
            "create table db.t (a int)",
            "insert into db.t values (1), (2)",
            "select a from db.t order by a",
            "select nope",
            "select 1"
        ]
    );
    assert!(
        matches!(answers[0].outcome, StepOutcome::Done),
        "{:?}",
        answers[0].outcome
    );
    assert!(
        matches!(
            answers[1].outcome,
            StepOutcome::Written {
                commit: Some(_),
                rows: 2
            }
        ),
        "{:?}",
        answers[1].outcome
    );
    // The read after the write, on the same session, sees it.
    let StepOutcome::Rows(rows) = &answers[2].outcome else {
        panic!("{:?}", answers[2].outcome)
    };
    assert_eq!((rows.rows, rows.truncated), (2, false));
    assert!(
        matches!(answers[3].outcome, StepOutcome::Failed(_)),
        "{:?}",
        answers[3].outcome
    );
    assert!(matches!(answers[4].outcome, StepOutcome::Skipped));
    assert!(answers[..4].iter().all(|a| a.ms.is_some()) && answers[4].ms.is_none());
    assert_eq!(answers.iter().map(|a| a.index).collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
    done(lake, dir).await;
}

#[tokio::test]
async fn a_read_answers_its_first_rows_and_says_there_were_more() {
    let (lake, dir) = lake().await;
    let answers = run(
        &lake,
        "select * from unnest(range(0, 10)) as t(n); select 1 as one",
        Some(3),
    )
    .await;
    let StepOutcome::Rows(rows) = &answers[0].outcome else {
        panic!("{:?}", answers[0].outcome)
    };
    assert_eq!((rows.rows, rows.truncated), (3, true));
    let StepOutcome::Rows(rows) = &answers[1].outcome else {
        panic!("{:?}", answers[1].outcome)
    };
    assert_eq!((rows.rows, rows.truncated), (1, false));
    done(lake, dir).await;
}

#[tokio::test]
async fn a_text_with_no_statement_fails_before_any_runs() {
    let (lake, dir) = lake().await;
    assert!(lake.run(statement("  -- nothing\n", None)).await.is_err());
    done(lake, dir).await;
}

/// A `USING` alone is a step of its own, which runs nothing and says the engine it chooses; a statement on the lake's
/// own engine says none. (DuckDB itself runs in Python, where `test_using.py` runs it.)
#[tokio::test]
async fn using_alone_is_a_step_that_says_its_engine() {
    let (lake, dir) = lake().await;
    let answers = run(
        &lake,
        "using duckdb; using metalake select 1 as one; using metalake",
        None,
    )
    .await;
    let seen: Vec<(&str, Option<&str>)> = answers.iter().map(|a| (a.sql.as_str(), a.engine.as_deref())).collect();
    assert_eq!(
        seen,
        [
            ("using duckdb", Some("duckdb")),
            ("using metalake select 1 as one", None),
            ("using metalake", None),
        ]
    );
    assert!(
        matches!(answers[0].outcome, StepOutcome::Done),
        "{:?}",
        answers[0].outcome
    );
    assert!(
        matches!(answers[1].outcome, StepOutcome::Rows(_)),
        "{:?}",
        answers[1].outcome
    );
    done(lake, dir).await;
}
