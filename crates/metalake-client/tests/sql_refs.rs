//! A ref's whole life in SQL (I5), end to end on the tests' Postgres: create, use, write, merge, log, tag, retain and
//! drop, each a statement of the session that `metalake sql`, `lake.sql` and Query all run, answering rows.

use std::sync::atomic::{AtomicU32, Ordering};

use datafusion::arrow::util::pretty::pretty_format_batches;
use futures_util::TryStreamExt;
use metalake_client::{Lake, Session};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

/// A lake of its own on the tests' Postgres, with `demo.t` holding 1 and 2 on `main`.
async fn lake() -> Lake {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!(
        "refs_sql_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let dir = std::env::temp_dir().join(&schema);
    let lake = Lake::create(
        &url(),
        Some(&dir.join("data").to_string_lossy()),
        Some(&schema),
        Some(&dir),
        Some(2),
    )
    .await
    .expect("a lake");
    let session = Session::new(&lake).await.unwrap();
    sql(
        &session,
        "create schema demo; create table demo.t (k bigint); insert into demo.t values (1), (2)",
    )
    .await;
    lake
}

/// What a text answers, as a table.
async fn sql(session: &Session, text: &str) -> String {
    let prepared = session.run(text).await.unwrap_or_else(|e| panic!("{text}: {e}"));
    let batches: Vec<_> = prepared.read().unwrap().try_collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

/// The message a text is refused with.
async fn refused(session: &Session, text: &str) -> String {
    match session.run(text).await {
        Ok(_) => panic!("{text}: not refused"),
        Err(e) => e.message,
    }
}

#[tokio::test]
async fn i5_a_branch_lives_its_whole_life_in_sql() {
    let lake = lake().await;
    let session = Session::new(&lake).await.unwrap();

    let created = sql(&session, "create branch dev").await;
    assert!(
        created.contains("| dev ") && created.contains("branch") && created.contains("main"),
        "{created}"
    );
    assert!(
        sql(&session, "create branch if not exists dev")
            .await
            .contains("| dev "),
        "IF NOT EXISTS answers it"
    );
    assert!(
        !refused(&session, "create branch dev").await.is_empty(),
        "a name taken is refused"
    );

    // USE moves the session: a name that says no ref is the ref's, as `lake.on` makes it.
    let used = sql(
        &session,
        "use branch dev; insert into demo.t values (3); select count(*) as n from demo.t",
    )
    .await;
    assert!(used.contains("| 3 "), "{used}");
    assert_eq!(session.current_ref(), "dev");
    assert!(
        sql(&session, "select count(*) as n from main.demo.t")
            .await
            .contains("| 2 "),
        "main untouched"
    );
    assert!(refused(&session, "use tag dev").await.contains("no tag dev"));

    let shown = sql(&session, "show branches").await;
    assert!(shown.contains("| main ") && shown.contains("| dev "), "{shown}");
    let log = sql(&session, "show log limit 1").await;
    assert!(
        log.contains("commit_seq") && log.lines().count() == 5,
        "the latest commit on dev: {log}"
    );

    let check = sql(&session, "merge branch dev dry run").await;
    assert!(check.contains("would merge"), "{check}");
    assert!(
        sql(&session, "select count(*) as n from main.demo.t")
            .await
            .contains("| 2 "),
        "a dry run merges nothing"
    );
    let merged = sql(&session, "merge branch dev message 'from sql' author 'ana'").await;
    assert!(merged.contains("merged") && merged.contains("demo.t"), "{merged}");
    assert!(
        sql(&session, "select count(*) as n from main.demo.t")
            .await
            .contains("| 3 "),
        "merged into main"
    );

    let tagged = sql(&session, "create tag v1 from main").await;
    assert!(tagged.contains("| v1 ") && tagged.contains("tag"), "{tagged}");
    assert!(sql(&session, "show tags").await.contains("| v1 "));

    let kept = sql(&session, "alter branch dev retain 30 days 3 snapshots").await;
    assert!(kept.contains("2592000") && kept.contains("| 3 "), "{kept}");
    assert!(sql(&session, "alter lake grace 7 days").await.contains("604800"));

    assert!(
        refused(&session, "drop branch dev force")
            .await
            .contains("the ref this session is on")
    );
    assert!(refused(&session, "drop branch dev").await.contains("FORCE"));
    sql(&session, "use branch main").await;
    assert!(sql(&session, "drop branch dev force").await.contains("| dev "));
    assert!(!sql(&session, "show branches").await.contains("| dev "), "gone");
    assert!(
        sql(&session, "drop branch if exists dev force")
            .await
            .contains("deleted"),
        "nothing, and no error"
    );
    assert!(refused(&session, "use branch dev").await.contains("no branch dev"));
}

#[tokio::test]
async fn i5_a_ref_verb_under_using_stays_the_clients() {
    let lake = lake().await;
    let session = Session::new(&lake).await.unwrap();
    // No DuckDB here: were it sent to the engine, it would fail; the client's verbs never are.
    let shown = sql(&session, "using duckdb; show branches").await;
    assert!(shown.contains("| main "), "{shown}");
}
