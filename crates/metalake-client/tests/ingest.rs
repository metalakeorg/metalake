//! A producer's writer into a table: it commits on its cadence, by time or by what it holds; with a key, each commit is
//! idempotent with the producer's version, a producer that comes back goes on after the version the key reached and
//! what it gives again of before is skipped; an upsert keeps the last row of each key; two producers with one key are
//! told apart.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::Int64Type;
use futures_util::TryStreamExt;
use metalake::NewTable;
use metalake_client::{Kind, Lake, Mode, Session, TablePath, Writer, WriterOptions, Wrote};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

fn shape() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]))
}

/// A lake on the tests' Postgres, in a directory of its own, with the table `db.t`.
async fn lake() -> (Arc<Lake>, PathBuf) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!("ingest_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
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
    let (tx, _) = main
        .transaction()
        .create_table(NewTable {
            namespace: &["db"],
            name: "t",
            schema: &shape(),
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![],
        })
        .unwrap();
    tx.commit().await.unwrap();
    (Arc::new(lake), dir)
}

fn t() -> TablePath {
    TablePath::parse("db.t").unwrap()
}

fn rows(pairs: &[(i64, &str)]) -> RecordBatch {
    let keys = Int64Array::from_iter_values(pairs.iter().map(|p| p.0));
    let values = StringArray::from_iter_values(pairs.iter().map(|p| p.1));
    RecordBatch::try_new(shape(), vec![Arc::new(keys), Arc::new(values)]).unwrap()
}

fn keyed(app_id: &str) -> WriterOptions {
    WriterOptions {
        app_id: Some(app_id.into()),
        commit_every: Duration::from_secs(3600),
        ..Default::default()
    }
}

/// What `db.t` holds, by key.
async fn table(lake: &Lake) -> Vec<(i64, String)> {
    let session = Session::new(lake).await.unwrap();
    let read = session.run("select k, v from db.t order by k, v").await.unwrap();
    let batches: Vec<RecordBatch> = read.read().unwrap().try_collect().await.unwrap();
    let mut all = Vec::new();
    for b in &batches {
        let (k, v) = (b.column(0).as_primitive::<Int64Type>(), b.column(1).as_string::<i32>());
        all.extend((0..b.num_rows()).map(|i| (k.value(i), v.value(i).to_string())));
    }
    all
}

fn pairs(list: &[(i64, &str)]) -> Vec<(i64, String)> {
    list.iter().map(|(k, v)| (*k, v.to_string())).collect()
}

async fn done(lake: Arc<Lake>, dir: PathBuf) {
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn writer_commits_on_cadence() {
    let (lake, dir) = lake().await;
    let options = WriterOptions {
        commit_every: Duration::from_millis(300),
        ..keyed("cadence")
    };
    let mut writer = lake.writer(&t(), options).await.unwrap();
    assert_eq!(writer.version(), None);

    // Held until its time comes and another version with it: then what it held is committed, whole.
    assert!(matches!(
        writer.write(rows(&[(1, "a")]), Some(1)).await.unwrap(),
        Wrote::Buffered
    ));
    assert!(matches!(
        writer.write(rows(&[(2, "b")]), Some(2)).await.unwrap(),
        Wrote::Buffered
    ));
    assert_eq!(table(&lake).await, []);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        matches!(writer.write(rows(&[(3, "c")]), Some(2)).await.unwrap(), Wrote::Buffered),
        "the same version"
    );
    match writer.write(rows(&[(4, "d")]), Some(3)).await.unwrap() {
        Wrote::Committed(written) => assert_eq!(written.rows, 3),
        other => panic!("{other:?}"),
    }
    assert_eq!(writer.version(), Some(2));
    assert_eq!(lake.key_version("main", "cadence").await.unwrap(), Some(2));
    assert_eq!(table(&lake).await, pairs(&[(1, "a"), (2, "b"), (3, "c")]));
    writer.close().await.unwrap().expect("what it held");
    assert_eq!(lake.key_version("main", "cadence").await.unwrap(), Some(3));

    // Or as soon as what it holds is as large as it may be.
    let options = WriterOptions {
        max_bytes: 1,
        ..keyed("bytes")
    };
    let mut small = lake.writer(&t(), options).await.unwrap();
    assert!(matches!(
        small.write(rows(&[(5, "e")]), Some(1)).await.unwrap(),
        Wrote::Buffered
    ));
    assert!(matches!(
        small.write(rows(&[(6, "f")]), Some(2)).await.unwrap(),
        Wrote::Committed(_)
    ));
    assert_eq!(
        small.close().await.unwrap().map(|w| w.rows),
        Some(1),
        "the last version"
    );
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_goes_on_after_its_key() {
    let (lake, dir) = lake().await;
    let mut first = lake.writer(&t(), keyed("producer")).await.unwrap();
    for v in 1..=3 {
        first.write(rows(&[(v, "first")]), Some(v)).await.unwrap();
    }
    first.flush().await.unwrap().expect("a commit");
    // It falls over holding rows it never committed.
    first.write(rows(&[(4, "lost")]), Some(4)).await.unwrap();
    drop(first);

    // A producer that comes back asks where it was, and gives what came after; what it gives of before is skipped.
    let mut again = lake.writer(&t(), keyed("producer")).await.unwrap();
    assert_eq!(again.version(), Some(3));
    for v in 2..=3 {
        let wrote = again.write(rows(&[(v, "again")]), Some(v)).await.unwrap();
        assert!(matches!(wrote, Wrote::Skipped), "{v}: {wrote:?}");
    }
    again.write(rows(&[(4, "again")]), Some(4)).await.unwrap();
    again.write(rows(&[(5, "again")]), Some(5)).await.unwrap();
    let refused = again.write(rows(&[(6, "late")]), Some(4)).await.unwrap_err();
    assert_eq!(refused.kind, Kind::Usage, "{}", refused.message);
    again.close().await.unwrap().expect("a commit");
    assert_eq!(
        table(&lake).await,
        pairs(&[(1, "first"), (2, "first"), (3, "first"), (4, "again"), (5, "again")])
    );
    assert_eq!(lake.key_version("main", "producer").await.unwrap(), Some(5));
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_versions_need_a_key() {
    let (lake, dir) = lake().await;
    let mut plain = lake.writer(&t(), WriterOptions::default()).await.unwrap();
    let refused = plain.write(rows(&[(1, "a")]), Some(1)).await.unwrap_err();
    assert_eq!(refused.kind, Kind::Usage, "{}", refused.message);
    assert!(matches!(
        plain.write(rows(&[(1, "a")]), None).await.unwrap(),
        Wrote::Buffered
    ));
    plain.close().await.unwrap().expect("a commit");

    let mut keyed = lake.writer(&t(), keyed("k")).await.unwrap();
    let refused = keyed.write(rows(&[(2, "b")]), None).await.unwrap_err();
    assert_eq!(refused.kind, Kind::Usage, "{}", refused.message);
    let wrong = Writer::close(keyed).await.unwrap();
    assert!(wrong.is_none(), "nothing was taken");
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_upsert_keeps_last_of_key() {
    let (lake, dir) = lake().await;
    let options = WriterOptions {
        mode: Mode::Upsert(vec!["k".into()]),
        ..keyed("upsert")
    };
    let mut writer = lake.writer(&t(), options.clone()).await.unwrap();
    writer.write(rows(&[(1, "a"), (2, "x")]), Some(1)).await.unwrap();
    writer.write(rows(&[(1, "b")]), Some(2)).await.unwrap();
    writer.close().await.unwrap().expect("a commit");
    assert_eq!(table(&lake).await, pairs(&[(1, "b"), (2, "x")]));

    let mut later = lake.writer(&t(), options).await.unwrap();
    later.write(rows(&[(1, "c"), (3, "y")]), Some(3)).await.unwrap();
    later.close().await.unwrap().expect("a commit");
    assert_eq!(table(&lake).await, pairs(&[(1, "c"), (2, "x"), (3, "y")]));

    let unknown = WriterOptions {
        mode: Mode::Upsert(vec!["nope".into()]),
        ..Default::default()
    };
    let refused = lake.writer(&t(), unknown).await.err().expect("refused");
    assert_eq!(refused.kind, Kind::Usage, "{}", refused.message);
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_second_producer_conflicts() {
    let (lake, dir) = lake().await;
    let mut one = lake.writer(&t(), keyed("shared")).await.unwrap();
    let mut two = lake.writer(&t(), keyed("shared")).await.unwrap();
    one.write(rows(&[(1, "one")]), Some(5)).await.unwrap();
    one.flush().await.unwrap().expect("a commit");

    // The other took the key past where this one read it.
    two.write(rows(&[(2, "two")]), Some(3)).await.unwrap();
    let refused = two.flush().await.unwrap_err();
    assert_eq!(refused.kind, Kind::Conflict, "{}", refused.message);
    assert_eq!(table(&lake).await, pairs(&[(1, "one")]));
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_empty_write_lends_no_version() {
    let (lake, dir) = lake().await;
    let mut writer = lake.writer(&t(), keyed("empty")).await.unwrap();
    writer.write(rows(&[(1, "a")]), Some(1)).await.unwrap();
    assert!(matches!(
        writer.write(rows(&[]), Some(2)).await.unwrap(),
        Wrote::Buffered
    ));
    writer.close().await.unwrap().expect("a commit");
    assert_eq!(
        lake.key_version("main", "empty").await.unwrap(),
        Some(1),
        "the rows were of 1"
    );

    // The rows of 2 that come later are not taken for done.
    let mut again = lake.writer(&t(), keyed("empty")).await.unwrap();
    assert!(matches!(
        again.write(rows(&[(2, "b")]), Some(2)).await.unwrap(),
        Wrote::Buffered
    ));
    again.close().await.unwrap().expect("a commit");
    assert_eq!(table(&lake).await, pairs(&[(1, "a"), (2, "b")]));
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_lost_answer_is_its_own_commit() {
    let (lake, dir) = lake().await;
    let mut writer = lake.writer(&t(), keyed("lost")).await.unwrap();
    writer.write(rows(&[(1, "a")]), Some(5)).await.unwrap();

    // Its send applied, and the answer never came back: the key stands at its version.
    let details = metalake_client::Details {
        idempotent: Some(("lost".into(), 5)),
        ..Default::default()
    };
    let sent = futures_util::stream::iter([Ok(rows(&[(1, "a")]))]);
    let applied = lake.append(&t(), &shape(), sent, details).await.unwrap();

    let written = writer.flush().await.unwrap().expect("what it held");
    assert_eq!(written.commit, applied.commit);
    assert!(!written.applied, "it applied before");
    assert_eq!(writer.version(), Some(5));
    writer.write(rows(&[(2, "b")]), Some(6)).await.unwrap();
    writer.close().await.unwrap().expect("a commit");
    assert_eq!(table(&lake).await, pairs(&[(1, "a"), (2, "b")]));
    done(lake, dir).await;
}

#[tokio::test]
async fn writer_stale_producer_ahead_conflicts() {
    let (lake, dir) = lake().await;
    let mut one = lake.writer(&t(), keyed("shared")).await.unwrap();
    let mut two = lake.writer(&t(), keyed("shared")).await.unwrap();
    one.write(rows(&[(1, "one")]), Some(5)).await.unwrap();
    one.flush().await.unwrap().expect("a commit");

    // A version past the other's is no less another writer's key.
    two.write(rows(&[(2, "two")]), Some(6)).await.unwrap();
    let refused = two.flush().await.unwrap_err();
    assert_eq!(refused.kind, Kind::Conflict, "{}", refused.message);
    assert_eq!(table(&lake).await, pairs(&[(1, "one")]));
    assert_eq!(lake.key_version("main", "shared").await.unwrap(), Some(5));
    done(lake, dir).await;
}
