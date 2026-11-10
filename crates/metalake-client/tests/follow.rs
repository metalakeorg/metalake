//! Following a table through the client (CHG-3 to CHG-5): where a reader is is the `_commit_seq` of the last rows it
//! dealt with, it goes on after it, ranges of commits chain without gaps, and a copy into another table that keeps that
//! number as its idempotency key takes each commit once across a restart.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::{Array, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use futures_util::{TryStreamExt, stream};
use metalake::NewTable;
use metalake_client::{Changes, Details, Follower, Kind, Lake, Session, StartAt, TablePath, WriterOptions};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

fn shape() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

/// A lake on the tests' Postgres, in a directory of its own, with tables `names` in `db`.
async fn lake(names: &[&str]) -> (Lake, PathBuf) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!("follow_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let dir = std::env::temp_dir().join(&schema);
    let warehouse = dir.join("data");
    let lake = Lake::create(
        &url(),
        Some(&warehouse.to_string_lossy()),
        Some(&schema),
        Some(&dir),
        Some(2),
    )
    .await
    .expect("a lake");
    let main = lake.catalog.branch("main").await.unwrap();
    main.transaction().create_namespace(&["db"]).commit().await.unwrap();
    for name in names {
        let (tx, _) = main
            .transaction()
            .create_table(NewTable {
                namespace: &["db"],
                name,
                schema: &shape(),
                location: None,
                cluster_key: vec![],
                partitioning: vec![],
                properties: vec![],
            })
            .unwrap();
        tx.commit().await.unwrap();
    }
    (lake, dir)
}

fn path(text: &str) -> TablePath {
    TablePath::parse(text).unwrap()
}

/// Appends `keys`, and answers the commit's sequence.
async fn append(lake: &Lake, to: &TablePath, keys: std::ops::Range<i64>, details: Details) -> i64 {
    let batch = RecordBatch::try_new(shape(), vec![Arc::new(Int64Array::from_iter_values(keys))]).unwrap();
    let written = lake
        .append(to, &shape(), stream::iter([Ok(batch)]), details)
        .await
        .unwrap();
    written.commit.expect("a commit")
}

/// A commit is in the feed once it is a second old (CHG-1).
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1150)).await;
}

/// Each row: `(what, key, commit)`, sorted.
async fn rows(changes: Changes) -> Vec<(String, i64, i64)> {
    let batches: Vec<RecordBatch> = changes.try_collect().await.unwrap();
    let mut rows = Vec::new();
    for b in &batches {
        let keys = b.column_by_name("k").unwrap().as_primitive::<Int64Type>();
        let what = b.column_by_name("_change_type").unwrap().as_string::<i32>();
        let seqs = b.column_by_name("_commit_seq").unwrap().as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            rows.push((what.value(i).to_string(), keys.value(i), seqs.value(i)));
        }
    }
    rows.sort();
    rows
}

/// Every row there is now.
async fn drain(follower: &mut Follower) -> Vec<(String, i64, i64)> {
    let mut all = Vec::new();
    while let Some(step) = follower.next().await.unwrap() {
        all.extend(rows(step).await);
    }
    all.sort();
    all
}

fn row(what: &str, key: i64, seq: i64) -> (String, i64, i64) {
    (what.to_string(), key, seq)
}

#[tokio::test]
async fn follow_goes_on_after_position() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    let first = append(&lake, &t, 0..3, Details::default()).await;

    let mut follower = lake.follow(&t, StartAt::Snapshot).await.unwrap();
    assert_eq!(follower.position(), None);
    let read = rows(follower.next().await.unwrap().expect("the read")).await;
    let at = follower.position().expect("the read's sequence");
    assert!(at >= first);
    assert_eq!(read, [row("insert", 0, at), row("insert", 1, at), row("insert", 2, at)]);

    let added = append(&lake, &t, 3..5, Details::default()).await;
    let session = Session::new(&lake).await.unwrap();
    let deleted = session.delete(&t, "k = 1", Details::default()).await.unwrap();
    let deleted = deleted.commit.expect("a commit");
    settle().await;
    assert_eq!(
        drain(&mut follower).await,
        [
            row("delete", 1, deleted),
            row("insert", 3, added),
            row("insert", 4, added)
        ]
    );
    assert_eq!(follower.position(), Some(deleted));

    // Another reader, from that number alone, takes only what comes after it.
    let mut again = lake.follow(&t, StartAt::After(deleted)).await.unwrap();
    assert_eq!(again.position(), Some(deleted));
    assert_eq!(drain(&mut again).await, []);
    let later = append(&lake, &t, 5..6, Details::default()).await;
    settle().await;
    assert_eq!(drain(&mut again).await, [row("insert", 5, later)]);
    assert_eq!(drain(&mut follower).await, [row("insert", 5, later)]);
    assert_eq!(again.position(), follower.position());

    // From now, nothing that was before.
    let mut latest = lake.follow(&t, StartAt::Latest).await.unwrap();
    assert_eq!(drain(&mut latest).await, []);
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn changes_between_commits() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    let one = append(&lake, &t, 0..2, Details::default()).await;
    let two = append(&lake, &t, 2..4, Details::default()).await;
    let session = Session::new(&lake).await.unwrap();
    let three = session.delete(&t, "k = 0", Details::default()).await.unwrap();
    let three = three.commit.expect("a commit");
    settle().await;

    let (schema, between) = lake.changes(&t, one, Some(two)).await.unwrap();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["k", "_change_type", "_commit_seq", "_commit_timestamp"]);
    let first = rows(between).await;
    assert_eq!(first, [row("insert", 2, two), row("insert", 3, two)]);
    let rest = rows(lake.changes(&t, two, None).await.unwrap().1).await;
    assert_eq!(rest, [row("delete", 0, three)]);

    // (one, two] then (two, three] is (one, three].
    let mut chained = [first, rest].concat();
    chained.sort();
    assert_eq!(rows(lake.changes(&t, one, Some(three)).await.unwrap().1).await, chained);

    let backwards = lake.changes(&t, two, Some(one)).await.err().expect("refused");
    assert_eq!(backwards.kind, Kind::Usage, "{}", backwards.message);
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

/// What a copy takes of a step: its inserted keys.
async fn inserted(step: Changes) -> Vec<RecordBatch> {
    let batches: Vec<RecordBatch> = step.try_collect().await.unwrap();
    batches
        .iter()
        .map(|b| RecordBatch::try_new(shape(), vec![b.column_by_name("k").unwrap().clone()]).unwrap())
        .collect()
}

/// A copier: it follows `source` from where its writer's key is, and writes each step into `copy` with the step's
/// position as its version: one step, or until it is past `until`. It answers where its key stopped.
async fn copier(lake: &Arc<Lake>, until: Option<i64>) -> Option<i64> {
    let options = WriterOptions {
        app_id: Some("copier".into()),
        commit_every: Duration::from_secs(3600),
        ..Default::default()
    };
    let mut writer = lake.writer(&path("db.copy"), options).await.unwrap();
    let start = writer.version().map_or(StartAt::Snapshot, StartAt::After);
    let mut follower = lake.follow(&path("db.source"), start).await.unwrap();
    loop {
        let step = follower.wait(Duration::from_millis(50)).await.unwrap();
        let batches = inserted(step).await;
        // Once its rows are read, the step counts.
        let position = follower.position();
        for batch in batches {
            writer.write(batch, position).await.unwrap();
        }
        writer.flush().await.unwrap();
        if until.is_none_or(|until| position >= Some(until)) {
            return writer.version();
        }
    }
}

#[tokio::test]
async fn copy_with_key_takes_each_commit_once() {
    let (lake, dir) = lake(&["source", "copy"]).await;
    let lake = Arc::new(lake);
    let source = path("db.source");
    append(&lake, &source, 0..3, Details::default()).await;

    // A copier takes the table as it is, then falls over.
    let first = copier(&lake, None).await.expect("its key");

    // While it is down, commits come; one that comes back goes on after its key, and takes each of them once.
    let mut last = 0;
    for n in 1..4i64 {
        last = append(&lake, &source, n * 10..n * 10 + 3, Details::default()).await;
    }
    let reached = copier(&lake, Some(last)).await.expect("its key");
    assert!(reached > first);
    assert_eq!(reached, last);

    let session = Session::new(&lake).await.unwrap();
    let keys = |name: &str| format!("select k from db.{name} order by k");
    let read = |batches: Vec<RecordBatch>| -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect()
    };
    let copied = read(
        session
            .run(&keys("copy"))
            .await
            .unwrap()
            .read()
            .unwrap()
            .try_collect()
            .await
            .unwrap(),
    );
    let original = read(
        session
            .run(&keys("source"))
            .await
            .unwrap()
            .read()
            .unwrap()
            .try_collect()
            .await
            .unwrap(),
    );
    assert_eq!(copied, original);
    assert_eq!(copied.len(), 12);
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn wait_takes_commit_when_it_comes() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    append(&lake, &t, 0..1, Details::default()).await;
    let mut follower = lake.follow(&t, StartAt::Latest).await.unwrap();
    let interval = Duration::from_millis(50);

    // The start comes at once, and has no rows.
    assert_eq!(rows(follower.wait(interval).await.unwrap()).await, []);
    let start = follower.position();

    // A wait dropped before anything came leaves the follower where it was.
    let dropped = tokio::time::timeout(Duration::from_millis(300), follower.wait(interval)).await;
    assert!(dropped.is_err(), "nothing came");
    assert_eq!(follower.position(), start);

    // A commit that lands while it waits is what it answers, once it is a second old.
    let later = append(&lake, &t, 1..3, Details::default()).await;
    let came = tokio::time::timeout(Duration::from_secs(10), follower.wait(interval))
        .await
        .expect("the commit came");
    assert_eq!(
        rows(came.unwrap()).await,
        [row("insert", 1, later), row("insert", 2, later)]
    );
    assert_eq!(follower.position(), Some(later));
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

/// Changes the columns of `db.t` on `main` in one commit, as the specification's `fields` says (§6).
async fn define(lake: &Lake, fields: Vec<serde_json::Value>) {
    let main = lake.catalog.branch("main").await.unwrap();
    let t = main.table(&["db"], "t").await.unwrap();
    main.transaction()
        .define(t.id(), serde_json::json!({ "fields": fields }))
        .commit()
        .await
        .unwrap();
}

/// A new id for a field of `db.t`.
async fn field_id(lake: &Lake) -> i32 {
    let main = lake.catalog.branch("main").await.unwrap();
    let t = main.table(&["db"], "t").await.unwrap();
    let pool = metalake_pg::Pool::new(&url(), 1).unwrap();
    let session = pool.get().await.unwrap();
    metalake_pg::Protocol::new(lake.schema())
        .next_id(&session, t.id(), "field")
        .await
        .unwrap()
}

#[tokio::test]
async fn follow_goes_on_after_added_column() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    append(&lake, &t, 0..2, Details::default()).await;
    let mut follower = lake.follow(&t, StartAt::Snapshot).await.unwrap();
    rows(follower.next().await.unwrap().expect("the read")).await;

    // A commit under the old columns, then one that adds a column, then a row that has it.
    let before = append(&lake, &t, 10..12, Details::default()).await;
    let id = field_id(&lake).await;
    define(
        &lake,
        vec![serde_json::json!({"field_id": id, "new": true, "position": 2, "name": "v", "type": "string"})],
    )
    .await;
    let wider = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]));
    let values: Vec<datafusion::arrow::array::ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![2])),
        Arc::new(datafusion::arrow::array::StringArray::from(vec![Some("two")])),
    ];
    let row_of_it = RecordBatch::try_new(wider.clone(), values).unwrap();
    let after = lake
        .append(&t, &wider, stream::iter([Ok(row_of_it)]), Details::default())
        .await
        .unwrap()
        .commit
        .unwrap();
    settle().await;

    // It goes on under the new columns: the rows of the earlier commit have the new one null.
    let mut seen = Vec::new();
    while let Some(step) = follower.next().await.unwrap() {
        let batches: Vec<RecordBatch> = step.try_collect().await.unwrap();
        for b in &batches {
            let keys = b.column_by_name("k").unwrap().as_primitive::<Int64Type>();
            let v = b.column_by_name("v").expect("the new column").as_string::<i32>();
            let seqs = b.column_by_name("_commit_seq").unwrap().as_primitive::<Int64Type>();
            for i in 0..b.num_rows() {
                seen.push((
                    keys.value(i),
                    v.is_valid(i).then(|| v.value(i).to_string()),
                    seqs.value(i),
                ));
            }
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        [
            (2, Some("two".to_string()), after),
            (10, None, before),
            (11, None, before)
        ]
    );
    assert!(follower.schema().column_with_name("v").is_some());
    assert_eq!(follower.position(), Some(after));
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn follow_stops_at_renamed_column() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    append(&lake, &t, 0..2, Details::default()).await;
    let mut follower = lake.follow(&t, StartAt::Latest).await.unwrap();
    rows(follower.next().await.unwrap().expect("its start")).await;
    let at = follower.position().unwrap();

    let main = lake.catalog.branch("main").await.unwrap();
    let k = main.table(&["db"], "t").await.unwrap().fields()[0].clone();
    define(
        &lake,
        vec![
            serde_json::json!({"field_id": k.field_id, "position": k.position, "name": "key", "type": k.ty,
                                "required": k.required}),
        ],
    )
    .await;
    let renamed = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(renamed.clone(), vec![Arc::new(Int64Array::from(vec![5]))]).unwrap();
    lake.append(&t, &renamed, stream::iter([Ok(batch)]), Details::default())
        .await
        .unwrap();
    settle().await;

    let stopped = follower.next().await.err().expect("refused");
    assert!(stopped.message.contains("k renamed to key"), "{}", stopped.message);
    assert!(stopped.message.contains(&format!("after {at}")), "{}", stopped.message);
    assert_eq!(follower.position(), Some(at), "where to follow again from");
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn follow_step_not_read_given_again() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    let first = append(&lake, &t, 0..2, Details::default()).await;
    let mut follower = lake.follow(&t, StartAt::After(first)).await.unwrap();
    let added = append(&lake, &t, 2..4, Details::default()).await;
    settle().await;

    // Its rows dropped before their end: the step does not count, and comes again.
    let dropped = follower.next().await.unwrap().expect("the commit");
    drop(dropped);
    assert_eq!(follower.position(), Some(first));
    let again = rows(follower.next().await.unwrap().expect("the commit again")).await;
    assert_eq!(again, [row("insert", 2, added), row("insert", 3, added)]);
    assert_eq!(follower.position(), Some(added));
    assert_eq!(drain(&mut follower).await, []);
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn follow_dropped_read_given_again_at_its_point() {
    let (lake, dir) = lake(&["t"]).await;
    let t = path("db.t");
    append(&lake, &t, 0..2, Details::default()).await;
    let mut follower = lake.follow(&t, StartAt::Snapshot).await.unwrap();
    let read = follower.next().await.unwrap().expect("the read");
    drop(read);
    let read_at = follower.position();
    assert_eq!(read_at, None, "the read does not count");

    // A commit lands before the read is given again: the read is the one it was, and the commit comes after it.
    let later = append(&lake, &t, 2..3, Details::default()).await;
    settle().await;
    let again = rows(follower.next().await.unwrap().expect("the read again")).await;
    let at = follower.position().expect("the read's sequence");
    assert!(at < later, "the read is at its first point, before {later}: {at}");
    assert_eq!(again, [row("insert", 0, at), row("insert", 1, at)]);
    assert_eq!(drain(&mut follower).await, [row("insert", 2, later)]);
    lake.close();
    let _ = std::fs::remove_dir_all(dir);
}
