//! Following a table with no engine (CHG-3, CHG-4). A reader takes what a table holds, then each commit as rows inserted
//! and deleted, so that applying them rebuilds the table: with commits landing while it starts, across a restart from its
//! cursor, and past compactions, which change no row. One that fell behind what a ref keeps fails, and never misses a
//! commit in silence.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{
    CHANGE_TYPE, COMMIT_SEQ, COMMIT_TIMESTAMP, Catalog, CatalogConfig, Error, FileAdd, NewTable, Outcome, Ref,
    Retention, Scan, Start, Storage, StreamCursor, StreamStep, Table, TableStream, TableWriter, WriteOptions,
};
use metalake_pg::{Commit, Pool, Protocol};
use object_store::ObjectStoreExt;
use serde_json::json;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn lake(properties: Vec<(&str, &str)>) -> Result<(Catalog, Ref, Storage), Error> {
    lake_in(&fresh_schema(), properties).await
}

async fn lake_in(schema: &str, properties: Vec<(&str, &str)>) -> Result<(Catalog, Ref, Storage), Error> {
    let config = CatalogConfig::new(url(), schema).warehouse("memory:///wh");
    let catalog = Catalog::init(config, None).await?;
    let main = catalog.branch("main").await?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0..1).schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties,
    })?;
    tx.commit().await?;
    Ok((catalog, main, Storage::in_memory()))
}

async fn add_table(main: &Ref, name: &str) -> Result<(), Error> {
    let (tx, _) = main.transaction().create_table(NewTable {
        namespace: &["db"],
        name,
        schema: &batch(0..1).schema(),
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    Ok(())
}

async fn append_to(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    name: &str,
    keys: std::ops::Range<i64>,
) -> Result<i64, Error> {
    let t = on.table(&["db"], name).await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    Ok(on.transaction().append(writer.finish().await?).commit().await?.seq())
}

async fn append(catalog: &Catalog, on: &Ref, storage: &Storage, keys: std::ops::Range<i64>) -> Result<i64, Error> {
    append_to(catalog, on, storage, "t", keys).await
}

/// Deletes the rows `chosen` keeps, as an engine does.
async fn delete(catalog: &Catalog, on: &Ref, storage: &Storage, chosen: impl Fn(i64) -> bool) -> Result<i64, Error> {
    let t = on.table(&["db"], "t").await?;
    let scan = Scan::new(catalog, &t).with_positions();
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut deletes = Deletes::new();
    for b in &batches {
        let keys = b.column(0).as_primitive::<Int64Type>();
        let keep: Vec<u32> = (0..b.num_rows() as u32)
            .filter(|i| chosen(keys.value(*i as usize)))
            .collect();
        deletes.add(&arrow_select::take::take_record_batch(b, &arrow_array::UInt32Array::from(keep)).unwrap())?;
    }
    Ok(on.transaction().delete(&t, storage, deletes).commit().await?.seq())
}

async fn keys_of(catalog: &Catalog, t: &Table, storage: &Storage) -> Result<Vec<i64>, Error> {
    let scan = Scan::new(catalog, t);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

/// A commit is in the feed once it is a second old (CHG-1).
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1150)).await;
}

/// The time a commit has in the catalog (TT-1), read apart from the feed.
async fn commit_time(schema: &str, seq: i64) -> Result<std::time::SystemTime, Box<dyn std::error::Error>> {
    let pool = Pool::new(&url(), 1)?;
    let row = pool
        .get()
        .await?
        .query_one(&format!("select {schema}.commit_time($1)"), &[&seq])
        .await?;
    Ok(row.get(0))
}

/// The rows of a step: each `(what, key, commit)`, sorted.
async fn rows_of(stream: &TableStream, step: &StreamStep, storage: &Storage) -> Result<Vec<(String, i64, i64)>, Error> {
    let mut rows = Vec::new();
    for task in &step.tasks {
        let batches: Vec<RecordBatch> = stream.open(task.clone(), storage).try_collect().await?;
        for b in &batches {
            assert_eq!(b.schema(), *stream.schema());
            let keys = b.column(0).as_primitive::<Int64Type>();
            let what = b.column_by_name(CHANGE_TYPE).unwrap().as_string::<i32>();
            let seqs = b.column_by_name(COMMIT_SEQ).unwrap().as_primitive::<Int64Type>();
            for i in 0..b.num_rows() {
                rows.push((what.value(i).to_string(), keys.value(i), seqs.value(i)));
            }
        }
    }
    rows.sort();
    Ok(rows)
}

/// Every row there is now.
async fn drain(stream: &mut TableStream, storage: &Storage) -> Result<Vec<(String, i64, i64)>, Error> {
    settle().await;
    let mut rows = Vec::new();
    while let Some(step) = stream.next().await? {
        rows.extend(rows_of(stream, &step, storage).await?);
    }
    Ok(rows)
}

/// The table those rows leave.
fn rebuilt(changes: &[(String, i64, i64)]) -> Vec<i64> {
    let mut table: Vec<i64> = Vec::new();
    let mut ordered = changes.to_vec();
    ordered.sort_by_key(|(what, _, seq)| (*seq, what == "insert"));
    for (what, key, _) in ordered {
        match what.as_str() {
            "insert" => table.push(key),
            _ => {
                let at = table.iter().position(|k| *k == key);
                table.remove(at.unwrap_or_else(|| panic!("deleted {key}, which is not there")));
            }
        }
    }
    table.sort_unstable();
    table
}

fn row(what: &str, key: i64, seq: i64) -> (String, i64, i64) {
    (what.to_string(), key, seq)
}

#[tokio::test]
async fn follow_each_change() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![("inline.flush-bytes", "1")]).await?;
    let first = append(&catalog, &main, &storage, 0..3).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Snapshot).await?;

    // What the table holds, at the sequence read.
    let start = stream.next().await?.expect("the first read");
    let read = rows_of(&stream, &start, &storage).await?;
    assert_eq!(read.iter().map(|r| r.1).collect::<Vec<_>>(), [0, 1, 2]);
    assert!(read.iter().all(|r| r.0 == "insert" && r.2 >= first));
    assert_eq!(stream.next().await?.map(|s| s.tasks), None, "nothing new");

    // Files added, one stored and one inline.
    let t2 = main.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(
        &catalog,
        &t2,
        storage.clone(),
        WriteOptions {
            stored: true,
            ..Default::default()
        },
    )?;
    writer.write(&batch(3..6)).await?;
    let stored = main.transaction().append(writer.finish().await?).commit().await?.seq();
    let inline = append(&catalog, &main, &storage, 6..8).await?;
    assert_eq!(
        drain(&mut stream, &storage).await?,
        [
            row("insert", 3, stored),
            row("insert", 4, stored),
            row("insert", 5, stored),
            row("insert", 6, inline),
            row("insert", 7, inline),
        ]
    );

    // A vector delivers only what it took: the second one, not the rows of the first again.
    let deleted = delete(&catalog, &main, &storage, |k| k == 4).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [row("delete", 4, deleted)]);
    let more = delete(&catalog, &main, &storage, |k| k == 5).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [row("delete", 5, more)]);

    // A file removed delivers the rows it had left.
    let t3 = main.table(&["db"], "t").await?;
    let tasks: Vec<_> = Scan::new(&catalog, &t3).plan().await?.try_collect().await?;
    let with_vector = tasks.iter().find(|f: &&metalake::ScanTask| f.vector.is_some()).unwrap();
    let removed = main
        .transaction()
        .remove(&t3, [with_vector.file_id])
        .commit()
        .await?
        .seq();
    assert_eq!(drain(&mut stream, &storage).await?, [row("delete", 3, removed)]);

    // A flush and a compaction change no row.
    let t4 = main.table(&["db"], "t").await?;
    let flushed = catalog.maintenance().flush(&t4, &storage).await?;
    assert!(flushed.is_some(), "the inline files were due");
    let t5 = main.table(&["db"], "t").await?;
    let compacted = catalog.maintenance().compact(&t5, &storage).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [], "{compacted:?}");
    let after = append(&catalog, &main, &storage, 8..9).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [row("insert", 8, after)]);
    Ok(())
}

#[tokio::test]
async fn each_change_has_its_commits_time() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let (catalog, main, storage) = lake_in(&schema, vec![("inline.flush-bytes", "1")]).await?;
    let first = append(&catalog, &main, &storage, 0..3).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Snapshot).await?;
    let read = stream.next().await?.expect("the first read");
    let added = append(&catalog, &main, &storage, 3..5).await?;
    let deleted = delete(&catalog, &main, &storage, |k| k == 1).await?;
    settle().await;
    let mut tail = Vec::new();
    while let Some(step) = stream.next().await? {
        tail.extend(step.tasks);
    }

    let mut time = std::collections::HashMap::new();
    for seq in [first, added, deleted] {
        time.insert(seq, commit_time(&schema, seq).await?);
    }

    // The first read has its own time: after what it holds, before what follows it.
    assert!(
        read.tasks
            .iter()
            .all(|r| time[&first] <= r.committed_at && r.committed_at <= time[&added])
    );
    assert_eq!(
        tail.iter().map(|r| (r.seq, r.committed_at)).collect::<Vec<_>>(),
        [(added, time[&added]), (deleted, time[&deleted])]
    );

    // And each row carries its task's, in microseconds.
    for task in read.tasks.into_iter().chain(tail) {
        let micros = task.committed_at.duration_since(std::time::UNIX_EPOCH)?.as_micros() as i64;
        let batches: Vec<RecordBatch> = stream.open(task, &storage).try_collect().await?;
        for b in &batches {
            let column = b
                .column_by_name(COMMIT_TIMESTAMP)
                .unwrap()
                .as_primitive::<TimestampMicrosecondType>();
            assert!(column.iter().all(|v| v == Some(micros)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn start_among_commits() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    append(&catalog, &main, &storage, 0..10).await?;
    let writer = {
        let (catalog, main, storage) = (catalog.clone(), main.clone(), storage.clone());
        tokio::spawn(async move {
            for n in 1..60i64 {
                append(&catalog, &main, &storage, n * 10..(n + 1) * 10).await?;
            }
            Ok::<_, Error>(())
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Snapshot).await?;
    let start = stream.next().await?.expect("the first read");
    let mut rows = rows_of(&stream, &start, &storage).await?;
    assert!(!writer.is_finished(), "the commits go on while the reader starts");
    writer.await??;
    rows.extend(drain(&mut stream, &storage).await?);
    assert_eq!(rebuilt(&rows), (0..600).collect::<Vec<_>>(), "each row once");
    Ok(())
}

#[tokio::test]
async fn start_over_a_commit_in_flight() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let (catalog, main, storage) = lake_in(&schema, vec![]).await?;
    append(&catalog, &main, &storage, 0..1).await?;
    let t = main.table(&["db"], "t").await?;

    // A commit that drew its sequence and has not ended when the reader takes its point.
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(1..2)).await?;
    let commit = Commit {
        ref_id: main.id(),
        operation: "append".into(),
        files: writer.finish().await?,
        ..Default::default()
    };
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    session.batch_execute("begin").await?;
    p.commit(&session, &commit).await?;

    let mut stream = main.stream(&t, Start::Snapshot).await?;
    let starting = tokio::spawn(async move {
        let step = stream.next().await?.expect("the first read");
        Ok::<_, Error>((stream, step))
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!starting.is_finished(), "its sequence is not stable yet");
    session.batch_execute("commit").await?;
    let (mut stream, step) = starting.await??;

    let mut rows = rows_of(&stream, &step, &storage).await?;
    rows.extend(drain(&mut stream, &storage).await?);
    assert_eq!(rebuilt(&rows), [0, 1], "in the read, and not again in the feed");
    Ok(())
}

/// What a sink has prepared for an epoch, as it comes back from a checkpoint.
async fn prepare(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    keys: std::ops::Range<i64>,
    options: WriteOptions,
) -> Result<Vec<FileAdd>, Box<dyn std::error::Error>> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), options)?;
    writer.write(&batch(keys)).await?;
    let files = writer.finish().await?;
    Ok(serde_json::from_slice(&serde_json::to_vec(&files)?)?)
}

fn stored(ttl: Duration) -> WriteOptions {
    WriteOptions {
        stored: true,
        intent_ttl: ttl,
        ..Default::default()
    }
}

#[tokio::test]
async fn commit_each_epoch_once() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    let t = main.table(&["db"], "t").await?;

    // One file inline, whose bytes only the checkpoint has, and one stored.
    let inline = prepare(&catalog, &t, &storage, 0..3, WriteOptions::default()).await?;
    assert!(inline[0].content.is_some());
    let first = main
        .transaction()
        .append(inline.clone())
        .idempotent("job", 1)
        .commit()
        .await?;
    assert!(matches!(first, Outcome::Committed(_)));
    let hour = Duration::from_secs(3600);
    let late = prepare(&catalog, &t, &storage, 3..6, stored(hour)).await?;
    main.transaction()
        .append(late.clone())
        .idempotent("job", 2)
        .commit()
        .await?;

    // A sink that fell over after those commits, and does them again, as its checkpoint says.
    main.restore_prepared("job", 1, &inline, &storage, hour).await?;
    main.restore_prepared("job", 2, &late, &storage, hour).await?;
    let again = main.transaction().append(inline).idempotent("job", 1).commit().await?;
    assert!(matches!(again, Outcome::AlreadyApplied(_)));
    main.transaction().append(late).idempotent("job", 2).commit().await?;
    let now = main.table(&["db"], "t").await?;
    assert_eq!(keys_of(&catalog, &now, &storage).await?, [0, 1, 2, 3, 4, 5]);
    Ok(())
}

#[tokio::test]
async fn restore_after_intent_expired() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    let t = main.table(&["db"], "t").await?;
    let files = prepare(&catalog, &t, &storage, 0..3, stored(Duration::from_millis(1))).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let refused = main
        .transaction()
        .append(files.clone())
        .idempotent("job", 1)
        .commit()
        .await;
    let refused = refused.expect_err("its intent expired");
    assert!(refused.to_string().contains("RET-7"), "{refused}");

    main.restore_prepared("job", 1, &files, &storage, Duration::from_secs(3600))
        .await?;
    assert_eq!(
        catalog.maintenance().orphans(&storage).await?,
        0,
        "renewed, it is no orphan"
    );
    main.transaction().append(files).idempotent("job", 1).commit().await?;
    let now = main.table(&["db"], "t").await?;
    assert_eq!(keys_of(&catalog, &now, &storage).await?, [0, 1, 2]);
    Ok(())
}

#[tokio::test]
async fn restore_refuses_what_collection_took() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    let t = main.table(&["db"], "t").await?;
    let files = prepare(&catalog, &t, &storage, 0..3, stored(Duration::from_millis(1))).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(catalog.maintenance().orphans(&storage).await?, 1);

    let hour = Duration::from_secs(3600);
    let gone = main
        .restore_prepared("job", 1, &files, &storage, hour)
        .await
        .expect_err("its object went");
    assert!(matches!(gone, Error::NotFound(_)), "{gone}");
    Ok(())
}

#[tokio::test]
async fn restore_leaves_what_committed() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    let t = main.table(&["db"], "t").await?;
    let hour = Duration::from_secs(3600);
    let files = prepare(&catalog, &t, &storage, 0..3, stored(hour)).await?;
    main.transaction()
        .append(files.clone())
        .idempotent("job", 2)
        .commit()
        .await?;

    // Its object may have left since, by a compaction: the commit stands, so nothing is wrong.
    let object = storage.path(files[0].path.as_deref().unwrap())?;
    storage.store().delete(&object).await?;
    main.restore_prepared("job", 1, &files, &storage, hour).await?;
    main.restore_prepared("job", 2, &files, &storage, hour).await?;
    Ok(())
}

#[tokio::test]
async fn resume_from_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    append(&catalog, &main, &storage, 0..5).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Snapshot).await?;
    let start = stream.next().await?.expect("the first read");
    let mut rows = rows_of(&stream, &start, &storage).await?;
    let mut saved: StreamCursor = start.cursor;

    for n in 1..5i64 {
        append(&catalog, &main, &storage, n * 5..(n + 1) * 5).await?;
        delete(&catalog, &main, &storage, |k| k == n).await?;
        settle().await;

        // A reader that fell over before it saved its cursor gets the same step again.
        let cursor = serde_json::to_string(&saved)?;
        let mut first = main.stream(&t, Start::Cursor(serde_json::from_str(&cursor)?)).await?;
        let lost = first.next().await?.expect("what it had");
        let mut second = main.stream(&t, Start::Cursor(serde_json::from_str(&cursor)?)).await?;
        let step = second.next().await?.expect("what it had");
        assert_eq!(lost, step);

        rows.extend(rows_of(&second, &step, &storage).await?);
        saved = step.cursor;
        assert_eq!(second.next().await?, None, "nothing after it");
    }
    let now = main.table(&["db"], "t").await?;
    assert_eq!(rebuilt(&rows), keys_of(&catalog, &now, &storage).await?);
    Ok(())
}

#[tokio::test]
async fn start_at_latest() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    append(&catalog, &main, &storage, 0..5).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Latest).await?;
    assert_eq!(stream.next().await?.expect("its cursor").tasks, []);
    let later = append(&catalog, &main, &storage, 5..6).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [row("insert", 5, later)]);
    Ok(())
}

#[tokio::test]
async fn start_from_a_time() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let (catalog, main, storage) = lake_in(&schema, vec![]).await?;
    let first = append(&catalog, &main, &storage, 0..2).await?;
    let from = commit_time(&schema, first).await?;
    let second = append(&catalog, &main, &storage, 2..3).await?;
    let on = main.as_of(from);
    let t = main.table(&["db"], "t").await?;

    // A reader from that time, given the table as it is now, reads what was made by then and follows what came after.
    let mut early = on.stream(&t, Start::Snapshot).await?;
    let read = early.next().await?.expect("the first read");
    let held = rows_of(&early, &read, &storage).await?;
    assert_eq!(held, [row("insert", 0, first), row("insert", 1, first)]);
    assert!(read.tasks.iter().all(|r| r.committed_at == from));

    // One that starts later, with a commit in between, takes the same: the same read, and the same commits after it.
    let third = append(&catalog, &main, &storage, 3..4).await?;
    let mut late = on.stream(&t, Start::Snapshot).await?;
    let again = late.next().await?.expect("the first read");
    assert_eq!(again.cursor, read.cursor);
    assert_eq!(rows_of(&late, &again, &storage).await?, held);
    let after = [row("insert", 2, second), row("insert", 3, third)];
    assert_eq!(drain(&mut early, &storage).await?, after);
    assert_eq!(drain(&mut late, &storage).await?, after);

    // From that time with no read, only what came after it.
    let mut only_after = on.stream(&t, Start::Latest).await?;
    assert_eq!(only_after.next().await?.expect("its cursor").tasks, []);
    assert_eq!(drain(&mut only_after, &storage).await?, after);

    // A time less than a second ago is waited for, not refused.
    let fourth = append(&catalog, &main, &storage, 4..5).await?;
    let now = main.as_of(commit_time(&schema, fourth).await?);
    let mut recent = now.stream(&t, Start::Snapshot).await?;
    let read = recent.next().await?.expect("the first read");
    assert_eq!(rows_of(&recent, &read, &storage).await?.len(), 5);

    // A reader from a cursor goes on from it, and is given no time.
    let cursor = read.cursor;
    assert!(on.stream(&t, Start::Cursor(cursor)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn behind_retention_fails() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    add_table(&main, "u").await?;
    append(&catalog, &main, &storage, 0..1).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Latest).await?;
    stream.next().await?;

    // Only another table moved: the floor passes the reader, and it missed nothing.
    append_to(&catalog, &main, &storage, "u", 0..1).await?;
    append_to(&catalog, &main, &storage, "u", 1..2).await?;
    main.set_retention(&Retention {
        min_snapshots: Some(2),
        ..Default::default()
    })
    .await?;
    let next = append(&catalog, &main, &storage, 1..2).await?;
    assert_eq!(drain(&mut stream, &storage).await?, [row("insert", 1, next)]);

    // Its table moved past the floor, and those commits are gone.
    append(&catalog, &main, &storage, 2..3).await?;
    append(&catalog, &main, &storage, 3..4).await?;
    append(&catalog, &main, &storage, 4..5).await?;
    catalog.set_collect_grace(Duration::ZERO).await?;
    catalog.maintenance().expire().await?;
    settle().await;
    let late = stream.next().await.expect_err("it fell behind");
    assert!(late.to_string().contains("keeps from"), "{late}");
    Ok(())
}

#[tokio::test]
async fn refuse_what_it_cannot_follow() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![]).await?;
    let seq = append(&catalog, &main, &storage, 0..1).await?;
    let t = main.table(&["db"], "t").await?;
    let dev = main.create_branch("dev").await?;
    let on_dev = dev.table(&["db"], "t").await?;
    let branch = dev
        .stream(&on_dev, Start::Snapshot)
        .await
        .expect_err("a merge would go unsaid");
    assert!(matches!(branch, Error::Unsupported(_)), "{branch}");
    let tag = main.create_tag("v1").await?;
    assert!(tag.stream(&t, Start::Snapshot).await.is_err(), "a tag takes no commits");
    assert!(
        main.at(seq).stream(&t, Start::Snapshot).await.is_err(),
        "nor does the past"
    );
    Ok(())
}

#[tokio::test]
async fn definition_change_stops_it() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let (catalog, main, storage) = lake_in(&schema, vec![]).await?;
    append(&catalog, &main, &storage, 0..2).await?;
    let t = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&t, Start::Latest).await?;
    let cursor = stream.next().await?.expect("its cursor").cursor;

    let pool = Pool::new(&url(), 1)?;
    let id = Protocol::new(&schema)
        .next_id(&pool.get().await?, t.id(), "field")
        .await?;
    let added = json!({"fields": [{"field_id": id, "new": true, "position": 2, "name": "v", "type": "string"}]});
    main.transaction().define(t.id(), added).commit().await?;
    let wider = main.table(&["db"], "t").await?;
    let values: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![2])),
        Arc::new(StringArray::from(vec![Some("two")])),
    ];
    let row_of_it = RecordBatch::try_new(wider.schema().clone(), values)?;
    let mut writer = TableWriter::new(&catalog, &wider, storage.clone(), WriteOptions::default())?;
    writer.write(&row_of_it).await?;
    let later = main.transaction().append(writer.finish().await?).commit().await?.seq();
    settle().await;
    let changed = stream.next().await.expect_err("its schema is not the table's");
    assert!(matches!(changed, Error::Unsupported(_)), "{changed}");

    // A stream of the table as it is now goes on from where that one was.
    let now = main.table(&["db"], "t").await?;
    let mut stream = main.stream(&now, Start::Cursor(cursor)).await?;
    assert_eq!(stream.schema().fields().len(), 5);
    let rows = drain(&mut stream, &storage).await?;
    assert_eq!(rows, [row("insert", 2, later)]);
    Ok(())
}
