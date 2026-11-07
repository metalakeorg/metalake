//! Following a table with no engine (CHG-3, CHG-4). A reader takes what a table holds, then each commit as rows inserted
//! and deleted, so that applying them rebuilds the table: with commits landing while it starts, across a restart from its
//! cursor, and past compactions, which change no row. One that fell behind what a ref keeps fails, and never misses a
//! commit in silence.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{
    CHANGE_TYPE, COMMIT_SEQ, Catalog, CatalogConfig, Error, NewTable, Ref, Retention, Scan, Start, Storage,
    StreamCursor, StreamStep, Table, TableStream, TableWriter, WriteOptions,
};
use metalake_pg::{Pool, Protocol};
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
    assert_eq!(stream.schema().fields().len(), 4);
    let rows = drain(&mut stream, &storage).await?;
    assert_eq!(rows, [row("insert", 2, later)]);
    Ok(())
}
