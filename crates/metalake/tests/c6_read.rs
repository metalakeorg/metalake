//! C6a (ADR 0003 §7): reading with deletion vectors. A deleted row is not read, whether its vector is inline in
//! the catalog or at an offset of a Puffin file (one GET more, for its range); a file whose every row is deleted
//! costs no request for its data; a range keeps its row groups' live rows only; a branch's vector is seen on the
//! branch, never on its parent; and a vector that is not what the catalog says is refused.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::{Counting, fresh_schema, url};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use metalake::partition::Scalar;
use metalake::vectors::DeletionVector;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, ScanTask, Storage, TableWriter, WriteOptions};
use metalake_pg::records::VectorSet;
use metalake_pg::{Commit, CreateRef, Pool, Protocol};

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.map(|k| format!("s{k}")))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn write(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    b: RecordBatch,
    options: WriteOptions,
) -> Result<(), Error> {
    let t = on.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), options)?;
    writer.write(&b).await?;
    let files = writer.finish().await?;
    on.transaction().append(files).commit().await?;
    Ok(())
}

async fn keys(scan: &Scan, storage: &Storage) -> Result<Vec<i64>, Error> {
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

/// A vector set inline, or at `path` at an offset.
fn vector(task: &ScanTask, positions: &[u64], path: Option<(&str, i64)>) -> VectorSet {
    let v = DeletionVector::from_positions(positions.iter().copied());
    let blob = v.to_blob();
    VectorSet {
        table_id: uuid::Uuid::nil(),
        file_id: task.file_id,
        base_seq: task.vector.as_ref().map(|v| v.begin_seq),
        path: path.map(|(p, _)| p.to_string()),
        content_offset: path.map(|(_, o)| o),
        content_size: blob.len() as i64,
        cardinality: v.cardinality() as i64,
        inline: path.is_none().then_some(blob),
    }
}

#[tokio::test]
async fn scan_applies_deletion_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let (tx, table_id) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0..1).schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let store = Arc::new(Counting::default());
    let storage = Storage::from_store("memory:///", store.clone())?;
    // A: 10 rows inline; B: 100,000 uploaded, in 5 row groups of 20,000; C: 50,000 uploaded, all to be deleted.
    write(&catalog, &main, &storage, batch(0..10), WriteOptions::default()).await?;
    let groups = WriteOptions {
        row_group_rows: 20_000,
        ..Default::default()
    };
    write(&catalog, &main, &storage, batch(10..100_010), groups).await?;
    write(
        &catalog,
        &main,
        &storage,
        batch(500_000..550_000),
        WriteOptions::default(),
    )
    .await?;
    let t = main.table(&["db"], "t").await?;
    let planned: Vec<ScanTask> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let file = |first: i64| planned.iter().find(|p| p.record_count == first).cloned().unwrap();
    let (a, b, c) = (file(10), file(100_000), file(50_000));
    assert!(a.content.is_some() && b.path.is_some() && c.path.is_some());

    // B's vector lives at an offset of a Puffin file: magic, then other bytes, then its blob, then more.
    let b_deleted = [0u64, 19_999, 20_000, 50_000, 50_001, 50_002, 99_999];
    let blob = DeletionVector::from_positions(b_deleted).to_blob();
    let mut puffin = b"PFA1".to_vec();
    puffin.extend_from_slice(&[7; 16]);
    puffin.extend_from_slice(&blob);
    puffin.extend_from_slice(&[9; 32]);
    storage
        .put("memory:///wh/db/t/vectors/b.puffin", Bytes::from(puffin))
        .await?;
    let mut sets = vec![
        vector(&a, &[0, 9], None),
        vector(&b, &b_deleted, Some(("memory:///wh/db/t/vectors/b.puffin", 20))),
        vector(&c, &(0..50_000).collect::<Vec<_>>(), None),
    ];
    for s in &mut sets {
        s.table_id = table_id;
    }
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    let commit = |ref_id: i32, vectors: Vec<VectorSet>| Commit {
        ref_id,
        operation: "delete".into(),
        vectors,
        ..Default::default()
    };
    p.commit(&session, &commit(main.id(), sets)).await?;

    // Every live row; B's data in one GET per row group and its vector in one; A's and C's in none: Parquet
    // fetches no row group a selection leaves empty.
    let t = main.table(&["db"], "t").await?;
    let deleted: BTreeSet<i64> = [0, 9]
        .into_iter()
        .chain(b_deleted.iter().map(|p| *p as i64 + 10))
        .collect();
    let live: Vec<i64> = (0..100_010).filter(|k| !deleted.contains(k)).collect();
    store.take();
    assert_eq!(keys(&Scan::new(&catalog, &t), &storage).await?, live);
    assert_eq!(store.take(), 5 + 1);

    // A range over the end of B's first row group and the start of its second: those groups, less what they lost.
    let scan = Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(20_005)), Some(Scalar::Long(20_015)))?;
    let want: Vec<i64> = (10..40_010).filter(|k| !deleted.contains(k)).collect();
    assert_eq!(keys(&scan, &storage).await?, want);
    let scan = Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(90_000)), None)?;
    let want: Vec<i64> = (80_010..100_010).filter(|k| !deleted.contains(k)).collect();
    assert_eq!(keys(&scan, &storage).await?, want);

    // A branch deletes one more row of A, its vector holding the parent's too (DV-4): only the branch loses it.
    p.create_ref(&session, &CreateRef::branch("dev", main.id())).await?;
    let dev = catalog.branch("dev").await?;
    let on_dev: Vec<ScanTask> = Scan::new(&catalog, &dev.table(&["db"], "t").await?)
        .plan()
        .await?
        .try_collect()
        .await?;
    let a_now = on_dev.iter().find(|x| x.file_id == a.file_id).unwrap();
    let mut more = vector(a_now, &[0, 5, 9], None);
    more.table_id = table_id;
    p.commit(&session, &commit(dev.id(), vec![more])).await?;
    let dev_keys = keys(&Scan::new(&catalog, &dev.table(&["db"], "t").await?), &storage).await?;
    assert!(!dev_keys.contains(&5) && dev_keys.len() == live.len() - 1);
    let main_keys = keys(&Scan::new(&catalog, &main.table(&["db"], "t").await?), &storage).await?;
    assert_eq!(main_keys, live);

    // A vector that is not what the catalog says: another count, or bytes that are not a vector.
    let scan = Scan::new(&catalog, &t);
    let tasks: Vec<ScanTask> = scan.plan().await?.try_collect().await?;
    let mut wrong_count = tasks.iter().find(|x| x.file_id == a.file_id).cloned().unwrap();
    wrong_count.vector.as_mut().unwrap().cardinality = 3;
    let mut not_a_vector = wrong_count.clone();
    let v = not_a_vector.vector.as_mut().unwrap();
    v.cardinality = 2;
    v.inline.as_mut().unwrap()[6] ^= 1;
    for task in [wrong_count, not_a_vector] {
        let refused = scan
            .read(stream::iter([Ok(task)]).boxed(), &storage)
            .try_collect::<Vec<_>>()
            .await;
        assert!(matches!(refused, Err(Error::Corrupt(_))), "{refused:?}");
    }
    Ok(())
}

/// What a vector costs a read: 10^6 rows in one file, read whole, then with 10% of its rows deleted at random.
#[tokio::test]
#[ignore = "a measurement: run with --release --ignored"]
async fn delete_cost_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let (tx, table_id) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0..1).schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let storage = Storage::in_memory();
    let groups = WriteOptions {
        row_group_rows: 125_000,
        ..Default::default()
    };
    write(&catalog, &main, &storage, batch(0..1_000_000), groups).await?;
    let t = main.table(&["db"], "t").await?;
    let task: ScanTask = Scan::new(&catalog, &t).plan().await?.try_next().await?.unwrap();
    let time = |task: ScanTask, scan: Scan| {
        let storage = storage.clone();
        async move {
            let mut best = std::time::Duration::MAX;
            let mut rows = 0;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                rows = scan
                    .open(task.clone(), &storage)
                    .try_fold(0, |n, b| async move { Ok(n + b.num_rows()) })
                    .await?;
                best = best.min(started.elapsed());
            }
            Ok::<_, Error>((rows, best))
        }
    };
    let (rows, whole) = time(task.clone(), Scan::new(&catalog, &t)).await?;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let positions: Vec<u64> = (0..1_000_000u64)
        .filter(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.is_multiple_of(10)
        })
        .collect();
    let mut set = vector(&task, &positions, None);
    set.table_id = table_id;
    let pool = Pool::new(&url(), 1)?;
    let p = Protocol::new(&schema);
    p.commit(
        &pool.get().await?,
        &Commit {
            ref_id: main.id(),
            operation: "delete".into(),
            vectors: vec![set],
            ..Default::default()
        },
    )
    .await?;
    let t = main.table(&["db"], "t").await?;
    let deleted: ScanTask = Scan::new(&catalog, &t).plan().await?.try_next().await?.unwrap();
    let blob = deleted.vector.as_ref().unwrap().inline.as_ref().unwrap().len();
    let (live, with) = time(deleted, Scan::new(&catalog, &t)).await?;
    assert_eq!(live, rows - positions.len());
    eprintln!(
        "10^6 rows: {whole:?} whole; {with:?} with {} deleted, a vector of {blob} bytes",
        positions.len()
    );
    Ok(())
}

#[tokio::test]
async fn sta1_unknown_footer_size_one_tail_request() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0..1).schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let store = Arc::new(Counting::default());
    let storage = Storage::from_store("memory:///", store.clone())?;

    // Two stored files with no footer in the catalog: one of a known footer_size, one of a size its writer did not
    // know, as a file another engine wrote (STA-1).
    let t = main.table(&["db"], "t").await?;
    let stored = WriteOptions {
        stored: true,
        ..Default::default()
    };
    let mut files = Vec::new();
    for (keys, known) in [(0..100, true), (100..200, false)] {
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), stored.clone())?;
        writer.write(&batch(keys)).await?;
        let mut file = writer.finish().await?.remove(0);
        file.footer = None;
        if !known {
            file.footer_size = 0;
        }
        files.push(file);
    }
    main.transaction().append(files).commit().await?;

    let t = main.table(&["db"], "t").await?;
    let scan = Scan::new(&catalog, &t);
    let mut tasks: Vec<ScanTask> = scan.plan().await?.try_collect().await?;
    tasks.sort_by_key(|task| task.footer_size == 0);
    let mut cost = Vec::new();
    for task in tasks {
        store.take();
        let read: Vec<RecordBatch> = scan.open(task, &storage).try_collect().await?;
        let keys: Vec<i64> = read
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        cost.push((keys.len(), store.take()));
    }
    assert_eq!(cost[0].0, 100);
    assert_eq!(
        cost[1], cost[0],
        "the same rows, and the footer in one request: {cost:?}"
    );
    Ok(())
}
