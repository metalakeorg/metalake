//! C6b (ADR 0003 §7): deleting rows. An engine reads a scan `with_positions`, chooses rows, and a transaction
//! deletes them: inline and uploaded files, and a range whose rows sit in a later row group, lose exactly those
//! rows; a vector past the table's inline limit goes to a Puffin file whose footer names it, its intent registered
//! and taken; two deletes from one read of the same file both survive, the second retried over the first's vector
//! (DV-4); deleting deleted rows changes nothing; a branch's delete stays on the branch; and positions a file has
//! not, or a file the table has not, are refused before the protocol.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::partition::Scalar;
use metalake::vectors::{self, Deletes};
use metalake::{
    Catalog, CatalogConfig, Error, NewTable, Ref, Scan, ScanTask, Storage, Table, TableWriter, WriteOptions,
};
use metalake_pg::{CreateRef, Pool, Protocol};

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

async fn table(catalog: &Catalog, properties: Vec<(&str, &str)>) -> Result<(), Error> {
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
    Ok(())
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

async fn keys(catalog: &Catalog, t: &Table, storage: &Storage) -> Result<Vec<i64>, Error> {
    let scan = Scan::new(catalog, t);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

/// What an engine does: reads the scan with positions and deletes the rows `chosen` keeps.
async fn delete_where(storage: &Storage, scan: Scan, chosen: impl Fn(i64) -> bool) -> Result<Deletes, Error> {
    let scan = scan.with_positions();
    assert_eq!(
        scan.schema()
            .fields()
            .iter()
            .rev()
            .take(2)
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        [vectors::POSITION, vectors::FILE_ID]
    );
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut deletes = Deletes::new();
    for b in &batches {
        let keep: Vec<u32> = (0..b.num_rows() as u32)
            .filter(|i| chosen(b.column(0).as_primitive::<Int64Type>().value(*i as usize)))
            .collect();
        let rows = arrow_select::take::take_record_batch(b, &arrow_array::UInt32Array::from(keep)).unwrap();
        deletes.add(&rows)?;
    }
    Ok(deletes)
}

#[tokio::test]
async fn delete_selected_rows() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    table(&catalog, vec![]).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    write(&catalog, &main, &storage, batch(0..10), WriteOptions::default()).await?;
    let groups = WriteOptions {
        row_group_rows: 20_000,
        ..Default::default()
    };
    write(&catalog, &main, &storage, batch(10..100_010), groups).await?;

    // Every third key below 20, and every key in [65,000, 65,100] divisible by 7: an inline file and a later row
    // group of the uploaded one, which a range reads alone.
    let t = main.table(&["db"], "t").await?;
    let first = |k: i64| k < 20 && k % 3 == 0;
    let mut deletes = delete_where(&storage, Scan::new(&catalog, &t), first).await?;
    let ranged = Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(65_000)), Some(Scalar::Long(65_100)))?;
    let second = |k: i64| (65_000..=65_100).contains(&k) && k % 7 == 0;
    let more = delete_where(&storage, ranged, second).await?;
    for (file, v) in more.files() {
        for p in v.positions() {
            deletes.insert(*file, p);
        }
    }
    assert_eq!(deletes.files().len(), 2);
    main.transaction().delete(&t, &storage, deletes).commit().await?;
    let t = main.table(&["db"], "t").await?;
    let live: Vec<i64> = (0..100_010).filter(|k| !first(*k) && !second(*k)).collect();
    assert_eq!(keys(&catalog, &t, &storage).await?, live);

    // Two deletes from that one read: the second finds the vector changed under it, and takes its union again.
    let a = delete_where(&storage, Scan::new(&catalog, &t), |k| k == 30_000).await?;
    let b = delete_where(&storage, Scan::new(&catalog, &t), |k| k == 30_001).await?;
    main.transaction().delete(&t, &storage, a).commit().await?;
    main.transaction().delete(&t, &storage, b).commit().await?;
    let now = main.table(&["db"], "t").await?;
    let live: Vec<i64> = live.into_iter().filter(|k| *k != 30_000 && *k != 30_001).collect();
    assert_eq!(keys(&catalog, &now, &storage).await?, live);

    // Deleting deleted rows: the vectors stay as they are.
    let planned: Vec<ScanTask> = Scan::new(&catalog, &now).plan().await?.try_collect().await?;
    let mut again = Deletes::new();
    for task in planned.iter().filter(|t| t.vector.is_some()) {
        let v = task.vector.as_ref().unwrap();
        let blob = v.inline.as_ref().expect("small vectors are inline");
        for p in vectors::DeletionVector::from_blob(blob)?.positions() {
            again.insert(task.file_id, p);
        }
    }
    let before = now.seq();
    main.transaction().delete(&now, &storage, again).commit().await?;
    let after: Vec<ScanTask> = Scan::new(&catalog, &main.table(&["db"], "t").await?)
        .plan()
        .await?
        .try_collect()
        .await?;
    assert_eq!(after, planned, "no vector changed since {before}");

    // A branch's delete stays on the branch.
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    p.create_ref(&session, &CreateRef::branch("dev", main.id())).await?;
    let dev = catalog.branch("dev").await?;
    let on_dev = dev.table(&["db"], "t").await?;
    let gone = delete_where(&storage, Scan::new(&catalog, &on_dev), |k| k == 50_000).await?;
    dev.transaction().delete(&on_dev, &storage, gone).commit().await?;
    assert!(
        !keys(&catalog, &dev.table(&["db"], "t").await?, &storage)
            .await?
            .contains(&50_000)
    );
    assert!(
        keys(&catalog, &main.table(&["db"], "t").await?, &storage)
            .await?
            .contains(&50_000)
    );

    // Positions a file has not, and a file the table has not, never reach the protocol.
    let t = main.table(&["db"], "t").await?;
    let inline = planned.iter().find(|x| x.content.is_some()).unwrap();
    let mut past = Deletes::new();
    past.insert(inline.file_id, 10);
    let refused = main.transaction().delete(&t, &storage, past).commit().await;
    assert!(matches!(refused, Err(Error::Invalid(_))), "{refused:?}");
    let mut nowhere = Deletes::new();
    nowhere.insert(999_999, 0);
    let refused = main.transaction().delete(&t, &storage, nowhere).commit().await;
    assert!(matches!(refused, Err(Error::NotFound(_))), "{refused:?}");
    assert_eq!(planned.iter().filter(|t| t.vector.is_some()).count(), 2);
    Ok(())
}

#[tokio::test]
async fn large_vector_stored_in_puffin() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    // Vectors and files past 1 KiB leave the catalog.
    table(&catalog, vec![("inline.max-bytes", "1024")]).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    write(&catalog, &main, &storage, batch(0..100_000), WriteOptions::default()).await?;
    let t = main.table(&["db"], "t").await?;
    let odd = |k: i64| k % 2 == 1;
    let deletes = delete_where(&storage, Scan::new(&catalog, &t), odd).await?;
    main.transaction().delete(&t, &storage, deletes).commit().await?;

    let t = main.table(&["db"], "t").await?;
    let planned: Vec<ScanTask> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let v = planned[0].vector.clone().expect("a vector");
    let path = v.path.clone().expect("in a Puffin file");
    assert!(v.inline.is_none() && v.cardinality == 50_000);
    assert_eq!(
        keys(&catalog, &t, &storage).await?,
        (0..100_000).step_by(2).collect::<Vec<i64>>()
    );

    // The Puffin file: its magic at both ends, a footer whose blob is the vector at its offset and length, for the
    // data file it deletes from.
    let puffin = storage.get(&path).await?;
    let n = puffin.len();
    assert_eq!((&puffin[..4], &puffin[n - 4..]), (&b"PFA1"[..], &b"PFA1"[..]));
    let size = u32::from_le_bytes(puffin[n - 12..n - 8].try_into()?) as usize;
    let footer: serde_json::Value = serde_json::from_slice(&puffin[n - 12 - size..n - 12])?;
    assert_eq!(&puffin[n - 16 - size..n - 12 - size], b"PFA1");
    let blob = &footer["blobs"][0];
    assert_eq!(blob["type"], "deletion-vector-v1");
    assert_eq!(
        (blob["offset"].as_i64(), blob["length"].as_i64()),
        (v.offset, Some(v.size))
    );
    assert_eq!(blob["properties"]["cardinality"], "50000");
    assert_eq!(
        blob["properties"]["referenced-data-file"].as_str(),
        planned[0].path.as_deref()
    );

    // The commit took the Puffin file's intent: it is no orphan.
    let pool = Pool::new(&url(), 1)?;
    let later = SystemTime::now() + Duration::from_secs(7200);
    assert!(
        Protocol::new(&schema)
            .orphans(&pool.get().await?, Some(later))
            .await?
            .is_empty()
    );

    // A delete whose file another commit removed: a conflict the union does not fix, so not retried; the Puffin
    // file it uploaded is an orphan, named by its intent.
    let read = main.table(&["db"], "t").await?;
    let more = delete_where(&storage, Scan::new(&catalog, &read), |k| k % 4 == 0).await?;
    main.transaction().remove(&read, [planned[0].file_id]).commit().await?;
    let refused = main.transaction().delete(&read, &storage, more).commit().await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.rule() == Some("CMT-7")),
        "{refused:?}"
    );
    let orphans = Protocol::new(&schema).orphans(&pool.get().await?, Some(later)).await?;
    assert!(
        orphans.len() == 1 && orphans[0].ends_with("-deletes.puffin"),
        "{orphans:?}"
    );
    Ok(())
}

#[tokio::test]
async fn dv4_delete_added_file_rows_applies() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    table(&catalog, vec![]).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    write(&catalog, &main, &storage, batch(0..10), WriteOptions::default()).await?;

    // An inline file and a stored one, written and located, lose rows in the commit that adds them, along with a row
    // of a file committed before.
    let t = main.table(&["db"], "t").await?;
    let mut deletes = delete_where(&storage, Scan::new(&catalog, &t), |k| k == 4).await?;
    let mut files = Vec::new();
    for (keys, stored) in [(10..20, false), (20..30, true)] {
        let options = WriteOptions {
            stored,
            ..Default::default()
        };
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), options)?;
        let located = writer.write_located(&batch(keys.clone())).await?;
        for (k, at) in keys.zip(located) {
            if k % 5 == 0 {
                deletes.insert(at.file_id, at.position);
            }
        }
        files.extend(writer.finish().await?);
    }
    assert_eq!(
        files.iter().map(|f| f.path.is_some()).collect::<Vec<_>>(),
        [false, true]
    );
    main.transaction()
        .append(files)
        .delete(&t, &storage, deletes)
        .commit()
        .await?;
    let live: Vec<i64> = (0..30).filter(|k| *k != 4 && !(*k >= 10 && k % 5 == 0)).collect();
    assert_eq!(keys(&catalog, &main.table(&["db"], "t").await?, &storage).await?, live);

    // A position past an added file's rows never reaches the protocol.
    let t = main.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
    let at = writer.write_located(&batch(30..31)).await?[0];
    let mut past = Deletes::new();
    past.insert(at.file_id, 1);
    let refused = main
        .transaction()
        .append(writer.finish().await?)
        .delete(&t, &storage, past)
        .commit()
        .await;
    assert!(matches!(refused, Err(Error::Invalid(_))), "{refused:?}");
    Ok(())
}
