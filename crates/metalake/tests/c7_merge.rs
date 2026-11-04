//! C7b (ADR 0003 §7): merges. A branch's writes and deletes reach its parent in one commit, as the report said, and
//! the sequence before it still reads what was; a branch with nothing to merge answers none; a conflict is refused
//! whole; one table merges alone and the rest stays on the branch; a commit across a selection needs `split`; and
//! when both sides delete from a file with vectors in Puffin, the core hands in their union and neither is lost.

mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{
    Catalog, CatalogConfig, Error, MergeReport, NewTable, Ref, Scan, ScanTask, Storage, Table, TableWriter,
    WriteOptions,
};
use metalake_pg::records::FileAdd;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn catalog(tables: &[&str], properties: Vec<(&str, &str)>) -> Result<Catalog, Error> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let shape = batch(0..1).schema();
    let mut tx = main.transaction().create_namespace(&["db"]);
    for name in tables {
        tx = tx
            .create_table(NewTable {
                namespace: &["db"],
                name,
                schema: &shape,
                location: None,
                cluster_key: vec!["k"],
                partitioning: vec![],
                properties: properties.clone(),
            })?
            .0;
    }
    tx.commit().await?;
    Ok(catalog)
}

async fn files(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    keys: std::ops::Range<i64>,
) -> Result<Vec<FileAdd>, Error> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    writer.finish().await
}

async fn append(
    catalog: &Catalog,
    on: &Ref,
    name: &str,
    storage: &Storage,
    keys: std::ops::Range<i64>,
) -> Result<i64, Error> {
    let t = on.table(&["db"], name).await?;
    let files = files(catalog, &t, storage, keys).await?;
    Ok(on.transaction().append(files).commit().await?.seq())
}

async fn keys(catalog: &Catalog, on: &Ref, name: &str, storage: &Storage) -> Result<Vec<i64>, Error> {
    let t = on.table(&["db"], name).await?;
    let scan = Scan::new(catalog, &t);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

/// Deletes the rows `chosen` keeps of table `name` as `on` sees it.
async fn delete(
    catalog: &Catalog,
    on: &Ref,
    name: &str,
    storage: &Storage,
    chosen: impl Fn(i64) -> bool,
) -> Result<(), Error> {
    let t = on.table(&["db"], name).await?;
    let scan = Scan::new(catalog, &t).with_positions();
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut deletes = Deletes::new();
    for b in &batches {
        let ks = b.column(0).as_primitive::<Int64Type>();
        let (fs, ps) = (
            b.column(1).as_primitive::<Int64Type>(),
            b.column(2).as_primitive::<Int64Type>(),
        );
        for row in (0..b.num_rows()).filter(|r| chosen(ks.value(*r))) {
            deletes.insert(fs.value(row), ps.value(row) as u64);
        }
    }
    on.transaction().delete(&t, storage, deletes).commit().await?;
    Ok(())
}

#[tokio::test]
async fn a_branch_reaches_its_parent_in_one_commit_or_not_at_all() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = catalog(&["a", "b"], vec![]).await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    let before = append(&catalog, &main, "a", &storage, 0..10).await?;
    let dev = main.create_branch("dev").await?;
    assert_eq!(dev.merge().report().await?, MergeReport::Nothing);
    assert_eq!(dev.merge().commit().await?, None);

    // A write and a delete on a, a write on b: the report names both tables, and changes nothing.
    append(&catalog, &dev, "a", &storage, 10..15).await?;
    delete(&catalog, &dev, "a", &storage, |k| k == 3).await?;
    append(&catalog, &dev, "b", &storage, 100..105).await?;
    let (a, b) = (dev.table(&["db"], "a").await?, dev.table(&["db"], "b").await?);
    let mut both = vec![a.id(), b.id()];
    both.sort();
    assert_eq!(
        dev.merge().report().await?,
        MergeReport::Merges {
            merged: both,
            rebased: vec![]
        }
    );
    assert_eq!(keys(&catalog, &main, "b", &storage).await?, Vec::<i64>::new());

    // One commit brings all of it; the sequence before it reads what was.
    let merged = dev.merge().message("all of it").commit().await?.expect("a merge");
    let live: Vec<i64> = (0..15).filter(|k| *k != 3).collect();
    assert_eq!(keys(&catalog, &main, "a", &storage).await?, live);
    assert_eq!(
        keys(&catalog, &main, "b", &storage).await?,
        (100..105).collect::<Vec<_>>()
    );
    assert_eq!(
        keys(&catalog, &main.at(merged - 1), "a", &storage).await?,
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(
        keys(&catalog, &main.at(before), "b", &storage).await?,
        Vec::<i64>::new()
    );
    assert_eq!(
        dev.merge().commit().await?,
        None,
        "the branch is empty after it (MRG-4)"
    );
    assert!(matches!(dev.at(before).merge().commit().await, Err(Error::Invalid(_))));

    // The branch deletes from a file main removes: the merge is refused whole, its write to b too.
    let conflict = main.create_branch("conflict").await?;
    delete(&catalog, &conflict, "a", &storage, |k| k == 0).await?;
    append(&catalog, &conflict, "b", &storage, 200..201).await?;
    let ours = main.table(&["db"], "a").await?;
    let planned: Vec<ScanTask> = Scan::new(&catalog, &ours).plan().await?.try_collect().await?;
    main.transaction()
        .remove(&ours, planned.iter().map(|t| t.file_id))
        .commit()
        .await?;
    let report = conflict.merge().report().await?;
    assert!(
        matches!(&report, MergeReport::Refused { rule: Some(_), .. }),
        "{report:?}"
    );
    assert!(matches!(conflict.merge().commit().await, Err(Error::Protocol(_))));
    assert_eq!(
        keys(&catalog, &main, "b", &storage).await?,
        (100..105).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn one_table_merges_alone_and_a_commit_across_needs_split() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = catalog(&["a", "b"], vec![]).await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    let dev = main.create_branch("dev").await?;
    append(&catalog, &dev, "a", &storage, 0..5).await?;
    append(&catalog, &dev, "b", &storage, 10..15).await?;
    let (a, b) = (dev.table(&["db"], "a").await?, dev.table(&["db"], "b").await?);
    assert_eq!(
        dev.merge().tables(&[&a]).report().await?,
        MergeReport::Merges {
            merged: vec![a.id()],
            rebased: vec![b.id()]
        }
    );
    dev.merge().tables(&[&a]).commit().await?;
    assert_eq!(keys(&catalog, &main, "a", &storage).await?, (0..5).collect::<Vec<_>>());
    assert_eq!(keys(&catalog, &main, "b", &storage).await?, Vec::<i64>::new());
    assert_eq!(keys(&catalog, &dev, "b", &storage).await?, (10..15).collect::<Vec<_>>());

    // One commit that wrote both: a selection of one is refused, unless split.
    let cross = main.create_branch("cross").await?;
    let (a, b) = (cross.table(&["db"], "a").await?, cross.table(&["db"], "b").await?);
    let both = [
        files(&catalog, &a, &storage, 20..21).await?,
        files(&catalog, &b, &storage, 30..31).await?,
    ]
    .concat();
    cross.transaction().append(both).commit().await?;
    let refused = cross.merge().tables(&[&a]).commit().await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.rule() == Some("MRG-7")),
        "{refused:?}"
    );
    cross.merge().tables(&[&a]).split().commit().await?;
    assert!(keys(&catalog, &main, "a", &storage).await?.contains(&20));
    assert!(!keys(&catalog, &main, "b", &storage).await?.contains(&30));
    Ok(())
}

#[tokio::test]
async fn vectors_in_puffin_changed_on_both_sides_are_unioned() -> Result<(), Box<dyn std::error::Error>> {
    // Vectors and files past 1 KiB leave the catalog.
    let catalog = catalog(&["t"], vec![("inline.max-bytes", "1024")]).await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    // Two files: both sides change the first one's vector, and only main the second's, which the branch inherits.
    append(&catalog, &main, "t", &storage, 0..100_000).await?;
    append(&catalog, &main, "t", &storage, 200_000..300_000).await?;
    delete(&catalog, &main, "t", &storage, |k| k >= 200_000 && k % 4 == 0).await?;
    let dev = main.create_branch("dev").await?;
    delete(&catalog, &dev, "t", &storage, |k| k < 100_000 && k % 4 == 1).await?;
    delete(&catalog, &main, "t", &storage, |k| k % 4 == 2).await?;

    // Without the storage the core cannot read them, and the protocol's refusal stands.
    let report = dev.merge().report().await?;
    assert!(
        matches!(&report, MergeReport::Refused { rule: Some(r), .. } if r == "MRG-5"),
        "{report:?}"
    );
    let refused = dev.merge().commit().await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.rule() == Some("MRG-5")),
        "{refused:?}"
    );

    // With it, the report says what the merge does, and writes nothing; the merge keeps both sides' deletes.
    let t = dev.table(&["db"], "t").await?;
    let objects = |s: &Storage| {
        let store = s.store().clone();
        async move { store.list(None).try_collect::<Vec<_>>().await.map(|o| o.len()) }
    };
    let held = objects(&storage).await?;
    assert_eq!(
        dev.merge().storage(&storage).report().await?,
        MergeReport::Merges {
            merged: vec![t.id()],
            rebased: vec![]
        }
    );
    assert_eq!(objects(&storage).await?, held, "a report uploads nothing");
    dev.merge().storage(&storage).commit().await?.expect("a merge");
    let live = |k: &i64| {
        let (first, r) = (*k < 100_000, k % 4);
        r == 3 || (first && r == 0) || (!first && r == 1)
    };
    let want: Vec<i64> = (0..100_000).chain(200_000..300_000).filter(live).collect();
    assert_eq!(keys(&catalog, &main, "t", &storage).await?, want);

    // The Puffin file the merge wrote holds the one vector it needed, not the second file's.
    let t = main.table(&["db"], "t").await?;
    let planned: Vec<ScanTask> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let merged = planned
        .iter()
        .map(|f| f.vector.as_ref().unwrap())
        .max_by_key(|v| v.begin_seq)
        .unwrap();
    let puffin = storage.get(merged.path.as_ref().expect("in Puffin")).await?;
    let n = puffin.len();
    let size = u32::from_le_bytes(puffin[n - 12..n - 8].try_into()?) as usize;
    let footer: serde_json::Value = serde_json::from_slice(&puffin[n - 12 - size..n - 12])?;
    assert_eq!(footer["blobs"].as_array().map(Vec::len), Some(1));

    // Again from the new fork: the branch inherits the unions, and only it changes them, so none is needed.
    delete(&catalog, &dev, "t", &storage, |k| k % 4 == 3).await?;
    assert!(dev.merge().commit().await?.is_some());
    let want: Vec<i64> = want.into_iter().filter(|k| k % 4 != 3).collect();
    assert_eq!(keys(&catalog, &main, "t", &storage).await?, want);
    Ok(())
}
