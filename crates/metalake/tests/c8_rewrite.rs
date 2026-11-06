//! C8b (ADR 0003 §7): rewriting files. A flush moves inline files to storage when they are due, and only then; a
//! compaction turns small files and files with vectors into fewer without them. The rows read do not change, in any
//! partition, nor does what the past or a branch reads; deleted rows stay deleted; and a rewrite from a state a delete
//! has passed is refused, since it would bring those rows back.

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
    Catalog, CatalogConfig, Error, NewTable, Ref, Scan, ScanTask, Storage, Table, TableWriter, WriteOptions,
};
use metalake_pg::ErrorKind;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn lake(
    properties: Vec<(&str, &str)>,
    partitioning: Vec<(&str, &str)>,
) -> Result<(Catalog, Ref, Storage), Error> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let shape = batch(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning,
        properties,
    })?;
    tx.commit().await?;
    Ok((catalog, main, Storage::in_memory()))
}

async fn append(catalog: &Catalog, on: &Ref, storage: &Storage, keys: std::ops::Range<i64>) -> Result<i64, Error> {
    let t = on.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    Ok(on.transaction().append(writer.finish().await?).commit().await?.seq())
}

async fn tasks(catalog: &Catalog, t: &Table) -> Result<Vec<ScanTask>, Error> {
    Scan::new(catalog, t).plan().await?.try_collect().await
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

async fn delete_first(on: &Ref, t: &Table, storage: &Storage, file_id: i64) -> Result<(), Error> {
    let mut deletes = Deletes::new();
    deletes.insert(file_id, 0);
    on.transaction().delete(t, storage, deletes).commit().await?;
    Ok(())
}

#[tokio::test]
async fn flush_inline_files_when_due() -> Result<(), Box<dyn std::error::Error>> {
    // Not due: under the limits, and young.
    let (catalog, main, storage) = lake(vec![], vec![]).await?;
    append(&catalog, &main, &storage, 0..3).await?;
    let t = main.table(&["db"], "t").await?;
    assert_eq!(catalog.maintenance().flush(&t, &storage).await?, None);
    assert_eq!(tasks(&catalog, &t).await?[0].path, None);

    // Due: past the limit in bytes. Two inline files, one of them under a stored one, and a row deleted.
    let (catalog, main, storage) = lake(vec![("inline.flush-bytes", "1")], vec![]).await?;
    let first = append(&catalog, &main, &storage, 0..3).await?;
    append(&catalog, &main, &storage, 3..6).await?;
    let t = main.table(&["db"], "t").await?;
    let before = tasks(&catalog, &t).await?;
    assert!(before.iter().all(|f| f.path.is_none()));
    delete_first(&main, &t, &storage, before[0].file_id).await?;
    let t = main.table(&["db"], "t").await?;
    let live = keys(&catalog, &t, &storage).await?;
    assert_eq!(live.len(), 5);

    assert!(catalog.maintenance().flush(&t, &storage).await?.is_some());
    let t = main.table(&["db"], "t").await?;
    let after = tasks(&catalog, &t).await?;
    assert_eq!(after.len(), 1);
    assert!(
        after[0].path.is_some() && after[0].vector.is_none(),
        "{:?}",
        after[0].path
    );
    assert_eq!(after[0].record_count, 5);
    assert_eq!(keys(&catalog, &t, &storage).await?, live);
    assert_eq!(catalog.maintenance().flush(&t, &storage).await?, None, "none left");

    // The past still reads the inline files.
    let then = main.at(first).table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &then, &storage).await?, [0, 1, 2]);

    // Every row of an inline file deleted: the flush only removes it.
    append(&catalog, &main, &storage, 10..11).await?;
    let t = main.table(&["db"], "t").await?;
    let inline = tasks(&catalog, &t).await?;
    let inline = inline.iter().find(|f| f.path.is_none()).unwrap().file_id;
    delete_first(&main, &t, &storage, inline).await?;
    let t = main.table(&["db"], "t").await?;
    assert!(catalog.maintenance().flush(&t, &storage).await?.is_some());
    let t = main.table(&["db"], "t").await?;
    assert_eq!(tasks(&catalog, &t).await?.len(), 1);
    assert_eq!(keys(&catalog, &t, &storage).await?, live);
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_rows_drops_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![("inline.max-bytes", "0")], vec![]).await?;
    for at in 0..3 {
        append(&catalog, &main, &storage, at * 10..at * 10 + 5).await?;
    }
    let branch = main.create_branch("b").await?;
    let t = main.table(&["db"], "t").await?;
    let files = tasks(&catalog, &t).await?;
    assert_eq!(files.len(), 3);
    delete_first(&main, &t, &storage, files[1].file_id).await?;
    let t = main.table(&["db"], "t").await?;
    assert!(tasks(&catalog, &t).await?.iter().any(|f| f.vector.is_some()));
    let live = keys(&catalog, &t, &storage).await?;
    assert_eq!(live.len(), 14);

    assert!(catalog.maintenance().compact(&t, &storage).await?.is_some());
    let t = main.table(&["db"], "t").await?;
    let after = tasks(&catalog, &t).await?;
    assert_eq!(after.len(), 1);
    assert!(after[0].vector.is_none() && after[0].record_count == 14);
    assert_eq!(keys(&catalog, &t, &storage).await?, live);
    assert_eq!(
        catalog.maintenance().compact(&t, &storage).await?,
        None,
        "one file, no vector"
    );

    // The branch keeps the files it inherited, and compacts on its own.
    let on_branch = branch.table(&["db"], "t").await?;
    assert_eq!(tasks(&catalog, &on_branch).await?.len(), 3);
    let all = keys(&catalog, &on_branch, &storage).await?;
    assert_eq!(all.len(), 15);
    assert!(catalog.maintenance().compact(&on_branch, &storage).await?.is_some());
    let on_branch = branch.table(&["db"], "t").await?;
    assert_eq!(tasks(&catalog, &on_branch).await?.len(), 1);
    assert_eq!(keys(&catalog, &on_branch, &storage).await?, all);
    assert_eq!(keys(&catalog, &t, &storage).await?, live);

    // A file at its target and alone, with no vector, is left; one with a vector is rewritten even so.
    let (catalog, main, storage) = lake(
        vec![("inline.max-bytes", "0"), ("write.target-file-size-bytes", "1")],
        vec![],
    )
    .await?;
    append(&catalog, &main, &storage, 0..5).await?;
    let t = main.table(&["db"], "t").await?;
    assert_eq!(catalog.maintenance().compact(&t, &storage).await?, None);
    let file = tasks(&catalog, &t).await?[0].file_id;
    delete_first(&main, &t, &storage, file).await?;
    let t = main.table(&["db"], "t").await?;
    assert!(catalog.maintenance().compact(&t, &storage).await?.is_some());
    let t = main.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &t, &storage).await?, [1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn compaction_per_partition() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![("inline.max-bytes", "0")], vec![("k", "bucket[2]")]).await?;
    append(&catalog, &main, &storage, 0..20).await?;
    append(&catalog, &main, &storage, 20..40).await?;
    let t = main.table(&["db"], "t").await?;
    let before = tasks(&catalog, &t).await?;
    assert_eq!(before.len(), 4);
    assert!(catalog.maintenance().compact(&t, &storage).await?.is_some());
    let t = main.table(&["db"], "t").await?;
    let after = tasks(&catalog, &t).await?;
    assert_eq!(after.len(), 2, "one for each bucket");
    assert_ne!(after[0].partition, after[1].partition);
    assert_eq!(keys(&catalog, &t, &storage).await?, (0..40).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn stale_rewrite_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage) = lake(vec![("inline.max-bytes", "0")], vec![]).await?;
    append(&catalog, &main, &storage, 0..5).await?;
    append(&catalog, &main, &storage, 5..10).await?;
    let read = main.table(&["db"], "t").await?;
    let files = tasks(&catalog, &read).await?;

    // The delete lands after the compaction read: its rows would come back.
    delete_first(&main, &read, &storage, files[0].file_id).await?;
    let refused = catalog.maintenance().compact(&read, &storage).await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.kind() == ErrorKind::Invalid && e.rule() == Some("CMT-11")),
        "{refused:?}"
    );
    let t = main.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &t, &storage).await?.len(), 9);

    // From the latest state it goes; and a delete from a read before it finds its file gone.
    assert!(catalog.maintenance().compact(&t, &storage).await?.is_some());
    let mut deletes = Deletes::new();
    deletes.insert(files[1].file_id, 0);
    let refused = main.transaction().delete(&t, &storage, deletes).commit().await;
    assert!(refused.is_err(), "{refused:?}");
    Ok(())
}
