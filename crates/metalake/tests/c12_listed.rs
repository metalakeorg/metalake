//! `listed_files`, against the reference's `iceberg_files` and `iceberg_dvs` (ICE-2, ICE-3, ICE-4): at every snapshot of
//! every ref, across a branch, a removal, a vector, a merge and an inline file, the same files, added by the same commits.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, NewTable, Scan, Storage, TableWriter, WriteOptions};
use metalake_pg::Pool;
use object_store::memory::InMemory;
use serde_json::Value;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// Per file, what both sides answer, with `ADDED` read from `begin_seq`, and the keys of its statistics.
const LISTED: &str = "select coalesce(jsonb_agg(jsonb_build_object(
      'file', coalesce(l.path, inline_path($2, l.file_id)),
      'status', case when l.begin_seq = $3 then 'ADDED' else 'EXISTING' end,
      'seq', l.begin_seq, 'rows', l.record_count, 'size', l.file_size,
      'stats', cardinality(l.stat_field_ids), 'vector', l.vector_cardinality) order by l.file_id), '[]')
    from listed_files($1, $2, $3) l";

const REFERENCE: &str = "select coalesce(jsonb_agg(jsonb_build_object(
      'file', f ->> 'path', 'status', f ->> 'status', 'seq', (f ->> 'data_sequence_number')::bigint,
      'rows', (f ->> 'record_count')::bigint, 'size', (f ->> 'file_size')::bigint,
      'stats', (select count(*) from stat_keys(f)),
      'vector', (select (d ->> 'cardinality')::bigint from iceberg_dvs($1, $2) d
                  where d ->> 'referenced_data_file' = f ->> 'path')) order by f ->> 'path'), '[]')
    from iceberg_files($1, $2) f";

fn by_file(mut files: Value) -> Value {
    files
        .as_array_mut()
        .unwrap()
        .sort_by_key(|f| f["file"].as_str().unwrap().to_string());
    files
}

#[tokio::test]
async fn listed_files_match_reference() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let config = CatalogConfig::new(url(), &schema).warehouse("memory:///wh");
    let catalog = Catalog::init(config, None).await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let shape = batch(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;

    let files = async |on: &metalake::Ref, keys| {
        let t = on.table(&["db"], "t").await?;
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(keys)).await?;
        let added = writer.finish().await?;
        on.transaction().append(added).commit().await
    };
    let delete = async |on: &metalake::Ref, position| {
        let t = on.table(&["db"], "t").await?;
        let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
        let mut deletes = Deletes::new();
        deletes.insert(planned[0].file_id, position);
        on.transaction().delete(&t, &storage, deletes).commit().await
    };

    files(&main, 0..5).await?;
    files(&main, 5..10).await?;
    let branch = main.create_branch("b").await?;
    files(&branch, 10..15).await?;
    delete(&branch, 1).await?;
    let t = branch.table(&["db"], "t").await?;
    let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    branch.transaction().remove(&t, [planned[1].file_id]).commit().await?;
    files(&main, 15..20).await?;
    delete(&main, 2).await?;
    branch.merge().commit().await?;

    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let reference = include_str!("../../../spec/v0/reference/16-iceberg.sql");
    session
        .batch_execute(&format!("set search_path = {schema}; {reference}"))
        .await?;
    let t = main.table(&["db"], "t").await?;
    let snapshots = catalog.table_snapshots(&t).await?;
    assert!(snapshots.len() >= 6, "{snapshots:?}");
    let refs = catalog.describe_refs().await?;
    for snapshot in &snapshots {
        let on = refs.iter().find(|r| r.name == snapshot.ref_name).expect("its ref");
        let (r, id, at) = (on.ref_id, t.id(), snapshot.snapshot_id);
        let listed: Value = session.query_one(LISTED, &[&r, &id, &at]).await?.get(0);
        let served: Value = session.query_one(REFERENCE, &[&id, &at]).await?.get(0);
        assert_eq!(by_file(listed), by_file(served), "at {snapshot:?}");
    }
    Ok(())
}

/// A merged file has several rows of `data_file`: a list names one, or it would count the file twice.
#[tokio::test]
async fn merged_file_listed_once() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let config = CatalogConfig::new(url(), &schema).warehouse("memory:///wh");
    let catalog = Catalog::init(config, None).await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let shape = batch(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;
    let branch = main.create_branch("b").await?;
    let t = branch.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(0..5)).await?;
    let added = writer.finish().await?;
    branch.transaction().append(added).commit().await?;
    branch.merge().commit().await?;
    let t = main.table(&["db"], "t").await?;
    let listed: Vec<_> = Scan::new(&catalog, &t).list().await?.try_collect().await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].record_count, 5);
    assert_eq!(listed[0].stat_field_ids, [1]);
    Ok(())
}
