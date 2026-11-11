//! C9h (ADR 0006 §4.2): a plan that crosses processes. A logical plan of MetaLake's tables is encoded by one lake's
//! codec and decoded by another's, connected apart to the same lake as a scheduler would be: its tables come back as
//! values, at the ref and point the statement named, and the plan reads there what it reads here. What is not
//! MetaLake's goes to the inner codec.

#![cfg(feature = "proto")]

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use metalake::{Catalog, CatalogConfig, NewTable, Storage, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;

fn shape() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.map(|k| format!("s{k}")))),
    ];
    RecordBatch::try_new(shape(), columns).unwrap()
}

/// A lake with `main.db.t` (keys 0..10, then 3 deleted) and `main.db.u` (keys 5..15), and the sequence `t` had
/// before its delete. Two lakes on it: where a plan is made, and where it is decoded, each with its own connections.
async fn lakes() -> Result<(MetaLake, MetaLake, i64), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    let shape = shape();
    let new = |name| NewTable {
        namespace: &["db"],
        name,
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    };
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new("t"))?;
    tx.commit().await?;
    let (tx, _) = main.transaction().create_table(new("u"))?;
    tx.commit().await?;
    let mut before = 0;
    for (name, keys) in [("t", 0..10), ("u", 5..15)] {
        let table = main.table(&["db"], name).await?;
        let mut writer = TableWriter::new(&catalog, &table, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(keys)).await?;
        before = main.transaction().append(writer.finish().await?).commit().await?.seq();
    }

    let here = MetaLake::new(catalog, storage.clone()).await?;
    here.context()?
        .sql("delete from main.db.t where k = 3")
        .await?
        .collect()
        .await?;
    let elsewhere = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    let there = MetaLake::new(elsewhere, storage).await?;
    Ok((here, there, before))
}

/// The rows of `sql` read here, and read there from the plan made here.
async fn both(here: &MetaLake, there: &MetaLake, sql: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let context = here.context()?;
    let direct = context.sql(sql).await?.collect().await?;

    let plan = context.sql(sql).await?.into_optimized_plan()?;
    let encoder = here.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &encoder)?;

    let remote = there.context()?;
    let decoder = there.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let decoded = logical_plan_from_bytes_with_extension_codec(&bytes, &remote.task_ctx(), &decoder)?;
    let physical = remote.state().create_physical_plan(&decoded).await?;
    let travelled = collect(physical, remote.task_ctx()).await?;
    Ok((
        pretty_format_batches(&direct)?.to_string(),
        pretty_format_batches(&travelled)?.to_string(),
    ))
}

#[tokio::test]
async fn logical_round_trip_join_with_delete() -> Result<(), Box<dyn std::error::Error>> {
    let (here, there, _) = lakes().await?;
    let sql = "select t.k, u.s from main.db.t join main.db.u on t.k = u.k where t.k < 9 order by 1";
    let (direct, travelled) = both(&here, &there, sql).await?;
    assert_eq!(direct, travelled);
    assert!(direct.contains("| 8 | s8 |") && !direct.contains("| 3 |"), "{direct}");
    Ok(())
}

#[tokio::test]
async fn logical_round_trip_at_point() -> Result<(), Box<dyn std::error::Error>> {
    let (here, there, before) = lakes().await?;
    // Before the delete: key 3 is there, at the sequence the statement named, wherever it is decoded.
    let sql = format!("select count(*) from \"main@{before}\".db.t where k = 3");
    let (direct, travelled) = both(&here, &there, &sql).await?;
    assert_eq!(direct, travelled);
    assert!(direct.contains("| 1 "), "{direct}");
    Ok(())
}

#[tokio::test]
async fn logical_encode_foreign_table_to_inner() -> Result<(), Box<dyn std::error::Error>> {
    let (here, _, _) = lakes().await?;
    // A table that is not the lake's, in a session of DataFusion's own.
    let context = SessionContext::new();
    let memory = MemTable::try_new(shape(), vec![vec![batch(0..2)]])?;
    context.register_table("m", Arc::new(memory))?;
    let plan = context.sql("select k from m").await?.into_optimized_plan()?;
    let codec = here.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let refused = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap_err();
    assert!(
        refused.to_string().contains("LogicalExtensionCodec is not provided"),
        "{refused}"
    );
    Ok(())
}
