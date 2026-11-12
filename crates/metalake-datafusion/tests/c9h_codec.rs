//! C9h (ADR 0006 §4.2): a plan that crosses processes. A statement is planned on one lake, its logical plan decoded on
//! another, connected apart to the same lake as a scheduler would be, planned there, and its physical plan decoded by a
//! codec with storage and no catalog, as an executor's. What it reads there is what it reads here: a vector's rows stay
//! deleted, an inline file is read with no object, a renamed column by its field id, a statement at its point, and a
//! file split in ranges once. What is not MetaLake's goes to the inner codec; a scan that says each row's file and
//! position is refused, not sent without them.

#![cfg(feature = "proto")]

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext, collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use metalake::{Catalog, CatalogConfig, NewTable, Storage, TableWriter, WriteOptions};
use metalake_datafusion::{MetaLake, MetaLakePhysicalCodec, written_schema};
use metalake_pg::{Commit, Pool, Protocol};
use serde_json::json;

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

/// Where a statement is planned, where it is decoded, and the storage an executor reads with.
struct Lakes {
    here: MetaLake,
    there: MetaLake,
    storage: Storage,
    /// The sequence before `t`'s delete.
    before: i64,
}

/// A lake with `main.db.t`, keys 0..10 in an object, its `s` then renamed `label`, then key 3 deleted; and
/// `main.db.u`, keys 5..15 in an inline file.
async fn lakes() -> Result<Lakes, Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    let shape = shape();
    let new = |name, properties| NewTable {
        namespace: &["db"],
        name,
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties,
    };
    let (tx, t_id) = main
        .transaction()
        .create_namespace(&["db"])
        .create_table(new("t", vec![("inline.max-bytes", "0")]))?;
    tx.commit().await?;
    let (tx, _) = main.transaction().create_table(new("u", vec![]))?;
    tx.commit().await?;
    for (name, keys) in [("t", 0..10), ("u", 5..15)] {
        let table = main.table(&["db"], name).await?;
        let mut writer = TableWriter::new(&catalog, &table, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(keys)).await?;
        main.transaction().append(writer.finish().await?).commit().await?;
    }

    // `s` becomes `label`: the same field id, the files keep their old name.
    let t = main.table(&["db"], "t").await?;
    let s = t.fields().iter().find(|f| f.name == "s").unwrap();
    let renamed = json!({"field_id": s.field_id, "parent_field_id": s.parent_field_id, "position": s.position,
                         "name": "label", "type": "string", "required": s.required});
    let pool = Pool::new(&url(), 1)?;
    let commit = Commit {
        ref_id: main.id(),
        operation: "update".into(),
        changes: Some(json!({ t_id.to_string(): { "fields": [renamed] } })),
        ..Default::default()
    };
    Protocol::new(&schema).commit(&pool.get().await?, &commit).await?;
    let before = main.table(&["db"], "t").await?.seq();

    let here = MetaLake::new(catalog, storage.clone()).await?;
    here.context()?
        .sql("delete from main.db.t where k = 3")
        .await?
        .collect()
        .await?;
    let elsewhere = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    let there = MetaLake::new(elsewhere, storage.clone()).await?;
    Ok(Lakes {
        here,
        there,
        storage,
        before,
    })
}

/// A session of `lake`, with `settings` run first.
async fn session(lake: &MetaLake, settings: &[&str]) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let context = lake.context()?;
    for setting in settings {
        context.sql(setting).await?.collect().await?;
    }
    Ok(context)
}

/// The rows of `sql` read here; the rows it reads after crossing to a scheduler and to an executor; and the plan the
/// executor decoded.
async fn both(
    lakes: &Lakes,
    sql: &str,
    settings: &[&str],
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let context = session(&lakes.here, settings).await?;
    let direct = context.sql(sql).await?.collect().await?;

    // The client: its optimized logical plan.
    let plan = context.sql(sql).await?.into_optimized_plan()?;
    let client = lakes.here.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let logical = logical_plan_to_bytes_with_extension_codec(&plan, &client)?;

    // The scheduler: plans it with its own catalog, and encodes the physical plan.
    let remote = session(&lakes.there, settings).await?;
    let scheduler = lakes.there.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let decoded = logical_plan_from_bytes_with_extension_codec(&logical, &remote.task_ctx(), &scheduler)?;
    let physical = remote.state().create_physical_plan(&decoded).await?;
    let planned = scan_statistics(&physical);
    let encoder = lakes.there.physical_codec(Arc::new(DefaultPhysicalExtensionCodec {}));
    let physical = physical_plan_to_bytes_with_extension_codec(physical, &encoder)?;

    // The executor: storage, no catalog, and a session of DataFusion's own with no store registered.
    let executor = MetaLakePhysicalCodec::new(lakes.storage.clone(), Arc::new(DefaultPhysicalExtensionCodec {}));
    let task = SessionContext::new().task_ctx();
    let plan = physical_plan_from_bytes_with_extension_codec(&physical, &task, &executor)?;
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    // Each partition of each scan knows what it knew before it travelled.
    assert_eq!(scan_statistics(&plan), planned, "{shown}");
    let travelled = collect(plan, task).await?;
    Ok((
        pretty_format_batches(&direct)?.to_string(),
        pretty_format_batches(&travelled)?.to_string(),
        shown,
    ))
}

/// The statistics of each partition of each MetaLake scan in `plan`, in the order a walk of it meets them.
fn scan_statistics(plan: &Arc<dyn ExecutionPlan>) -> Vec<Statistics> {
    let mut found = Vec::new();
    let mut nodes = vec![Arc::clone(plan)];
    while let Some(node) = nodes.pop() {
        if node.name() == "MetaScanExec" {
            for partition in 0..node.properties().partitioning.partition_count() {
                let args = StatisticsArgs::new().with_partition(Some(partition));
                let statistics = StatisticsContext::new().compute(node.as_ref(), &args).unwrap();
                found.push(statistics.as_ref().clone());
            }
        }
        nodes.extend(node.children().into_iter().cloned());
    }
    found
}

#[tokio::test]
async fn round_trip_vector_inline_rename() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    let sql = "select t.k, t.label, u.s from main.db.t join main.db.u on t.k = u.k where t.k < 9 order by 1";
    let (direct, travelled, shown) = both(&lakes, sql, &[]).await?;
    assert_eq!(direct, travelled);
    assert!(direct.contains("| 8 | s8    | s8 |"), "{direct}");
    assert!(!direct.contains("| 3 |"), "{direct}");
    assert!(shown.contains("inline/") && shown.contains(".parquet"), "{shown}");
    Ok(())
}

/// A scan with nothing pushed into it knows its rows per partition, before and after it travels.
#[tokio::test]
async fn round_trip_partition_statistics() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    let sql = "select k, s from main.db.u order by k";
    let (direct, travelled, _) = both(&lakes, sql, &[]).await?;
    assert_eq!(direct, travelled);
    let context = lakes.here.context()?;
    let plan = context.sql(sql).await?.create_physical_plan().await?;
    let statistics = scan_statistics(&plan);
    assert!(!statistics.is_empty());
    assert!(
        statistics.iter().all(|s| !matches!(s.num_rows, Precision::Absent)),
        "{statistics:?}"
    );
    Ok(())
}

#[tokio::test]
async fn round_trip_at_point() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    // Before the delete: key 3 is there, at the sequence the statement named, wherever it is read.
    let sql = format!("select k, label from \"main@{}\".db.t where k = 3", lakes.before);
    let (direct, travelled, _) = both(&lakes, &sql, &[]).await?;
    assert_eq!(direct, travelled);
    assert!(direct.contains("| 3 | s3    |"), "{direct}");
    Ok(())
}

#[tokio::test]
async fn round_trip_file_in_ranges() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    let settings = [
        "set datafusion.optimizer.repartition_file_min_size = 1",
        "set datafusion.execution.target_partitions = 4",
    ];
    let sql = "select count(*) as c, sum(k) as s from main.db.t where label <> 'x'";
    let (direct, travelled, shown) = both(&lakes, sql, &settings).await?;
    assert_eq!(direct, travelled);
    assert!(direct.contains("| 9 | 42 |"), "{direct}");
    assert!(shown.matches(".parquet:").count() >= 2, "one file in ranges: {shown}");
    Ok(())
}

/// A write's sink commits where it runs: an insert, a delete, an update or a merge travels, and a process without a
/// catalog refuses to run it; one with the lake's catalog takes it.
#[tokio::test]
async fn encode_writes() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    let context = lakes.here.context()?;
    let codec = lakes.here.physical_codec(Arc::new(DefaultPhysicalExtensionCodec {}));
    let reader = MetaLakePhysicalCodec::new(lakes.storage.clone(), Arc::new(DefaultPhysicalExtensionCodec {}));
    let writer = lakes.there.physical_codec(Arc::new(DefaultPhysicalExtensionCodec {}));
    let task = SessionContext::new().task_ctx();
    context.sql("set datafusion.execution.target_partitions = 16").await?.collect().await?;
    for sql in [
        "insert into main.db.t select k, s from main.db.u",
        "delete from main.db.t where k = 4",
        "update main.db.t set label = 'x' where k = 5",
        "merge into main.db.t t using main.db.u u on t.k = u.k when matched then delete",
    ] {
        let plan = context.sql(sql).await?.create_physical_plan().await?;
        // The sink right under the write's row, however many partitions: no repartition between them.
        assert_eq!(plan.children()[0].name(), "DataSinkExec", "{sql}");
        let bytes = physical_plan_to_bytes_with_extension_codec(plan, &codec)?;
        let refused = physical_plan_from_bytes_with_extension_codec(&bytes, &task, &reader).unwrap_err();
        assert!(refused.to_string().contains("no catalog"), "{sql}: {refused}");
        // Around its sink, the write's row, which crosses with it.
        let decoded = physical_plan_from_bytes_with_extension_codec(&bytes, &task, &writer)?;
        assert_eq!((decoded.name(), decoded.schema()), ("MetaWriteExec", written_schema()), "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn encode_foreign_table_to_inner() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    // A table that is not the lake's, in a session of DataFusion's own.
    let context = SessionContext::new();
    let memory = MemTable::try_new(shape(), vec![vec![batch(0..2)]])?;
    context.register_table("m", Arc::new(memory))?;
    let plan = context.sql("select k from m").await?.into_optimized_plan()?;
    let codec = lakes.here.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let refused = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap_err();
    assert!(
        refused.to_string().contains("LogicalExtensionCodec is not provided"),
        "{refused}"
    );
    Ok(())
}

/// `table_changes` crosses both hops as its value, the table and its tasks: the executor reads the commits' rows, live
/// and deleted, with storage and no catalog, as a local read does.
#[tokio::test]
async fn round_trip_table_changes() -> Result<(), Box<dyn std::error::Error>> {
    let lakes = lakes().await?;
    let context = lakes.here.context()?;
    context
        .sql("insert into main.db.t values (50, 'x')")
        .await?
        .collect()
        .await?;
    // After the rename: key 3 deleted, key 50 inserted.
    // With no end, the range reaches the last commit a second old (CHG-1).
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let sql = format!(
        "select k, _change_type from table_changes('main.db.t', {}) order by k, _change_type",
        lakes.before
    );
    let statement = || DFParser::parse_sql(&sql).map(|mut s| s.pop_front().unwrap());
    let direct = lakes.here.execute(&context, statement()?).await?.collect().await?;

    let plan = lakes
        .here
        .execute(&context, statement()?)
        .await?
        .into_optimized_plan()?;
    let client = lakes.here.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let logical = logical_plan_to_bytes_with_extension_codec(&plan, &client)?;
    let remote = lakes.there.context()?;
    let scheduler = lakes.there.logical_codec(Arc::new(DefaultLogicalExtensionCodec {}));
    let decoded = logical_plan_from_bytes_with_extension_codec(&logical, &remote.task_ctx(), &scheduler)?;
    let physical = remote.state().create_physical_plan(&decoded).await?;
    let encoder = lakes.there.physical_codec(Arc::new(DefaultPhysicalExtensionCodec {}));
    let physical = physical_plan_to_bytes_with_extension_codec(physical, &encoder)?;
    let reader = MetaLakePhysicalCodec::new(lakes.storage.clone(), Arc::new(DefaultPhysicalExtensionCodec {}));
    let task = SessionContext::new().task_ctx();
    let plan = physical_plan_from_bytes_with_extension_codec(&physical, &task, &reader)?;
    let travelled = collect(plan, task).await?;

    let (direct, travelled) = (
        pretty_format_batches(&direct)?.to_string(),
        pretty_format_batches(&travelled)?.to_string(),
    );
    assert_eq!(direct, travelled);
    assert!(
        direct.contains("| 3  | delete") && direct.contains("| 50 | insert"),
        "{direct}"
    );
    Ok(())
}
