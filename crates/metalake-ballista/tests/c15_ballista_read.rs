//! C15 (ADR 0006 milestone 3): reads on Ballista. A cluster in this process, its scheduler on a lake connected apart and
//! its executor with storage and nothing registered, answers what a local session answers: a join over a table with a
//! deleted row, a renamed column and another table's inline file; a statement at a ref's past point. The statement
//! goes through Ballista, not around it; and a write, which does not cross processes yet, fails and does not hang.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use metalake::{Catalog, CatalogConfig, NewTable, Storage, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;
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

/// A local session of the lake, a session of the same lake on a cluster in this process, and the sequence before `t`'s
/// delete. The lake: `main.db.t`, keys 0..10 in an object, its `s` then renamed `label`, then key 3 deleted; and
/// `main.db.u`, keys 5..15 in an inline file.
async fn sessions() -> Result<(SessionContext, SessionContext, i64), Box<dyn std::error::Error>> {
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

    let client = MetaLake::new(catalog, storage.clone()).await?;
    let local = client.context()?;
    local.sql("delete from main.db.t where k = 3").await?.collect().await?;

    // The scheduler's lake, connected apart; the executor, storage alone.
    let scheduler = MetaLake::new(
        Catalog::connect(CatalogConfig::new(url(), &schema)).await?,
        storage.clone(),
    )
    .await?;
    let cluster = metalake_ballista::standalone(&client, scheduler, storage).await?;
    Ok((local, cluster, before))
}

async fn rows(context: &SessionContext, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(pretty_format_batches(&context.sql(sql).await?.collect().await?)?.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_join_vector_inline_rename() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _) = sessions().await?;
    for sql in [
        "select t.k, t.label, u.s from main.db.t join main.db.u on t.k = u.k where t.k < 9 order by 1",
        "select count(*) as c, sum(k) as s, min(label) as l from main.db.t",
        "select u.k % 3 as g, count(*) as c from main.db.u group by 1 order by 1",
    ] {
        let (here, there) = (rows(&local, sql).await?, rows(&cluster, sql).await?);
        assert_eq!(here, there, "{sql}");
    }
    let joined = rows(
        &cluster,
        "select t.k from main.db.t join main.db.u on t.k = u.k order by 1",
    )
    .await?;
    assert!(joined.contains("| 8 |") && !joined.contains("| 3 |"), "{joined}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_at_point() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, before) = sessions().await?;
    let sql = format!("select k, label from \"main@{before}\".db.t where k = 3");
    let (here, there) = (rows(&local, &sql).await?, rows(&cluster, &sql).await?);
    assert_eq!(here, there);
    assert!(there.contains("| 3 | s3    |"), "{there}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_plan_distributed() -> Result<(), Box<dyn std::error::Error>> {
    let (_, cluster, _) = sessions().await?;
    let plan = cluster
        .sql("select k from main.db.u where k > 7")
        .await?
        .create_physical_plan()
        .await?;
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    assert!(shown.contains("DistributedQueryExec"), "{shown}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_write_fails() -> Result<(), Box<dyn std::error::Error>> {
    let (_, cluster, _) = sessions().await?;
    let insert = async {
        cluster
            .sql("insert into main.db.t select k, s from main.db.u")
            .await?
            .collect()
            .await
    };
    let done = tokio::time::timeout(Duration::from_secs(60), insert).await;
    assert!(matches!(done, Ok(Err(_))), "a write fails, and does not hang: {done:?}");
    Ok(())
}
