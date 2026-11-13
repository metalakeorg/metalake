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
use datafusion::sql::parser::DFParser;
use metalake::{Catalog, CatalogConfig, NewTable, Outcome, Ref, Storage, TableWriter, WriteOptions};
use metalake_ballista::Standalone;
use metalake_datafusion::{MetaLake, WrittenRow};
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

/// A local session of the lake, the same lake on a cluster in this process, the sequence before `t`'s delete,
/// `main`, to read its commits, and the client's lake, which runs what a statement needs it to (`table_changes`). The lake: `main.db.t`, keys 0..10 in an object, its `s` then renamed `label`, then key 3 deleted; and
/// `main.db.u`, keys 5..15 in an inline file.
async fn sessions() -> Result<(SessionContext, Standalone, i64, Ref, MetaLake), Box<dyn std::error::Error>> {
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

    // The scheduler's lake, connected apart; the executor, storage and, to commit a write, a catalog of its own.
    let scheduler = MetaLake::new(
        Catalog::connect(CatalogConfig::new(url(), &schema)).await?,
        storage.clone(),
    )
    .await?;
    let writes = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    let cluster = metalake_ballista::standalone(&client, scheduler, storage, Some(writes)).await?;
    // On the cluster, whatever the router's rule would say of these small tables.
    cluster.sql("set metalake.engine = 'cluster'").await?.collect().await?;
    Ok((local, cluster, before, main, client))
}

/// The row a write answers.
async fn written(context: &SessionContext, sql: &str) -> Result<WrittenRow, Box<dyn std::error::Error>> {
    let batches = context.sql(sql).await?.collect().await?;
    Ok(WrittenRow::of(&batches[0])?)
}

async fn rows(context: &SessionContext, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(pretty_format_batches(&context.sql(sql).await?.collect().await?)?.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_join_vector_inline_rename() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, _, _) = sessions().await?;
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
    let (local, cluster, before, _, _) = sessions().await?;
    let sql = format!("select k, label from \"main@{before}\".db.t where k = 3");
    let (here, there) = (rows(&local, &sql).await?, rows(&cluster, &sql).await?);
    assert_eq!(here, there);
    assert!(there.contains("| 3 | s3    |"), "{there}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ballista_plan_distributed() -> Result<(), Box<dyn std::error::Error>> {
    let (_, cluster, _, _, _) = sessions().await?;
    let plan = cluster
        .sql("select k from main.db.u where k > 7")
        .await?
        .create_physical_plan()
        .await?;
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    assert!(shown.contains("DistributedQueryExec"), "{shown}");
    Ok(())
}

/// `metalake.engine` says where a session's statements run: forced local, its plan runs in the client's process, with
/// the same rows; forced back, on the cluster again. A value that is no engine is refused.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_engine_forced() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, _, _) = sessions().await?;
    let sql = "select k from main.db.u where k > 7 order by k";
    assert!(shown(&cluster, sql).await?.contains("DistributedQueryExec"));
    cluster.sql("set metalake.engine = 'local'").await?.collect().await?;
    let here = shown(&cluster, sql).await?;
    assert!(!here.contains("DistributedQueryExec") && here.contains("MetaScanExec"), "{here}");
    assert_eq!(rows(&cluster, sql).await?, rows(&local, sql).await?);
    cluster.sql("set metalake.engine = 'cluster'").await?.collect().await?;
    assert!(shown(&cluster, sql).await?.contains("DistributedQueryExec"));
    assert!(cluster.sql("set metalake.engine = 'mars'").await.is_err());
    Ok(())
}

/// `auto` lets the rule choose: the tests' tables are far under its thresholds, so their statements run locally; with
/// a threshold of none, on the cluster; the same rows either way.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_engine_auto() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, _, _) = sessions().await?;
    let sql = "select u.k % 3 as g, count(*) as c from main.db.u group by 1 order by 1";
    cluster.sql("set metalake.engine = 'auto'").await?.collect().await?;
    let small = shown(&cluster, sql).await?;
    assert!(!small.contains("DistributedQueryExec") && small.contains("MetaScanExec"), "{small}");
    let here = rows(&cluster, sql).await?;
    for set in [
        "set metalake.engine.threshold = 0",
        "set metalake.engine.shuffle_threshold = 0",
    ] {
        cluster.sql(set).await?.collect().await?;
    }
    assert!(shown(&cluster, sql).await?.contains("DistributedQueryExec"));
    assert_eq!(rows(&cluster, sql).await?, here);
    // Writes too, whose reads are in their sinks.
    for write in [
        "insert into main.db.t select k + 100, s from main.db.u",
        "delete from main.db.t where k = 4",
        "update main.db.t set label = 'x' where k = 5",
        "merge into main.db.t t using main.db.u u on t.k = u.k when matched then delete",
    ] {
        assert!(shown(&cluster, write).await?.contains("DistributedQueryExec"), "{write}");
    }
    assert_eq!(here, rows(&local, sql).await?);
    Ok(())
}

/// A cluster that does not answer: `auto` runs what the rule would send to it locally, and answers; forced to the
/// cluster, the statement fails.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_engine_auto_without_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let (local, _, _, _, client) = sessions().await?;
    let nowhere = metalake_ballista::session(&client, &format!("df://127.0.0.1:{}", closed_port()?)).await?;
    for set in ["set metalake.engine = 'auto'", "set metalake.engine.threshold = 0"] {
        nowhere.sql(set).await?.collect().await?;
    }
    let sql = "select k from main.db.u where k > 7 order by k";
    let shown_here = shown(&nowhere, sql).await?;
    assert!(!shown_here.contains("DistributedQueryExec"), "{shown_here}");
    assert_eq!(rows(&nowhere, sql).await?, rows(&local, sql).await?);
    nowhere.sql("set metalake.engine = 'cluster'").await?.collect().await?;
    assert!(rows(&nowhere, sql).await.is_err());
    Ok(())
}

/// A port nothing listens on: one bound and let go.
fn closed_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr().map(|a| a.port())
}

/// The physical plan of `sql`, shown.
async fn shown(context: &SessionContext, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let plan = context.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
}

/// A client that speaks another version of MetaLake's plans is refused by the cluster, which names both versions; the
/// same session, back to its own, is heard.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_refuses_another_plan_version() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, _, _) = sessions().await?;
    let sql = "select k from main.db.u where k > 7 order by k";
    cluster.sql("set metalake.plan_version = 99").await?.collect().await?;
    let refused = rows(&cluster, sql).await.unwrap_err().to_string();
    assert!(refused.contains("version 99") && refused.contains("matching versions"), "{refused}");
    cluster
        .sql(&format!("set metalake.plan_version = {}", metalake_datafusion::PLAN_VERSION))
        .await?
        .collect()
        .await?;
    assert_eq!(rows(&cluster, sql).await?, rows(&local, sql).await?);
    Ok(())
}

/// An insert runs on the cluster: its rows read there, written and committed by the executor, with what the session's
/// options ask of the commit; again with the same writer's key and version, it applies once. What came of each commit
/// comes back from the executor as the write's row.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_insert() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, main, _) = sessions().await?;
    for set in [
        "set metalake.author = 'ana'",
        "set metalake.message = 'from u'",
        "set metalake.app = 'loader'",
        "set metalake.app_version = 1",
    ] {
        cluster.sql(set).await?.collect().await?;
    }
    let insert = "insert into main.db.t select k + 100, s from main.db.u where k < 8";
    let first = written(&cluster, insert).await?;
    let last = &main.history(1).await?[0];
    assert_eq!(
        (last.author.as_deref(), last.message.as_deref()),
        (Some("ana"), Some("from u"))
    );
    assert_eq!((first.count, first.outcome), (3, Some(Outcome::Committed(last.commit_seq))));

    // The same writer and version again: already applied, nothing written twice, and the row says which commit had.
    let again = written(&cluster, insert).await?;
    assert_eq!(again.outcome, Some(Outcome::AlreadyApplied(last.commit_seq)));
    let sql = "select k, label from main.db.t where k >= 100 order by k";
    let (here, there) = (rows(&local, sql).await?, rows(&cluster, sql).await?);
    assert_eq!(here, there);
    assert_eq!(here.matches("| 10").count(), 3, "{here}");
    Ok(())
}

/// A delete and an update run on the cluster: the executor makes their rows again from the statement's filters (the
/// core's scan, pruned in Postgres, and their filter), and commits with what the session's options ask.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_delete_update() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, main, _) = sessions().await?;
    cluster.sql("set metalake.author = 'bea'").await?.collect().await?;

    let deleted = rows(&cluster, "delete from main.db.t where k >= 7").await?;
    assert!(deleted.contains("| 3     |"), "{deleted}");
    assert_eq!(main.history(1).await?[0].author.as_deref(), Some("bea"));

    let updated = rows(
        &cluster,
        "update main.db.t set label = concat(label, '!') where k in (1, 2)",
    )
    .await?;
    assert!(updated.contains("| 2     |"), "{updated}");
    let last = &main.history(1).await?[0];
    assert_eq!(
        (last.operation.as_str(), last.author.as_deref()),
        // New rows and vectors in one commit: the protocol's overwrite.
        ("overwrite", Some("bea"))
    );

    let sql = "select k, label from main.db.t order by k";
    let (here, there) = (rows(&local, sql).await?, rows(&cluster, sql).await?);
    assert_eq!(here, there);
    assert!(
        here.contains("| 1 | s1!   |") && here.contains("| 6 | s6    |"),
        "{here}"
    );
    assert!(!here.contains("| 7 |") && !here.contains("| 3 |"), "{here}");
    Ok(())
}

/// A merge runs on the cluster: the executor plans the join of the source and the table (each row with its file and
/// position) from the statement it was sent, and commits what its clauses did, once, with the session's author.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_merge() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, _, main, _) = sessions().await?;
    cluster.sql("set metalake.author = 'cai'").await?.collect().await?;
    let merge = "merge into main.db.t t using (select k, s from main.db.u where k < 12) u on t.k = u.k
                 when matched and u.k = 9 then delete
                 when matched then update set label = concat(u.s, '*')
                 when not matched then insert (k, label) values (u.k, u.s)";
    let affected = written(&cluster, merge).await?;
    // Keys 5..12 of u: 5, 6, 7, 8 and 9 match t (9 is deleted), 10 and 11 are new.
    assert_eq!((affected.count, affected.inserted, affected.updated), (7, 2, 5));
    assert!(matches!(affected.outcome, Some(Outcome::Committed(_))));
    assert_eq!(main.history(1).await?[0].author.as_deref(), Some("cai"));

    let sql = "select k, label from main.db.t order by k";
    let (here, there) = (rows(&local, sql).await?, rows(&cluster, sql).await?);
    assert_eq!(here, there);
    assert!(
        here.contains("| 5  | s5*   |") && here.contains("| 11 | s11   |"),
        "{here}"
    );
    assert!(!here.contains("| 9  |") && !here.contains("| 3  |"), "{here}");
    Ok(())
}

/// `table_changes` runs on the cluster: the client finds the range, and it travels as its value to the executor, which
/// reads the commits' rows as a local read does.
#[tokio::test(flavor = "multi_thread")]
async fn ballista_table_changes() -> Result<(), Box<dyn std::error::Error>> {
    let (local, cluster, before, _, client) = sessions().await?;
    rows(&cluster, "insert into main.db.t values (50, 'x')").await?;
    // After the rename: key 3 deleted, key 50 inserted on the cluster.
    // With no end, the range reaches the last commit a second old (CHG-1).
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let sql =
        format!("select k, label, _change_type from table_changes('main.db.t', {before}) order by k, _change_type");
    let statement = || DFParser::parse_sql(&sql).map(|mut s| s.pop_front().unwrap());
    let here = client.execute(&local, statement()?).await?.collect().await?;
    let there = client.execute(&cluster, statement()?).await?.collect().await?;
    let (here, there) = (
        pretty_format_batches(&here)?.to_string(),
        pretty_format_batches(&there)?.to_string(),
    );
    assert_eq!(here, there);
    assert!(
        here.contains("| 3  | s3    | delete") && here.contains("| 50 | x     | insert"),
        "{here}"
    );
    Ok(())
}

/// A cluster's servers end with it: once dropped, its scheduler's port takes no connection.
#[tokio::test(flavor = "multi_thread")]
async fn standalone_drop_stops_servers() -> Result<(), Box<dyn std::error::Error>> {
    let (_, cluster, _, _, _) = sessions().await?;
    let port = cluster.scheduler().rsplit(':').next().unwrap().parse::<u16>()?;
    assert!(tokio::net::TcpStream::connect(("localhost", port)).await.is_ok());
    drop(cluster);
    let mut refused = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("localhost", port)).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        refused,
        "the scheduler on {port} still answers after its cluster was dropped"
    );
    Ok(())
}
