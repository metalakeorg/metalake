//! C9d (ADR 0003 §7): the path of C7 through SQL. A branch and a tag are catalogs; the past is a catalog's name with a
//! point, a sequence or a time; a read in the past takes no write; a bad point is an error. And a statement reads one
//! point (VIS-11): tables written by one commit agree, whatever a writer does while the statement runs.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Storage, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;
use metalake_pg::records::FileAdd;
use metalake_pg::{Pool, Protocol};

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn lake(tables: &[&str]) -> Result<(Catalog, Ref, Storage, String), Error> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    let shape = batch(0..1).schema();
    let mut tx = main.transaction().create_namespace(&["db"]);
    for name in tables {
        (tx, _) = tx.create_table(NewTable {
            namespace: &["db"],
            name,
            schema: &shape,
            location: None,
            cluster_key: vec!["k"],
            partitioning: vec![],
            properties: vec![],
        })?;
    }
    tx.commit().await?;
    Ok((catalog, main, storage, schema))
}

async fn files(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    name: &str,
    keys: std::ops::Range<i64>,
) -> Result<Vec<FileAdd>, Error> {
    let t = on.table(&["db"], name).await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    writer.finish().await
}

async fn keys(context: &SessionContext, table: &str) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let sql = format!("select k from {table} order by k");
    let batches = context.sql(&sql).await?.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect())
}

fn rfc3339(time: std::time::SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339()
}

#[tokio::test]
async fn sql_branches_tags_time_travel() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, schema) = lake(&["t"]).await?;
    let context = MetaLake::new(catalog.clone(), storage).await?.context()?;
    context
        .sql("insert into main.db.t values (1), (2)")
        .await?
        .collect()
        .await?;
    let first = main.table(&["db"], "t").await?.snapshot_id().unwrap();
    main.at(first).create_tag("v1").await?;
    context.sql("insert into main.db.t values (3)").await?.collect().await?;
    let second = main.table(&["db"], "t").await?.snapshot_id().unwrap();
    main.create_branch("dev").await?;

    // A branch, written apart; a tag stays.
    context.sql("insert into dev.db.t values (4)").await?.collect().await?;
    assert_eq!(keys(&context, "dev.db.t").await?, [1, 2, 3, 4]);
    assert_eq!(keys(&context, "main.db.t").await?, [1, 2, 3]);
    assert_eq!(keys(&context, "v1.db.t").await?, [1, 2]);

    // By sequence.
    assert_eq!(keys(&context, &format!(r#""main@{first}".db.t"#)).await?, [1, 2]);
    assert_eq!(keys(&context, &format!(r#""main@{second}".db.t"#)).await?, [1, 2, 3]);

    // By time, as of the commit that wrote the first rows.
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    let (at_first, at_second) = (
        p.commit_time(&session, first).await?,
        p.commit_time(&session, second).await?,
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        keys(&context, &format!(r#""main@{}".db.t"#, rfc3339(at_first))).await?,
        [1, 2]
    );
    assert_eq!(
        keys(&context, &format!(r#""main@{}".db.t"#, rfc3339(at_second))).await?,
        [1, 2, 3]
    );

    // The past together with now, in one statement.
    let sql = format!(r#"select count(*) as c from main.db.t now cross join "main@{first}".db.t old"#);
    let joined = context.sql(&sql).await?.collect().await?;
    assert_eq!(joined[0].column(0).as_primitive::<Int64Type>().value(0), 3 * 2);

    // The past takes no write, and a point that is none is an error of the query.
    let sql = format!(r#"insert into "main@{first}".db.t values (9)"#);
    let refused = match context.sql(&sql).await {
        Ok(statement) => statement.collect().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(refused.is_err_and(|e| e.to_string().contains("past")));
    assert!(context.sql(r#"select * from "main@soon".db.t"#).await.is_err());
    assert!(context.sql(r#"select * from "nothing@1".db.t"#).await.is_err());
    Ok(())
}

#[tokio::test]
async fn statement_snapshot_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, _) = lake(&["a", "b"]).await?;
    let context = MetaLake::new(catalog.clone(), storage.clone()).await?.context()?;

    // Planned before a commit and run after it: both tables read after it, together.
    let sql = "select (select count(*) from main.db.a) as a, (select count(*) from main.db.b) as b";
    let both = [
        files(&catalog, &main, &storage, "a", 0..1).await?,
        files(&catalog, &main, &storage, "b", 0..1).await?,
    ]
    .concat();
    let planned = context.sql(sql).await?;
    main.transaction().append(both).commit().await?;
    let row = planned.collect().await?;
    let value = |c: usize| row[0].column(c).as_primitive::<Int64Type>().value(0);
    assert_eq!((value(0), value(1)), (1, 1));

    // A writer of one commit over both tables, against statements reading them: they never disagree.
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (catalog, main, storage, done) = (catalog.clone(), main.clone(), storage.clone(), done.clone());
        tokio::spawn(async move {
            let mut at = 1;
            while !done.load(Ordering::Relaxed) {
                let both = [
                    files(&catalog, &main, &storage, "a", at..at + 1).await?,
                    files(&catalog, &main, &storage, "b", at..at + 1).await?,
                ]
                .concat();
                main.transaction().append(both).commit().await?;
                at += 1;
            }
            Ok::<i64, Error>(at)
        })
    };
    for _ in 0..40 {
        let row = context.sql(sql).await?.collect().await?;
        let value = |c: usize| row[0].column(c).as_primitive::<Int64Type>().value(0);
        assert_eq!(value(0), value(1));
    }
    done.store(true, Ordering::Relaxed);
    assert!(writer.await?? > 1, "the writer wrote while they read");
    Ok(())
}

#[tokio::test]
async fn branch_inherited_read_and_cross_ref_join() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, _) = lake(&["a"]).await?;
    let context = MetaLake::new(catalog.clone(), storage).await?.context()?;
    context
        .sql("insert into main.db.a values (1), (2)")
        .await?
        .collect()
        .await?;
    let dev = main.create_branch("dev").await?;
    context.sql("insert into main.db.a values (3)").await?.collect().await?;

    // Untouched on the branch: read at the branch's base, not at the older snapshot, below which it sees nothing.
    let inherited = dev.table(&["db"], "a").await?;
    assert!(inherited.seq() > inherited.snapshot_id().unwrap());
    assert_eq!(keys(&context, "dev.db.a").await?, [1, 2]);

    // Two refs in one statement, each with its own state.
    let sql = "select count(*) as c from main.db.a m cross join dev.db.a d";
    let joined = context.sql(sql).await?.collect().await?;
    assert_eq!(joined[0].column(0).as_primitive::<Int64Type>().value(0), 3 * 2);

    // Once the branch writes the table, it reads its own snapshot, and a read of it is a point that stays.
    context.sql("insert into dev.db.a values (4)").await?.collect().await?;
    let own = dev.table(&["db"], "a").await?;
    assert_eq!(Some(own.seq()), own.snapshot_id());
    assert_eq!(keys(&context, "dev.db.a").await?, [1, 2, 4]);
    Ok(())
}
