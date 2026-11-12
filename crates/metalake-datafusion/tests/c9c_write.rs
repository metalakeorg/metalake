//! C9c (ADR 0003 §7): writing through SQL. An `insert` is one commit however many files it makes, read back at once,
//! from values or a query, in any column order; an inline-sized one stays inline. A commit the protocol refuses shows
//! nothing and leaves only orphans. A `delete` matches by filter, as vectors, and only what it matches, on the ref it
//! names. A tag takes no statement, and what a table cannot do yet is refused, not done differently.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Outcome, Ref, Scan, Storage, Table};
use metalake_datafusion::{MetaLake, WrittenRow};
use metalake_pg::{Commit, Pool, Protocol};
use serde_json::json;

fn shape() -> Schema {
    Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ])
}

struct Lake {
    catalog: Catalog,
    main: Ref,
    schema: String,
    context: SessionContext,
}

async fn lake() -> Result<Lake, Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    let shape = shape();
    let table = |name| NewTable {
        namespace: &["db"],
        name,
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    };
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(table("t"))?;
    let (tx, _) = tx.create_table(NewTable {
        properties: vec![],
        ..table("small")
    })?;
    let (tx, _) = tx.create_table(table("source"))?;
    tx.commit().await?;
    let context = MetaLake::new(catalog.clone(), storage).await?.context()?;
    Ok(Lake {
        catalog,
        main,
        schema,
        context,
    })
}

impl Lake {
    /// How many rows a statement says it wrote.
    async fn run(&self, sql: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches[0].column(0).as_primitive::<UInt64Type>().value(0))
    }

    async fn keys(&self, table: &str) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let sql = format!("select k from {table} order by k");
        let batches: Vec<RecordBatch> = self.context.sql(&sql).await?.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect())
    }

    async fn table(&self, name: &str) -> Result<Table, Error> {
        self.main.table(&["db"], name).await
    }

    /// How many rows `name` had just before its latest commit: with what a statement wrote gone, it wrote in one.
    async fn rows_before_last_commit(&self, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let seq = self.table(name).await?.snapshot_id().unwrap();
        let then = self.main.at(seq - 1).table(&["db"], name).await?;
        Ok(self.files(&then).await?.iter().map(|f| f.record_count as u64).sum())
    }

    async fn snapshot(&self, name: &str) -> Result<Option<i64>, Error> {
        Ok(self.table(name).await?.snapshot_id())
    }

    async fn files(&self, t: &Table) -> Result<Vec<metalake::ScanTask>, Error> {
        Scan::new(&self.catalog, t).plan().await?.try_collect().await
    }
}

#[tokio::test]
async fn insert_single_commit() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    assert_eq!(lake.run("insert into main.db.t values (1, 'a'), (2, null)").await?, 2);
    assert_eq!(lake.keys("main.db.t").await?, [1, 2]);

    // Columns in another order, and one left out.
    assert_eq!(lake.run("insert into main.db.t (s, k) values ('c', 3)").await?, 1);
    assert_eq!(lake.run("insert into main.db.t (k) values (4)").await?, 1);
    let row = lake
        .context
        .sql("select s from main.db.t where k = 3")
        .await?
        .collect()
        .await?;
    assert_eq!(row[0].num_rows(), 1);
    assert_eq!(lake.keys("main.db.t").await?, [1, 2, 3, 4]);

    // From a query over many files, into one commit. What was read keeps reading.
    for at in 0..4 {
        let sql = format!("insert into main.db.source values ({at}, 'x'), ({}, 'y')", at + 100);
        lake.run(&sql).await?;
    }
    let sql = "insert into main.db.t select k + 1000, s from main.db.source where k >= 100";
    assert_eq!(lake.run(sql).await?, 4);
    assert_eq!(lake.rows_before_last_commit("t").await?, 4, "all four in one commit");
    assert_eq!(lake.keys("main.db.t").await?, [1, 2, 3, 4, 1100, 1101, 1102, 1103]);

    // A table can read itself into itself, and nothing inserted is nothing committed but an empty answer.
    assert_eq!(
        lake.run("insert into main.db.t select * from main.db.t where k = 1")
            .await?,
        1
    );
    assert_eq!(lake.keys("main.db.t").await?.len(), 9);
    assert_eq!(
        lake.run("insert into main.db.t select * from main.db.t where k = -1")
            .await?,
        0
    );
    assert_eq!(lake.keys("main.db.t").await?.len(), 9);

    // A small insert stays inline, under the table's limit.
    lake.run("insert into main.db.small values (1, 'a')").await?;
    let small = lake.table("small").await?;
    assert!(lake.files(&small).await?.iter().all(|f| f.path.is_none()));
    assert_eq!(lake.keys("main.db.small").await?, [1]);
    Ok(())
}

#[tokio::test]
async fn refused_commit_rolls_back() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    let t = lake.table("t").await?;
    // Planned, not yet run; then the table goes, and the commit has nowhere to land.
    let statement = lake.context.sql("insert into main.db.t values (1, 'a')").await?;
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&lake.schema);
    let drop = Commit {
        ref_id: lake.main.id(),
        operation: "update".into(),
        changes: Some(json!({ t.id().to_string(): {"drop": true} })),
        ..Default::default()
    };
    p.commit(&session, &drop).await?;
    let refused = statement.collect().await;
    assert!(refused.is_err(), "{refused:?}");

    // Its file is there, and an orphan once its intent expires.
    let later = SystemTime::now() + Duration::from_secs(7200);
    assert_eq!(p.orphans(&session, Some(later)).await?.len(), 1);
    assert!(matches!(lake.table("t").await, Err(Error::NotFound(_))));
    Ok(())
}

#[tokio::test]
async fn delete_by_predicate_on_ref() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 'a'), (2, 'b'), (3, null)")
        .await?;
    lake.run("insert into main.db.t values (4, 'a'), (5, 'b')").await?;
    lake.main.create_branch("dev").await?;

    // Nothing matches: no commit.
    let before = lake.snapshot("t").await?;
    assert_eq!(lake.run("delete from main.db.t where k > 100").await?, 0);
    assert_eq!(lake.snapshot("t").await?, before);

    // A filter off the cluster key, across files; the row where it is null does not match. The files stay, with
    // vectors, and the branch reads what it did.
    assert_eq!(lake.run("delete from main.db.t where s = 'a'").await?, 2);
    assert_ne!(lake.snapshot("t").await?, before);
    assert_eq!(lake.keys("main.db.t").await?, [2, 3, 5]);
    assert_eq!(lake.keys("dev.db.t").await?, [1, 2, 3, 4, 5]);
    let t = lake.table("t").await?;
    let files = lake.files(&t).await?;
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .all(|f| f.vector.as_ref().is_some_and(|v| v.cardinality == 1))
    );

    // On the cluster key, and on the branch only.
    assert_eq!(lake.run("delete from dev.db.t where k = 5").await?, 1);
    assert_eq!(lake.keys("dev.db.t").await?, [1, 2, 3, 4]);
    assert_eq!(lake.keys("main.db.t").await?, [2, 3, 5]);

    // Everything.
    assert_eq!(lake.run("delete from main.db.t").await?, 3);
    assert!(lake.keys("main.db.t").await?.is_empty());
    assert_eq!(lake.keys("dev.db.t").await?, [1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn tag_refuses_writes() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 'a')").await?;
    lake.main.create_tag("v1").await?;
    for sql in ["insert into v1.db.t values (2, 'b')", "delete from v1.db.t"] {
        let refused = match lake.context.sql(sql).await {
            Ok(statement) => statement.collect().await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(refused.is_err_and(|e| e.to_string().contains("tag")), "{sql}");
    }
    assert_eq!(lake.keys("v1.db.t").await?, [1]);
    Ok(())
}

/// What a commit says comes in the session's options, which SQL sets and which travel to any process: an `insert`
/// commits with its author and message, and one with a writer's key and a version it already used applies once.
#[tokio::test]
async fn insert_commit_options() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    let context = &lake.context;
    for set in [
        "set metalake.author = 'ana'",
        "set metalake.message = 'two rows'",
        "set metalake.app = 'loader'",
        "set metalake.app_version = 1",
    ] {
        context.sql(set).await?.collect().await?;
    }
    let insert = "insert into main.db.t values (1, 'a'), (2, 'b')";
    let written = context.sql(insert).await?.collect().await?;
    let last = &lake.main.history(1).await?[0];
    assert_eq!(
        (last.author.as_deref(), last.message.as_deref()),
        (Some("ana"), Some("two rows"))
    );
    // What came of the commit is the write's row, with its count.
    let row = WrittenRow::of(&written[0])?;
    assert_eq!((row.count, row.outcome), (2, Some(Outcome::Committed(last.commit_seq))));

    // The same writer and version again: already applied, nothing written twice.
    let again = context.sql(insert).await?.collect().await?;
    assert_eq!(WrittenRow::of(&again[0])?.outcome, Some(Outcome::AlreadyApplied(last.commit_seq)));
    let count = context.sql("select count(*) from main.db.t").await?.collect().await?;
    assert_eq!(count[0].column(0).as_primitive::<Int64Type>().value(0), 2);
    Ok(())
}
