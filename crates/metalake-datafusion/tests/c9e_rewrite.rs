//! C9e (ADR 0003 §7): rewriting rows through SQL. An overwrite replaces a table whole, in one commit, with the past
//! and branches untouched. An update is a delete and a write in one commit, with expressions over the row. What matters
//! most: neither commutes, so a commit that got in first refuses them, and no row comes back or doubles.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, ScanTask, Storage, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;
use metalake_pg::ErrorKind;

struct Lake {
    catalog: Catalog,
    main: Ref,
    storage: Storage,
    context: SessionContext,
}

async fn lake() -> Result<Lake, Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    let shape = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("n", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]);
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
    let (tx, _) = tx.create_table(table("source"))?;
    tx.commit().await?;
    let context = MetaLake::new(catalog.clone(), storage.clone()).await?.context()?;
    Ok(Lake {
        catalog,
        main,
        storage,
        context,
    })
}

impl Lake {
    async fn run(&self, sql: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches[0].column(0).as_primitive::<UInt64Type>().value(0))
    }

    /// The rows of `table` as `(k, n)`, `n` null as -1.
    async fn rows(&self, table: &str) -> Result<Vec<(i64, i32)>, Box<dyn std::error::Error>> {
        let sql = format!("select k, cast(coalesce(n, -1) as int) as n from {table} order by k, n");
        let batches = self.context.sql(&sql).await?.collect().await?;
        let mut rows = Vec::new();
        for b in batches {
            let k = b.column(0).as_primitive::<Int64Type>();
            let n = b.column(1).as_primitive::<Int32Type>();
            rows.extend(k.values().iter().copied().zip(n.values().iter().copied()));
        }
        Ok(rows)
    }

    async fn tasks(&self) -> Result<Vec<ScanTask>, Error> {
        let t = self.main.table(&["db"], "t").await?;
        Scan::new(&self.catalog, &t).plan().await?.try_collect().await
    }
}

#[tokio::test]
async fn an_overwrite_replaces_the_table_whole() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b')")
        .await?;
    lake.run("insert into main.db.t values (3, 3, 'c')").await?;
    lake.run("insert into main.db.source values (7, 7, 'x'), (8, 8, 'y')")
        .await?;
    let before = lake.main.table(&["db"], "t").await?.snapshot_id().unwrap();
    lake.main.create_branch("dev").await?;

    assert_eq!(
        lake.run("insert overwrite main.db.t select * from main.db.source")
            .await?,
        2
    );
    assert_eq!(lake.rows("main.db.t").await?, [(7, 7), (8, 8)]);
    assert_eq!(lake.tasks().await?.len(), 1);
    assert_eq!(
        lake.rows("dev.db.t").await?,
        [(1, 1), (2, 2), (3, 3)],
        "a branch keeps its rows"
    );
    let past = format!(r#""main@{before}".db.t"#);
    assert_eq!(
        lake.rows(&past).await?,
        [(1, 1), (2, 2), (3, 3)],
        "and the past its own"
    );

    // From values, on a branch, and with nothing, which empties a table.
    assert_eq!(lake.run("insert overwrite dev.db.t values (9, 9, 'z')").await?, 1);
    assert_eq!(lake.rows("dev.db.t").await?, [(9, 9)]);
    assert_eq!(lake.rows("main.db.t").await?, [(7, 7), (8, 8)]);
    assert_eq!(
        lake.run("insert overwrite main.db.t select * from main.db.t where k < 0")
            .await?,
        0
    );
    assert!(lake.rows("main.db.t").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_update_is_a_delete_and_a_write_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 10, 'a'), (2, 20, 'b'), (3, null, 'c')")
        .await?;
    lake.run("insert into main.db.t values (4, 40, 'a')").await?;
    lake.main.create_branch("dev").await?;
    let before = lake.main.table(&["db"], "t").await?.snapshot_id().unwrap();

    // Off the cluster key, across files, from the row itself.
    assert_eq!(lake.run("update main.db.t set n = n + 1 where s = 'a'").await?, 2);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 11), (2, 20), (3, -1), (4, 41)]);
    let latest = lake.main.table(&["db"], "t").await?.snapshot_id().unwrap();
    let tasks = lake.tasks().await?;
    assert_eq!(tasks.len(), 3, "the two files with vectors, and the new one");
    assert_eq!(tasks.iter().filter(|f| f.vector.is_some()).count(), 2);

    // One commit: the sequence before it has the old values, not some of each.
    let then = format!(r#""main@{}".db.t"#, latest - 1);
    assert_eq!(lake.rows(&then).await?, [(1, 10), (2, 20), (3, -1), (4, 40)]);
    assert_ne!(before, latest);

    // On the cluster key, with a constant of another type, a null, and a column to null.
    assert_eq!(lake.run("update main.db.t set n = 5, s = null where k = 2").await?, 1);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 11), (2, 5), (3, -1), (4, 41)]);
    let nulls = lake
        .context
        .sql("select s from main.db.t where k = 2")
        .await?
        .collect()
        .await?;
    assert!(nulls[0].column(0).is_null(0));

    // A filter null for a row does not match it; nothing matching is no commit; a branch is apart.
    let latest = lake.main.table(&["db"], "t").await?.snapshot_id();
    assert_eq!(lake.run("update main.db.t set n = 0 where n > 1000").await?, 0);
    assert_eq!(lake.main.table(&["db"], "t").await?.snapshot_id(), latest);
    assert_eq!(lake.run("update dev.db.t set n = 0").await?, 4);
    assert_eq!(lake.rows("dev.db.t").await?, [(1, 0), (2, 0), (3, 0), (4, 0)]);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 11), (2, 5), (3, -1), (4, 41)]);

    // What a required column cannot hold is refused, and nothing changes.
    assert!(
        lake.context
            .sql("update main.db.t set k = null where k = 1")
            .await
            .is_err()
            || {
                lake.context
                    .sql("update main.db.t set k = null where k = 1")
                    .await?
                    .collect()
                    .await
                    .is_err()
            }
    );
    assert_eq!(lake.rows("main.db.t").await?, [(1, 11), (2, 5), (3, -1), (4, 41)]);
    Ok(())
}

#[tokio::test]
async fn a_rewrite_loses_to_a_commit_that_got_in_first() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b')")
        .await?;

    // An update planned, then a delete of its row gets in: were the update to go on, its row would come back.
    let update = lake.context.sql("update main.db.t set n = 100 where k = 1").await?;
    lake.run("delete from main.db.t where k = 1").await?;
    assert!(update.collect().await.is_err());
    assert_eq!(lake.rows("main.db.t").await?, [(2, 2)]);

    // Two updates of one row: one stands, and the row is there once.
    let first = lake.context.sql("update main.db.t set n = 100 where k = 2").await?;
    let second = lake.context.sql("update main.db.t set n = 200 where k = 2").await?;
    first.collect().await?;
    assert!(second.collect().await.is_err());
    assert_eq!(lake.rows("main.db.t").await?, [(2, 100)]);

    // Two updates of rows of different files both stand: only what they touch conflicts.
    lake.run("insert into main.db.t values (5, 5, 'e')").await?;
    let first = lake.context.sql("update main.db.t set n = 300 where k = 2").await?;
    let second = lake.context.sql("update main.db.t set n = 500 where k = 5").await?;
    first.collect().await?;
    second.collect().await?;
    assert_eq!(lake.rows("main.db.t").await?, [(2, 300), (5, 500)]);

    // An overwrite planned, then an insert: its rows are not lost in silence.
    let overwrite = lake
        .context
        .sql("insert overwrite main.db.t values (9, 9, 'z')")
        .await?;
    lake.run("insert into main.db.t values (3, 3, 'c')").await?;
    assert!(overwrite.collect().await.is_err());
    assert_eq!(lake.rows("main.db.t").await?, [(2, 300), (3, 3), (5, 500)]);
    Ok(())
}

/// In the core itself: a stale delete alone takes the union again; one that also adds files does not.
#[tokio::test]
async fn only_a_commit_that_adds_nothing_takes_the_union_again() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd')")
        .await?;
    let main = &lake.main;
    let read = main.table(&["db"], "t").await?;
    let file = lake.tasks().await?[0].file_id;
    let at = |position| {
        let mut deletes = Deletes::new();
        deletes.insert(file, position);
        deletes
    };
    main.transaction().delete(&read, &lake.storage, at(0)).commit().await?;

    // From the same read: the vector changed under it.
    main.transaction().delete(&read, &lake.storage, at(1)).commit().await?;
    assert_eq!(lake.rows("main.db.t").await?, [(3, 3), (4, 4)]);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![30])),
        Arc::new(arrow_array::Int32Array::from(vec![30])),
        Arc::new(arrow_array::StringArray::from(vec!["c"])),
    ];
    let row = RecordBatch::try_new(read.schema().clone(), columns)?;
    let mut writer = TableWriter::new(&lake.catalog, &read, lake.storage.clone(), WriteOptions::default())?;
    writer.write(&row).await?;
    let files = writer.finish().await?;
    let refused = main
        .transaction()
        .append(files)
        .delete(&read, &lake.storage, at(2))
        .commit()
        .await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.kind() == ErrorKind::Conflict && e.rule() == Some("DV-4")),
        "{refused:?}"
    );
    assert_eq!(lake.rows("main.db.t").await?, [(3, 3), (4, 4)]);
    Ok(())
}
