//! C9f (ADR 0003 §7): `merge into`. Each source row takes the first clause it meets, in order and by its predicate, and
//! all of it lands in one commit: what a read sees is worked out by hand here, row by row.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Storage};
use metalake_datafusion::MetaLake;

struct Lake {
    main: Ref,
    context: SessionContext,
}

async fn lake() -> Result<Lake, Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
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
    let (tx, _) = tx.create_table(table("changes"))?;
    tx.commit().await?;
    let context = MetaLake::new(catalog, Storage::in_memory()).await?.context()?;
    Ok(Lake { main, context })
}

impl Lake {
    async fn run(&self, sql: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches[0].column(0).as_primitive::<UInt64Type>().value(0))
    }

    async fn rows(&self, table: &str) -> Result<Vec<(i64, i32)>, Box<dyn std::error::Error>> {
        let sql = format!("select k, cast(coalesce(n, -1) as int) as n from {table} order by k, n");
        let mut rows = Vec::new();
        for b in self.context.sql(&sql).await?.collect().await? {
            let k = b.column(0).as_primitive::<Int64Type>();
            let n = b.column(1).as_primitive::<Int32Type>();
            rows.extend(k.values().iter().copied().zip(n.values().iter().copied()));
        }
        Ok(rows)
    }
}

#[tokio::test]
async fn merge_into_single_commit() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
        .await?;
    lake.run("insert into main.db.t values (4, 40, 'd')").await?;
    let before = lake.main.table(&["db"], "t").await?.snapshot_id().unwrap();
    lake.run("insert into main.db.changes values (1, 11, 'keep'), (2, 22, 'gone'), (5, 50, 'new'), (6, null, 'new')")
        .await?;

    // 1 is updated from both sides; 2 deleted, since that clause comes first; 3 and 4 are in no source row;
    // 5 and 6 are new, 6 with its n null and its s left out.
    let sql = "merge into main.db.t t using main.db.changes c on t.k = c.k
               when matched and c.s = 'gone' then delete
               when matched then update set n = t.n + c.n
               when not matched then insert (k, n) values (c.k, c.n)";
    assert_eq!(lake.run(sql).await?, 4);
    assert_eq!(
        lake.rows("main.db.t").await?,
        [(1, 21), (3, 30), (4, 40), (5, 50), (6, -1)]
    );
    let inserted = lake
        .context
        .sql("select s from main.db.t where k = 5")
        .await?
        .collect()
        .await?;
    assert!(inserted[0].column(0).is_null(0));

    // One commit: the sequence before it is as it was, and the one before the merge's is `before`.
    let latest = lake.main.table(&["db"], "t").await?.snapshot_id().unwrap();
    assert_ne!(latest, before);
    let past = format!(r#""main@{}".db.t"#, latest - 1);
    assert_eq!(lake.rows(&past).await?, [(1, 10), (2, 20), (3, 30), (4, 40)]);
    Ok(())
}

#[tokio::test]
async fn merge_into_clause_order() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b'), (3, 3, null)")
        .await?;

    // Both clauses match 1: the first takes it. A predicate null for 3 does not hold, so the next clause does.
    let sql = "merge into main.db.t t using (select k from main.db.t) c on t.k = c.k
               when matched and t.s = 'a' then update set n = 100
               when matched and t.k < 3 then update set n = 200
               when matched then update set n = 300";
    assert_eq!(lake.run(sql).await?, 3);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 100), (2, 200), (3, 300)]);

    // A row no clause takes is left alone, and counts for nothing.
    let sql = "merge into main.db.t t using main.db.t c on t.k = c.k when matched and t.n = 100 then delete";
    assert_eq!(lake.run(sql).await?, 1);
    assert_eq!(lake.rows("main.db.t").await?, [(2, 200), (3, 300)]);
    Ok(())
}

#[tokio::test]
async fn merge_into_noop_skips_commit() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a')").await?;
    lake.main.create_branch("dev").await?;
    let before = lake.main.table(&["db"], "t").await?.snapshot_id();

    // An empty source, and a source that matches no clause.
    let sql = "merge into main.db.t t using main.db.changes c on t.k = c.k when matched then delete";
    assert_eq!(lake.run(sql).await?, 0);
    let sql = "merge into main.db.t t using (select 9 as k) c on t.k = c.k when matched then delete";
    assert_eq!(lake.run(sql).await?, 0);
    assert_eq!(lake.main.table(&["db"], "t").await?.snapshot_id(), before);

    // From values, on a branch; no list of columns inserts them all.
    let sql = "merge into dev.db.t t using (select 1 as k, 5 as n, 'x' as s union all select 2, 6, 'y') c on t.k = c.k
               when matched then update set n = c.n
               when not matched then insert values (c.k, c.n, c.s)";
    assert_eq!(lake.run(sql).await?, 2);
    assert_eq!(lake.rows("dev.db.t").await?, [(1, 5), (2, 6)]);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 1)]);

    // A tag takes no merge.
    lake.main.create_tag("v1").await?;
    let sql = "merge into v1.db.t t using main.db.changes c on t.k = c.k when matched then delete";
    let refused = match lake.context.sql(sql).await {
        Ok(statement) => statement.collect().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(refused.is_err_and(|e| e.to_string().contains("tag")));
    Ok(())
}

#[tokio::test]
async fn merge_into_duplicate_match_refused() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b')")
        .await?;
    lake.run("insert into main.db.changes values (1, 10, 'x'), (1, 20, 'y')")
        .await?;
    let before = lake.main.table(&["db"], "t").await?.snapshot_id();
    for action in ["update set n = c.n", "delete"] {
        let sql = format!("merge into main.db.t t using main.db.changes c on t.k = c.k when matched then {action}");
        assert!(lake.context.sql(&sql).await?.collect().await.is_err(), "{action}");
    }
    assert_eq!(lake.rows("main.db.t").await?, [(1, 1), (2, 2)]);
    assert_eq!(lake.main.table(&["db"], "t").await?.snapshot_id(), before);

    // A clause that takes only one of them is no double match; two inserts of one key are the source's to say.
    let sql = "merge into main.db.t t using main.db.changes c on t.k = c.k
               when matched and c.s = 'x' then update set n = c.n";
    assert_eq!(lake.run(sql).await?, 1);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 10), (2, 2)]);
    let sql = "merge into main.db.changes t using (select 7 as k union all select 7) c on t.k = c.k
               when not matched then insert (k) values (c.k)";
    assert_eq!(lake.run(sql).await?, 2);
    Ok(())
}

#[tokio::test]
async fn merge_into_not_matched_by_source() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd')")
        .await?;
    lake.run("insert into main.db.changes values (1, 10, 'x')").await?;

    // Only the rows the predicate holds for, and matched rows go their own way.
    let sql = "merge into main.db.t t using main.db.changes c on t.k = c.k
               when matched then update set n = c.n
               when not matched by source and t.k = 2 then delete
               when not matched by source and t.k = 3 then update set n = 33";
    assert_eq!(lake.run(sql).await?, 3);
    assert_eq!(lake.rows("main.db.t").await?, [(1, 10), (3, 33), (4, 4)]);

    // An empty source: everything is missing from it, and all of it goes.
    lake.run("delete from main.db.changes").await?;
    let sql = "merge into main.db.t t using main.db.changes c on t.k = c.k when not matched by source then delete";
    assert_eq!(lake.run(sql).await?, 3);
    assert!(lake.rows("main.db.t").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_into_conflicts_with_earlier_commit() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.run("insert into main.db.t values (1, 1, 'a')").await?;
    lake.run("insert into main.db.t values (2, 2, 'b')").await?;
    let upsert = "merge into main.db.t t using (select 7 as k, 70 as n) c on t.k = c.k
                  when matched then update set n = c.n
                  when not matched then insert (k, n) values (c.k, c.n)";

    // Two merges adding one key: neither sees the other's row, and no vector tells them apart.
    let (first, second) = (lake.context.sql(upsert).await?, lake.context.sql(upsert).await?);
    first.collect().await?;
    assert!(second.collect().await.is_err());
    assert_eq!(lake.rows("main.db.t").await?, [(1, 1), (2, 2), (7, 70)]);

    // A delete of a row gets in before a merge updating it: the row does not come back.
    let update = "merge into main.db.t t using (select 1 as k) c on t.k = c.k when matched then update set n = 100";
    let merge = lake.context.sql(update).await?;
    lake.run("delete from main.db.t where k = 1").await?;
    assert!(merge.collect().await.is_err());
    assert_eq!(lake.rows("main.db.t").await?, [(2, 2), (7, 70)]);

    // Merges that only update other files both stand.
    let on = |k: i64, n: i32| {
        format!("merge into main.db.t t using (select {k} as k) c on t.k = c.k when matched then update set n = {n}")
    };
    let (first, second) = (lake.context.sql(&on(2, 22)).await?, lake.context.sql(&on(7, 77)).await?);
    first.collect().await?;
    second.collect().await?;
    assert_eq!(lake.rows("main.db.t").await?, [(2, 22), (7, 77)]);
    Ok(())
}
