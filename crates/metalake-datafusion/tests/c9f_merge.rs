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
async fn each_source_row_takes_its_first_clause_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
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
async fn the_order_of_the_clauses_decides() -> Result<(), Box<dyn std::error::Error>> {
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
async fn nothing_to_do_is_no_commit_and_a_branch_is_apart() -> Result<(), Box<dyn std::error::Error>> {
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
