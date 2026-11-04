//! C9fc (ADR 0003 §7): a merge reads the table with DataFusion's reader, so the source's keys spare it the files they
//! cannot be in. Its rows are named by position in that reader, so a vector must land on the right row: past row groups
//! it skipped, and past rows already deleted.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{Counting, fresh_schema, url};
use datafusion::prelude::SessionContext;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Storage, Table, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("n", DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(Int32Array::from_iter_values(keys.map(|k| k as i32))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

struct Lake {
    catalog: Catalog,
    main: Ref,
    storage: Storage,
    store: Arc<Counting>,
    context: SessionContext,
}

async fn lake() -> Result<Lake, Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let store = Arc::new(Counting::default());
    let storage = Storage::from_store("memory:///wh", store.clone())?;
    let shape = batch(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;
    let context = MetaLake::new(catalog.clone(), storage.clone()).await?.context()?;
    Ok(Lake {
        catalog,
        main,
        storage,
        store,
        context,
    })
}

impl Lake {
    async fn table(&self) -> Result<Table, metalake::Error> {
        self.main.table(&["db"], "t").await
    }

    async fn write(&self, keys: std::ops::Range<i64>, row_group_rows: usize) -> Result<(), metalake::Error> {
        let options = WriteOptions {
            row_group_rows,
            ..Default::default()
        };
        let t = self.table().await?;
        let mut writer = TableWriter::new(&self.catalog, &t, self.storage.clone(), options)?;
        writer.write(&batch(keys)).await?;
        self.main.transaction().append(writer.finish().await?).commit().await?;
        Ok(())
    }

    async fn run(&self, sql: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches[0].column(0).as_primitive::<UInt64Type>().value(0))
    }

    async fn rows(&self) -> Result<Vec<(i64, i32)>, Box<dyn std::error::Error>> {
        let batches = self
            .context
            .sql("select k, n from main.db.t order by k, n")
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for b in batches {
            let k = b.column(0).as_primitive::<Int64Type>();
            let n = b.column(1).as_primitive::<Int32Type>();
            rows.extend(k.values().iter().copied().zip(n.values().iter().copied()));
        }
        Ok(rows)
    }
}

#[tokio::test]
async fn a_small_merge_opens_few_files() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    for at in 0..20 {
        lake.write(at * 1000..at * 1000 + 100, 1024).await?;
    }
    let sql = "merge into main.db.t t using (select 7050 as k union all select 7051 union all select 7052) c
               on t.k = c.k when matched then update set n = -1";
    lake.store.take();
    assert_eq!(lake.run(sql).await?, 3);
    let requests = lake.store.take();
    println!("requests: {requests}");
    let changed = lake.rows().await?.into_iter().filter(|(_, n)| *n == -1).count();
    assert_eq!(changed, 3);
    assert_eq!(lake.rows().await?.len(), 2000);
    assert!(requests <= 4, "one file of twenty, and a few requests: {requests}");
    Ok(())
}

#[tokio::test]
async fn a_merge_marks_the_right_row() -> Result<(), Box<dyn std::error::Error>> {
    let lake = lake().await?;
    lake.write(0..100, 10).await?;

    // A row deleted before it, and one in a row group of its own that the reader skips: the position stays the file's.
    assert_eq!(lake.run("delete from main.db.t where k = 3").await?, 1);
    let sql = "merge into main.db.t t using (select 55 as k, 500 as n) c on t.k = c.k
               when matched then update set n = c.n";
    assert_eq!(lake.run(sql).await?, 1);
    let mut want: Vec<(i64, i32)> = (0..100).filter(|k| *k != 3).map(|k| (k, k as i32)).collect();
    want[54] = (55, 500);
    assert_eq!(lake.rows().await?, want);

    // A row deleted is not there to match: it is new.
    let sql = "merge into main.db.t t using (select 3 as k, 300 as n) c on t.k = c.k
               when matched then update set n = 0
               when not matched then insert (k, n) values (c.k, c.n)";
    assert_eq!(lake.run(sql).await?, 1);
    want.push((3, 300));
    want.sort();
    assert_eq!(lake.rows().await?, want);

    // A delete by merge, in the last row group, and a source row with nothing to match.
    let sql = "merge into main.db.t t using (select 99 as k union all select 1000) c on t.k = c.k
               when matched then delete";
    assert_eq!(lake.run(sql).await?, 1);
    want.pop();
    assert_eq!(lake.rows().await?, want);
    Ok(())
}
