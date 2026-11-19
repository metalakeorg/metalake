//! C9j: `read_orc('path')` in SQL. A file Hive wrote (ORC Java 1.8.5, ZLIB, `marts.top_clientes` of the demo), in a
//! directory of the lake's store beside Hive's `_SUCCESS`, is read and brought into a MetaLake table by
//! `CREATE TABLE … AS SELECT`. A transactional table's directory and a partitioned one are refused, saying why.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use bytes::Bytes;
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use metalake::{Catalog, CatalogConfig, Storage};
use metalake_datafusion::MetaLake;

/// `marts.top_clientes` as Hive wrote it: the top 100 customers, `cliente_id, nombre, pedidos, importe, puesto`.
const HIVE_ORC: &[u8] = include_bytes!("data/hive_top_clientes.orc");

struct Lake {
    engine: MetaLake,
    context: SessionContext,
}

impl Lake {
    async fn run(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let mut statements = DFParser::parse_sql(sql)?;
        let statement = statements.pop_front().expect("a statement");
        self.engine.execute(&self.context, statement).await?.collect().await
    }

    async fn count(&self, sql: &str) -> i64 {
        let batches = self.run(sql).await.unwrap();
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    }
}

#[tokio::test]
async fn c9j_read_orc_into_a_table() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::in_memory();
    storage
        .put("memory:///hive/top_clientes/000000_0", Bytes::from_static(HIVE_ORC))
        .await?;
    storage
        .put("memory:///hive/top_clientes/_SUCCESS", Bytes::new())
        .await?;
    storage
        .put(
            "memory:///hive/pedidos/delta_0000001_0000001_0000/bucket_00000",
            Bytes::from_static(HIVE_ORC),
        )
        .await?;
    storage
        .put("memory:///hive/part/dt=1/000000_0", Bytes::from_static(HIVE_ORC))
        .await?;
    let engine = MetaLake::new(catalog, storage).await?;
    let context = engine.context()?;
    let lake = Lake { engine, context };

    // Read as it is: Hive's columns by their names, `_SUCCESS` left out.
    assert_eq!(
        lake.count("select count(*) from read_orc('/hive/top_clientes')").await,
        100
    );
    assert_eq!(
        lake.count("select min(puesto) + max(puesto) * 1000 from read_orc('/hive/top_clientes')")
            .await,
        100_001
    );

    // Brought in: a MetaLake table, Parquet in the warehouse, the same rows.
    lake.run("create schema db").await?;
    lake.run("create table db.top as select * from read_orc('/hive/top_clientes')")
        .await?;
    assert_eq!(lake.count("select count(*) from db.top").await, 100);
    assert_eq!(
        lake.count(
            "select count(*) from db.top t join read_orc('/hive/top_clientes') o on t.cliente_id = o.cliente_id"
        )
        .await,
        100
    );

    // A transactional table, and a partitioned directory, refused saying why.
    let acid = lake
        .run("select * from read_orc('/hive/pedidos')")
        .await
        .unwrap_err()
        .to_string();
    assert!(acid.contains("transactional"), "{acid}");
    let parted = lake
        .run("select * from read_orc('/hive/part')")
        .await
        .unwrap_err()
        .to_string();
    assert!(parted.contains("directories"), "{parted}");
    let none = lake
        .run("select * from read_orc('/nothing')")
        .await
        .unwrap_err()
        .to_string();
    assert!(none.contains("no file"), "{none}");
    Ok(())
}
