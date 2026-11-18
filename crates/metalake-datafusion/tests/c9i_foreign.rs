//! C9i (ENG-3, ENG-4): DataFusion reads files another system wrote as the core does. Two Parquet files written as Hive
//! writes them, with no field ids, `value` for the table's `v` and the partition column `dt` only in their
//! directories, are queried with SQL: every column, a range on a column found by name, and a filter on the partition
//! column, which each file's statistics hold as a constant and so prune.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::{fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use metalake::mapping::PROPERTY;
use metalake::stats::{self, FileFacts};
use metalake::{Catalog, CatalogConfig, NewTable, Storage};
use metalake_datafusion::MetaLake;
use object_store::memory::InMemory;
use serde_json::json;

fn hive_file(keys: &[i64], values: &[&str]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(StringArray::from(values.to_vec())),
    ];
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), None).unwrap();
    writer.write(&RecordBatch::try_new(schema, columns).unwrap()).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

#[tokio::test]
async fn c9i_hive_files_with_sql() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let shape = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("dt", DataType::Date32, true),
    ]);
    let mapping = r#"[{"field-id": 1, "names": ["k"]}, {"field-id": 2, "names": ["v", "value"]},
                      {"field-id": 3, "names": ["dt"]}]"#;
    let (tx, _) = main
        .transaction()
        .create_namespace(&["default", "db"])
        .create_table(NewTable {
            namespace: &["default", "db"],
            name: "ventas",
            schema: &shape,
            location: None,
            cluster_key: vec![],
            partitioning: vec![("dt", "identity")],
            properties: vec![(PROPERTY, mapping), ("inline.max-bytes", "0")],
        })?;
    tx.commit().await?;
    let table = main.table(&["default", "db"], "ventas").await?;

    let mut adds = Vec::new();
    for (day, keys, values) in [
        ("2024-01-02", [1, 2, 3], ["a", "b", "c"]),
        ("2024-01-03", [10, 11, 12], ["d", "e", "f"]),
    ] {
        let bytes = hive_file(&keys, &values);
        let path = format!("memory:///wh/hive/ventas/dt={day}/part-0.parquet");
        storage.put(&path, bytes.clone()).await?;
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
        adds.push(stats::of_footer(&table, &metadata, &HashMap::new()).into_file_add(
            table.id(),
            FileFacts {
                path: Some(path),
                content: None,
                file_size: bytes.len() as i64,
                footer_size: stats::footer_size(&bytes).unwrap(),
                footer: None,
                spec_id: table.spec_id().unwrap_or(0),
                partition: json!([day]),
                sort_order_id: None,
            },
        ));
    }
    main.transaction().append(adds).commit().await?;

    let context = MetaLake::new(catalog, storage).await?.context()?;
    let answer = async |sql: &str| -> Result<String, Box<dyn std::error::Error>> {
        Ok(pretty_format_batches(&context.sql(sql).await?.collect().await?)?.to_string())
    };
    let every = answer("select k, v, dt from db.ventas order by k").await?;
    let expected = [
        "+----+---+------------+",
        "| k  | v | dt         |",
        "+----+---+------------+",
        "| 1  | a | 2024-01-02 |",
        "| 2  | b | 2024-01-02 |",
        "| 3  | c | 2024-01-02 |",
        "| 10 | d | 2024-01-03 |",
        "| 11 | e | 2024-01-03 |",
        "| 12 | f | 2024-01-03 |",
        "+----+---+------------+",
    ];
    assert_eq!(every, expected.join("\n"));
    assert_eq!(
        answer("select v from db.ventas where k between 10 and 11 order by v").await?,
        ["+---+", "| v |", "+---+", "| d |", "| e |", "+---+"].join("\n")
    );
    assert_eq!(
        answer("select count(*) as n from db.ventas where dt = date '2024-01-03'").await?,
        ["+---+", "| n |", "+---+", "| 3 |", "+---+"].join("\n")
    );
    Ok(())
}
