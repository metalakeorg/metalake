//! C16 (ENG-3, ENG-4): a file another system wrote, with no field ids, its partition column only in its directory, as
//! Hive writes them, reads in place. Its statistics are found through the table's name mapping, by a name or an alias
//! of it; a scan reads its columns by name, its partition column from the catalog's partition value, and a row
//! group range on a column it found by name.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int64Type};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::mapping::{NameMapping, PROPERTY};
use metalake::partition::Scalar;
use metalake::stats::{self, FileFacts};
use metalake::{Catalog, CatalogConfig, NewTable, Scan, Storage};
use object_store::memory::InMemory;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaDataReader;
use serde_json::json;

/// A Parquet file as Hive writes one: plain column names, no field ids, no partition column.
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
async fn c16_hive_file_reads_in_place() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;

    // The table: `v`, which Hive's files call `value`, and `dt`, partitioned by identity, in no file.
    let shape = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("dt", DataType::Date32, true),
    ]);
    let mapping = r#"[{"field-id": 1, "names": ["k"]}, {"field-id": 2, "names": ["v", "value"]},
                      {"field-id": 3, "names": ["dt"]}]"#;
    NameMapping::parse(mapping)?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "ventas",
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![("dt", "identity")],
        properties: vec![(PROPERTY, mapping), ("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;
    let table = main.table(&["db"], "ventas").await?;

    // Two files of two days, where Hive left them.
    let mut adds = Vec::new();
    for (day, keys, values) in [
        ("2024-01-02", [1, 2, 3], ["a", "b", "c"]),
        ("2024-01-03", [10, 11, 12], ["d", "e", "f"]),
    ] {
        let bytes = hive_file(&keys, &values);
        let path = format!("memory:///wh/hive/ventas/dt={day}/part-0.parquet");
        storage.put(&path, bytes.clone()).await?;
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
        let statistics = stats::of_footer(&table, &metadata, &HashMap::new());
        assert_eq!(
            statistics.field_ids,
            [1, 2],
            "k by its name, v by its alias `value`: {statistics:?}"
        );
        adds.push(statistics.into_file_add(
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
    let table = main.table(&["db"], "ventas").await?;

    // Every column: k and v by name, dt from each file's partition.
    let scan = Scan::new(&catalog, &table);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, &storage).try_collect().await?;
    let mut rows: Vec<(i64, String, i32)> = batches
        .iter()
        .flat_map(|b| {
            let k = b.column(0).as_primitive::<Int64Type>().clone();
            let v = b.column(1).as_string::<i32>().clone();
            let dt = b.column(2).as_primitive::<Date32Type>().clone();
            (0..b.num_rows())
                .map(move |i| (k.value(i), v.value(i).to_string(), dt.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    let (jan2, jan3) = (19724, 19725);
    assert_eq!(
        rows,
        [
            (1, "a".into(), jan2),
            (2, "b".into(), jan2),
            (3, "c".into(), jan2),
            (10, "d".into(), jan3),
            (11, "e".into(), jan3),
            (12, "f".into(), jan3)
        ]
    );

    // A range on k: the catalog prunes the other day's file by the bounds found through the mapping.
    let narrow = Scan::new(&catalog, &table).filter("k", Some(Scalar::Long(10)), Some(Scalar::Long(11)))?;
    let planned: Vec<_> = narrow.plan().await?.try_collect().await?;
    assert_eq!(planned.len(), 1, "one file holds k in [10, 11]");
    Ok(())
}

/// OWN-2 through the core: a table adopts a prefix in the commit that creates it, and another later.
#[tokio::test]
async fn c16_own_prefixes() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(
        CatalogConfig::new(url(), schema.clone()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let shape = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
    let (tx, id) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.own(id, &["memory:///wh/hive/t/"]).commit().await?;
    main.transaction().own(id, &["memory:///wh/old"]).commit().await?;

    let pool = metalake_pg::Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let owned = metalake_pg::Protocol::new(&schema).owned_prefixes(&session, id).await?;
    assert_eq!(
        owned,
        ["memory:///wh/hive/t", "memory:///wh/old"],
        "a trailing / is the same prefix"
    );
    Ok(())
}
