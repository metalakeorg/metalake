//! C17: registering files in place. A Hive table's directory (Parquet with no field ids, `dt=` directories, `_SUCCESS`
//! and a `.crc`) becomes a table with one run: created with its name mapping, its files' rows and statistics, read
//! back with the partition column. A check commits nothing; a refused file stops the run before any commit; a second
//! run adds only what is new; adopting records the source as owned, and strays then lists it (RET-7b).

mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int64Type};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::register::{BATCH, Register, register};
use metalake::{Catalog, CatalogConfig, Scan, Storage};
use object_store::memory::InMemory;
use parquet::arrow::ArrowWriter;
use serde_json::json;

fn hive_file(keys: &[i64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    let values: Vec<String> = keys.iter().map(|k| format!("v{k}")).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(StringArray::from(values)),
    ];
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), None).unwrap();
    writer.write(&RecordBatch::try_new(schema, columns).unwrap()).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

const SOURCE: &str = "memory:///wh/hive/ventas";

fn request<'a>(check: bool, adopt: bool) -> Register<'a> {
    Register {
        source: Some(SOURCE),
        namespace: &["db"],
        name: "ventas",
        partition: vec![("dt", "date")],
        adopt,
        location: None,
        check,
        batch: BATCH,
    }
}

async fn rows(catalog: &Catalog, storage: &Storage) -> Result<Vec<(i64, i32)>, Box<dyn std::error::Error>> {
    let table = catalog.branch("main").await?.table(&["db"], "ventas").await?;
    let scan = Scan::new(catalog, &table).columns(["k", "dt"])?;
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut rows: Vec<(i64, i32)> = batches
        .iter()
        .flat_map(|b| {
            let (k, dt) = (
                b.column(0).as_primitive::<Int64Type>(),
                b.column(1).as_primitive::<Date32Type>(),
            );
            (0..b.num_rows()).map(|i| (k.value(i), dt.value(i))).collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn c17_register_a_hive_directory() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(
        CatalogConfig::new(url(), schema.clone()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    for (path, bytes) in [
        ("dt=2024-01-02/part-0.parquet", hive_file(&[1, 2])),
        ("dt=2024-01-03/part-0.parquet", hive_file(&[3])),
        ("_SUCCESS", Bytes::new()),
        ("dt=2024-01-02/.part-0.parquet.crc", Bytes::from_static(b"crc")),
    ] {
        storage.put(&format!("{SOURCE}/{path}"), bytes).await?;
    }

    // A check reports and commits nothing.
    let checked = register(&main, &storage, &request(true, false)).await?;
    assert_eq!(
        (checked.registered, checked.skipped_hidden, checked.rows),
        (2, 2, 3),
        "{checked:?}"
    );
    assert!(checked.commits.is_empty() && checked.refused.is_empty(), "{checked:?}");
    assert!(main.table(&["db"], "ventas").await.is_err(), "no table yet");

    // A file that is not Parquet stops the run before any commit.
    storage
        .put(
            &format!("{SOURCE}/dt=2024-01-04/notes.txt"),
            Bytes::from_static(b"not parquet"),
        )
        .await?;
    let refused = register(&main, &storage, &request(false, false)).await?;
    assert_eq!(refused.refused.len(), 1, "{refused:?}");
    assert_eq!(refused.refused[0].rule, "parquet");
    assert!(refused.commits.is_empty() && main.table(&["db"], "ventas").await.is_err());
    storage.delete(&format!("{SOURCE}/dt=2024-01-04/notes.txt")).await?;

    // The run: created, registered, read back with dt from the directories.
    let first = register(&main, &storage, &request(false, false)).await?;
    assert!(
        first.created && first.registered == 2 && first.commits.len() == 2,
        "{first:?}"
    );
    let (jan2, jan3, jan5) = (19724, 19725, 19727);
    assert_eq!(rows(&catalog, &storage).await?, [(1, jan2), (2, jan2), (3, jan3)]);
    let table = main.table(&["db"], "ventas").await?;
    assert!(table.properties().contains_key(metalake::mapping::PROPERTY));
    let planned: Vec<_> = Scan::new(&catalog, &table).plan().await?.try_collect().await?;
    assert!(
        planned
            .iter()
            .all(|t| t.path.as_deref().is_some_and(|p| p.starts_with(SOURCE))),
        "in place"
    );

    // Again, with one file more and adopting: only the new one, and the source is owned.
    storage
        .put(&format!("{SOURCE}/dt=2024-01-05/part-0.parquet"), hive_file(&[4]))
        .await?;
    let second = register(&main, &storage, &request(false, true)).await?;
    assert!(
        !second.created && second.registered == 1 && second.already == 2,
        "{second:?}"
    );
    assert_eq!(second.adopted.as_deref(), Some(SOURCE));
    assert_eq!(
        rows(&catalog, &storage).await?,
        [(1, jan2), (2, jan2), (3, jan3), (4, jan5)]
    );
    let pool = metalake_pg::Pool::new(&url(), 1)?;
    let owned = metalake_pg::Protocol::new(&schema)
        .owned_prefixes(&pool.get().await?, table.id())
        .await?;
    assert_eq!(owned, [SOURCE]);

    // Owned now, the source is listed by strays: what is there and was never registered, Hive's `_SUCCESS` and `.crc`
    // and a file it left, becomes expired intents for orphans to delete; the registered files do not.
    storage
        .put(&format!("{SOURCE}/dt=2024-01-02/old.orc"), Bytes::from_static(b"orc"))
        .await?;
    let table = main.table(&["db"], "ventas").await?;
    let strays = catalog
        .maintenance()
        .strays(&table, &storage, std::time::Duration::ZERO)
        .await?;
    assert_eq!(strays, 3);
    assert_eq!(rows(&catalog, &storage).await?.len(), 4);

    // A source on another store is refused, naming both.
    let elsewhere = Register {
        source: Some("s3://other/hive"),
        ..request(true, false)
    };
    let refused = register(&main, &storage, &elsewhere).await.unwrap_err().to_string();
    assert!(
        refused.contains("s3://other/hive") && refused.contains("memory://"),
        "{refused}"
    );
    Ok(())
}

/// A table registered as a user says it: a path in the store, its partition found in its directories, and run again
/// with no source, from where it was registered.
#[tokio::test]
async fn c17_register_discovers_and_remembers() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let source = "memory:///wh/raw/pedidos";
    for path in ["year=2024/month=1/part-0.parquet", "year=2024/month=12/part-0.parquet"] {
        storage.put(&format!("{source}/{path}"), hive_file(&[1])).await?;
    }
    let found = |source| Register {
        source,
        namespace: &["ventas"],
        name: "pedidos",
        partition: vec![],
        adopt: false,
        location: None,
        check: false,
        batch: BATCH,
    };

    // A file laid out otherwise than the first is refused, saying how.
    storage
        .put(&format!("{source}/region=EU/part-0.parquet"), hive_file(&[9]))
        .await?;
    let refused = register(&main, &storage, &found(Some("/wh/raw/pedidos"))).await?;
    assert_eq!(refused.refused.len(), 1, "{refused:?}");
    assert_eq!(refused.refused[0].rule, "partition");
    storage.delete(&format!("{source}/region=EU/part-0.parquet")).await?;

    // A path in the store, no partition said: both found.
    let first = register(&main, &storage, &found(Some("/wh/raw/pedidos/"))).await?;
    assert_eq!(first.source, source);
    assert_eq!(
        first.partition,
        [
            ("year".to_string(), "int".to_string()),
            ("month".to_string(), "int".to_string())
        ]
    );
    assert!(first.created && first.registered == 2, "{first:?}");
    let table = main.table(&["ventas"], "pedidos").await?;
    assert_eq!(
        table
            .properties()
            .get(metalake::register::SOURCE_PROPERTY)
            .map(String::as_str),
        Some(source)
    );

    // Again with no source: from the one it keeps, its partition its own.
    storage
        .put(&format!("{source}/year=2025/month=1/part-0.parquet"), hive_file(&[2]))
        .await?;
    let again = register(&main, &storage, &found(None)).await?;
    assert!(
        !again.created && again.registered == 1 && again.already == 2,
        "{again:?}"
    );
    assert_eq!(again.partition, first.partition);

    // A declared partition the table does not have is refused; nothing to register from, a new table says so.
    let other = Register {
        partition: vec![("year", "string")],
        ..found(None)
    };
    assert!(register(&main, &storage, &other).await.is_err());
    let unknown = Register {
        name: "nada",
        ..found(None)
    };
    let said = register(&main, &storage, &unknown).await.unwrap_err().to_string();
    assert!(said.contains("does not exist"), "{said}");

    // A table that keeps no source, as one registered by 0.9.0: run with one and nothing new, it keeps it by a commit
    // of its own, and from then on needs none.
    let table = main.table(&["ventas"], "pedidos").await?;
    let forgotten = json!({"properties": {metalake::register::SOURCE_PROPERTY: null}});
    main.transaction().define(table.id(), forgotten).commit().await?;
    let table = main.table(&["ventas"], "pedidos").await?;
    assert!(!table.properties().contains_key(metalake::register::SOURCE_PROPERTY));
    assert!(
        register(&main, &storage, &found(None)).await.is_err(),
        "no source kept, none given"
    );
    let kept = register(&main, &storage, &found(Some("/wh/raw/pedidos"))).await?;
    assert!(kept.registered == 0 && kept.commits.len() == 1, "{kept:?}");
    let table = main.table(&["ventas"], "pedidos").await?;
    assert_eq!(
        table
            .properties()
            .get(metalake::register::SOURCE_PROPERTY)
            .map(String::as_str),
        Some(source)
    );
    assert!(
        table.properties().contains_key(metalake::mapping::PROPERTY),
        "the other properties stay"
    );
    let again = register(&main, &storage, &found(None)).await?;
    assert!(again.registered == 0 && again.commits.is_empty(), "{again:?}");
    Ok(())
}

/// A Hive database's directory registered as a schema: each prefix under it a table, by its name, its partition
/// found; one that refuses leaves the others registered; run again, a table new under it is registered and the
/// others take what is new. A prefix that is a table, partitioned or with files of its own, is refused.
#[tokio::test]
async fn c17_register_a_schema() -> Result<(), Box<dyn std::error::Error>> {
    use metalake::register::{RegisterSchema, register_schema};
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let db = "memory:///wh/hive/curated.db";
    for (path, bytes) in [
        ("ventas/dt=2024-01-01/000000_0", hive_file(&[1, 2])),
        ("ventas/dt=2024-01-02/000000_0", hive_file(&[3])),
        ("clientes/000000_0", hive_file(&[7])),
        ("raros/000000_0", Bytes::from_static(b"ORC, not Parquet")),
        ("otra.db/t/000000_0", hive_file(&[9])),
        ("_tmp/x", Bytes::from_static(b"hidden")),
    ] {
        storage.put(&format!("{db}/{path}"), bytes).await?;
    }
    let schema = |source| RegisterSchema {
        source,
        namespace: &["curated"],
        adopt: false,
        check: false,
        batch: BATCH,
    };

    let first = register_schema(&main, &storage, &schema("/wh/hive/curated.db")).await?;
    let names: Vec<&str> = first.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["clientes", "otra.db", "raros", "ventas"]);
    let table = |name: &str| first.tables.iter().find(|t| t.name == name).unwrap();
    let ventas = table("ventas").report.as_ref().unwrap();
    assert!(ventas.created && ventas.registered == 2, "{ventas:?}");
    assert_eq!(ventas.partition, [("dt".to_string(), "date".to_string())]);
    assert_eq!(table("clientes").report.as_ref().unwrap().registered, 1);
    assert_eq!(table("raros").report.as_ref().unwrap().refused[0].rule, "parquet");
    assert!(table("otra.db").error.as_deref().unwrap().contains("schema of its own"));
    assert!(main.table(&["curated"], "ventas").await.is_ok());
    assert!(
        main.table(&["curated"], "raros").await.is_err(),
        "a refused table commits nothing"
    );

    // Again: a table new under the prefix is registered, the others take what is new.
    storage
        .put(&format!("{db}/productos/000000_0"), hive_file(&[5]))
        .await?;
    storage
        .put(&format!("{db}/ventas/dt=2024-01-03/000000_0"), hive_file(&[4]))
        .await?;
    let again = register_schema(&main, &storage, &schema(db)).await?;
    let table = |name: &str| {
        again
            .tables
            .iter()
            .find(|t| t.name == name)
            .unwrap()
            .report
            .clone()
            .unwrap()
    };
    assert!(table("productos").created && table("productos").registered == 1);
    assert!(!table("ventas").created && table("ventas").registered == 1 && table("ventas").already == 2);
    assert_eq!(table("clientes").registered, 0);

    // A table is not a schema.
    let parted = register_schema(&main, &storage, &schema("/wh/hive/curated.db/ventas")).await;
    assert!(parted.unwrap_err().to_string().contains("partitioned"));
    let own = register_schema(&main, &storage, &schema("/wh/hive/curated.db/clientes")).await;
    assert!(own.unwrap_err().to_string().contains("files of its own"));
    Ok(())
}
