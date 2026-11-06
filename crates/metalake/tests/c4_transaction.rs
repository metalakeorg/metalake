//! C4a (ADR 0003 §7): transactions. Two tables created from Arrow schemas in one commit and read back as the same
//! schemas; one commit writing both; an idempotent repeat that applies nothing; a write that expected a table
//! unchanged refused once it changed; a removal; and the definitions the core refuses before calling.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Outcome, Table};
use metalake_pg::records::FileAdd;
use metalake_pg::{ErrorKind, Pool, Protocol};
use serde_json::json;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("core_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

fn events() -> Schema {
    Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
    ])
}

/// A schema without its fields' metadata, to compare what was given with what is read back.
fn bare(schema: &Schema) -> Schema {
    fn strip(f: &Field) -> Field {
        let ty = match f.data_type() {
            DataType::Struct(fs) => DataType::Struct(fs.iter().map(|f| strip(f)).collect()),
            DataType::List(e) => DataType::List(Arc::new(strip(e))),
            other => other.clone(),
        };
        Field::new(f.name(), ty, f.is_nullable())
    }
    Schema::new(schema.fields().iter().map(|f| strip(f)).collect::<Vec<_>>())
}

/// A file of `table` whose keys run from `lo` to `hi`, as a writer would give it.
fn file(table: &Table, name: &str, lo: i64, hi: i64) -> FileAdd {
    FileAdd {
        table_id: table.id(),
        file_id: None,
        path: Some(format!("{}/data/{name}.parquet", table.location())),
        spec_id: table.spec_id().unwrap_or(0),
        partition: json!([]),
        sort_order_id: None,
        record_count: 10,
        file_size: 1000,
        footer_size: 100,
        first_row_id: None,
        stat_field_ids: vec![1],
        lower: vec![Some(metalake::bounds::long(lo))],
        upper: vec![Some(metalake::bounds::long(hi))],
        null_count: vec![Some(0)],
        nan_count: vec![None],
        value_count: None,
        content: None,
        footer: None,
    }
}

#[tokio::test]
async fn transaction_create_write_and_refusals() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;

    // Two tables and their namespace, in one commit.
    let (tx, events_id) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "events",
        schema: &events(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![("owner", "a")],
    })?;
    let keys = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let (tx, _) = tx.create_table(NewTable {
        namespace: &["db"],
        name: "keys",
        schema: &keys,
        location: Some("memory:///elsewhere/keys".into()),
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    assert!(matches!(tx.commit().await?, Outcome::Committed(_)));

    let events_table = main.table(&["db"], "events").await?;
    assert_eq!(events_table.id(), events_id);
    // A timestamptz is an instant: given in any zone, it is read back in UTC, as the specification stores it.
    let mut expected: Vec<Field> = bare(&events()).fields().iter().map(|f| f.as_ref().clone()).collect();
    expected[1] = Field::new(
        "at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        true,
    );
    assert_eq!(bare(events_table.schema()), Schema::new(expected));
    assert!(
        events_table.location().starts_with("memory:///wh/db/events-"),
        "{}",
        events_table.location()
    );
    assert_eq!(
        (events_table.cluster_key(), events_table.properties()["owner"].as_str()),
        (&[1][..], "a")
    );
    let keys_table = main.table(&["db"], "keys").await?;
    assert_eq!(keys_table.location(), "memory:///elsewhere/keys");

    // One commit writes both tables: both see it at the same sequence.
    let both = main
        .transaction()
        .append([file(&events_table, "a", 0, 9), file(&keys_table, "a", 0, 9)])
        .author("c4")
        .commit()
        .await?;
    let (events_table, keys_table) = (main.table(&["db"], "events").await?, main.table(&["db"], "keys").await?);
    assert_eq!(events_table.snapshot_id(), Some(both.seq()));
    assert_eq!(keys_table.snapshot_id(), Some(both.seq()));

    // An idempotent commit, repeated, applies nothing the second time.
    let once = || {
        main.transaction()
            .append([file(&events_table, "b", 10, 19)])
            .idempotent("ingest", 7)
    };
    let first = once().commit().await?;
    assert!(matches!(first, Outcome::Committed(_)));
    assert_eq!(once().commit().await?, Outcome::AlreadyApplied(first.seq()));

    // A write that expected the table as it read it is refused once another commit changed it.
    let stale = main
        .transaction()
        .append([file(&events_table, "c", 0, 1)])
        .expect_unchanged(&events_table);
    let refused = stale.commit().await.unwrap_err();
    match &refused {
        Error::Protocol(e) => {
            assert_eq!(e.kind(), ErrorKind::Conflict, "{e}");
            assert!(e.rule().is_some_and(|r| r.starts_with("CMT-")), "{e}");
        }
        other => panic!("{other}"),
    }

    // A removal: the file is gone from the next read.
    let pool = Pool::new(&url(), 1)?;
    let p = Protocol::new(&schema);
    let s = pool.get().await?;
    let planned = p
        .plan(&s, 1, keys_table.id(), None, None)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(planned.len(), 1);
    main.transaction()
        .remove(&keys_table, [planned[0].file_id])
        .commit()
        .await?;
    assert!(
        p.plan(&s, 1, keys_table.id(), None, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );

    // Definitions the core refuses before it calls.
    let no_column = main.transaction().create_table(NewTable {
        namespace: &["db"],
        name: "bad",
        schema: &keys,
        location: None,
        cluster_key: vec!["nope"],
        partitioning: vec![],
        properties: vec![],
    });
    assert!(matches!(no_column, Err(Error::Schema(_))));
    Ok(())
}

/// A table of `name`, with no location, created through `catalog`, and where it went.
async fn located(catalog: &Catalog, name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let main = catalog.branch("main").await?;
    let schema = bare(&events());
    let (tx, _) = main.transaction().create_table(NewTable {
        namespace: &["db"],
        name,
        schema: &schema,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    Ok(main.table(&["db"], name).await?.location().to_string())
}

#[tokio::test]
async fn catalog_warehouse_default() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let created = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh/"), None).await?;
    let (tx, _) = created
        .branch("main")
        .await?
        .transaction()
        .create_namespace(&["db"])
        .create_table(NewTable {
            namespace: &["db"],
            name: "first",
            schema: &bare(&events()),
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![],
        })?;
    tx.commit().await?;

    // A connection that names none takes the catalog's.
    let plain = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    assert_eq!(plain.warehouse(), Some("memory:///wh/"));
    assert!(located(&plain, "plain").await?.starts_with("memory:///wh/db/plain-"));

    // One that names its own uses it, and changes nothing for the others.
    let elsewhere = Catalog::connect(CatalogConfig::new(url(), &schema).warehouse("memory:///other")).await?;
    assert!(
        located(&elsewhere, "moved")
            .await?
            .starts_with("memory:///other/db/moved-")
    );
    let again = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    assert_eq!(again.warehouse(), Some("memory:///wh/"));

    // A catalog with none asks for a location, as it always did.
    let schema = fresh_schema();
    let none = Catalog::init(CatalogConfig::new(url(), &schema), None).await?;
    assert_eq!(none.warehouse(), None);
    let no_location = none.branch("main").await?.transaction().create_table(NewTable {
        namespace: &["db"],
        name: "bad",
        schema: &bare(&events()),
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    });
    assert!(matches!(no_location, Err(Error::Location(_))));
    Ok(())
}
