//! C9a (ADR 0003 §7): DataFusion sees the lake. Refs are catalogs and namespaces schemas; a table is found at once and
//! listed from a picture. `count(*)`, `min` and `max` are answered from what the catalog keeps, by field id, with no
//! request to storage and no file in the plan; and where a statistic would not hold of the rows a read sees, a deleted
//! bound, a NaN, a column the file lacks or holds in an earlier type, the plan reads instead of answering wrongly.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{Counting, fresh_schema, url};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, Storage, Table, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;
use metalake_pg::{Commit, Pool, Protocol};
use serde_json::{Value, json};

fn shape() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
        Field::new("x", DataType::Float64, true),
        Field::new("n", DataType::Int32, true),
    ]))
}

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.clone().map(|k| format!("s{k:03}")))),
        Arc::new(Float64Array::from_iter_values(keys.clone().map(|k| k as f64))),
        Arc::new(Int32Array::from_iter_values(keys.map(|k| k as i32))),
    ];
    RecordBatch::try_new(shape(), columns).unwrap()
}

fn new_table<'a>(
    namespace: &'a [&'a str],
    name: &'a str,
    schema: &'a Schema,
    properties: Vec<(&'a str, &'a str)>,
) -> NewTable<'a> {
    NewTable {
        namespace,
        name,
        schema,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties,
    }
}

async fn lake(store: Arc<Counting>) -> Result<(Catalog, Ref, Storage, String), Error> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    Ok((catalog, main, Storage::from_store("memory:///wh", store)?, schema))
}

async fn append(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    t: &Table,
    keys: std::ops::Range<i64>,
) -> Result<(), Error> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    on.transaction().append(writer.finish().await?).commit().await?;
    Ok(())
}

/// Whether the plan of `sql` opens the table, or answers without it.
async fn reads(context: &SessionContext, sql: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let plan = context.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref())
        .indent(false)
        .to_string()
        .contains("metalake="))
}

async fn one(context: &SessionContext, sql: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let batches = context.sql(sql).await?.collect().await?;
    assert_eq!(batches.len(), 1, "{sql}");
    Ok(batches.into_iter().next().unwrap())
}

async fn long(context: &SessionContext, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(one(context, sql).await?.column(0).as_primitive::<Int64Type>().value(0))
}

async fn names(context: &SessionContext) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql = "select table_catalog, table_schema, table_name from information_schema.tables
               where table_schema <> 'information_schema' order by 1, 2, 3";
    let mut names = Vec::new();
    for b in context.sql(sql).await?.collect().await? {
        let column = |at: usize, row: usize| text(&b.column(at).slice(row, 1));
        names.extend((0..b.num_rows()).map(|i| format!("{}.{}.{}", column(0, i), column(1, i), column(2, i))));
    }
    Ok(names)
}

#[tokio::test]
async fn ref_catalogs_and_table_resolution() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, _) = lake(Arc::default()).await?;
    let shape = shape();
    let (tx, _) = main
        .transaction()
        .create_namespace(&["db"])
        .create_table(new_table(&["db"], "t", &shape, vec![]))?;
    tx.commit().await?;
    let (tx, _) = main
        .transaction()
        .create_namespace(&["a"])
        .create_namespace(&["a", "b"])
        .create_table(new_table(&["a", "b"], "u", &shape, vec![]))?;
    tx.commit().await?;
    main.create_branch("dev").await?;

    let lake = MetaLake::new(catalog, storage).await?;
    let context = lake.context()?;
    assert_eq!(
        names(&context).await?,
        ["dev.a.b.u", "dev.db.t", "main.a.b.u", "main.db.t"]
    );
    let t = context.table("main.db.t").await?;
    let fields: Vec<&str> = t.schema().fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(fields, ["k", "s", "x", "n"]);
    assert_eq!(long(&context, r#"select count(*) from main."a.b".u"#).await?, 0);
    assert_eq!(long(&context, "select count(*) from dev.db.t").await?, 0);

    // A table created after the picture is found by name at once, and listed after a refresh.
    let (tx, _) = main
        .transaction()
        .create_table(new_table(&["db"], "later", &shape, vec![]))?;
    tx.commit().await?;
    assert_eq!(long(&context, "select count(*) from main.db.later").await?, 0);
    assert_eq!(names(&context).await?.len(), 4);
    lake.refresh().await?;
    assert_eq!(names(&context).await?.len(), 5);

    // What does not exist is an error of the query.
    assert!(context.sql("select * from main.db.nothing").await.is_err());
    assert!(context.sql("select * from nothing.db.t").await.is_err());
    Ok(())
}

#[tokio::test]
async fn aggregates_from_statistics() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, main, storage, _) = lake(store.clone()).await?;
    let shape = shape();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        &["db"],
        "t",
        &shape,
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    append(&catalog, &main, &storage, &t, 0..10).await?;
    append(&catalog, &main, &storage, &t, 10..25).await?;
    let (tx, _) = main
        .transaction()
        .create_table(new_table(&["db"], "small", &shape, vec![]))?;
    tx.commit().await?;
    let small = main.table(&["db"], "small").await?;
    append(&catalog, &main, &storage, &small, 5..8).await?;
    let (tx, _) = main
        .transaction()
        .create_table(new_table(&["db"], "none", &shape, vec![]))?;
    tx.commit().await?;

    let context = MetaLake::new(catalog, storage).await?.context()?;
    store.take();
    for sql in [
        "select count(*), min(k), max(k), min(s), max(s) from main.db.t",
        "select count(*), min(k), max(k), min(s), max(s) from main.db.small",
        "select count(*) from main.db.none",
    ] {
        let plan = context.sql(sql).await?.create_physical_plan().await?;
        assert!(
            !reads(&context, sql).await?,
            "{sql}
{}",
            displayable(plan.as_ref()).indent(true)
        );
    }
    let row = one(
        &context,
        "select count(*), min(k), max(k), min(s), max(s) from main.db.t",
    )
    .await?;
    assert_eq!(row.column(0).as_primitive::<Int64Type>().value(0), 25);
    assert_eq!(row.column(1).as_primitive::<Int64Type>().value(0), 0);
    assert_eq!(row.column(2).as_primitive::<Int64Type>().value(0), 24);
    assert_eq!(
        (text(row.column(3)), text(row.column(4))),
        ("s000".into(), "s024".into())
    );
    let row = one(&context, "select min(k), max(k) from main.db.small").await?;
    assert_eq!(row.column(1).as_primitive::<Int64Type>().value(0), 7);
    assert_eq!(long(&context, "select count(*) from main.db.none").await?, 0);
    assert_eq!(store.take(), 0, "no request to storage");
    Ok(())
}

fn text(column: &ArrayRef) -> String {
    match column.data_type() {
        DataType::Utf8View => column.as_string_view().value(0).to_string(),
        _ => column.as_string::<i32>().value(0).to_string(),
    }
}

#[tokio::test]
async fn inexact_statistics_not_used() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, main, storage, schema) = lake(store.clone()).await?;
    let shape = shape();
    let (tx, table_id) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        &["db"],
        "t",
        &shape,
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    append(&catalog, &main, &storage, &t, 0..10).await?;

    // Floats are never exact: Parquet leaves NaN out of its bounds.
    let lake = MetaLake::new(catalog.clone(), storage.clone()).await?;
    let context = lake.context()?;
    assert!(reads(&context, "select max(x) from main.db.t").await?);
    assert!(!reads(&context, "select max(k), max(n) from main.db.t").await?);

    // After a rename the file's bounds are still found, by field id; after a promotion, or for a column the file
    // lacks, they are not.
    let by = |name: &str| t.fields().iter().find(|f| f.name == name).unwrap().clone();
    let field = |f: &metalake::Field, name: &str, ty: &str| -> Value {
        json!({"field_id": f.field_id, "parent_field_id": f.parent_field_id, "position": f.position, "name": name,
               "type": ty, "required": f.required, "initial_default": f.initial_default,
               "write_default": f.write_default, "doc": f.doc})
    };
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    let new = p.next_id(&session, table_id, "field").await?;
    let changes = json!({ table_id.to_string(): { "fields": [
        field(&by("s"), "label", "string"),
        field(&by("n"), "n", "long"),
        {"field_id": new, "new": true, "parent_field_id": null, "position": 5, "name": "extra", "type": "long",
         "required": false, "initial_default": 7, "write_default": 7},
    ]}});
    p.commit(
        &session,
        &Commit {
            ref_id: main.id(),
            operation: "update".into(),
            changes: Some(changes),
            ..Default::default()
        },
    )
    .await?;
    assert!(!reads(&context, "select min(label) from main.db.t").await?);
    let row = one(&context, "select min(label) from main.db.t").await?;
    assert_eq!(text(row.column(0)), "s000");
    assert!(reads(&context, "select max(n) from main.db.t").await?, "promoted");
    assert!(reads(&context, "select max(extra) from main.db.t").await?, "absent");

    // A delete may have taken a bound: the count stays exact, the bounds do not.
    let now = main.table(&["db"], "t").await?;
    let tasks: Vec<_> = Scan::new(&catalog, &now).plan().await?.try_collect().await?;
    let mut deletes = Deletes::new();
    deletes.insert(tasks[0].file_id, 0);
    main.transaction().delete(&now, &storage, deletes).commit().await?;
    assert!(!reads(&context, "select count(*) from main.db.t").await?);
    assert_eq!(long(&context, "select count(*) from main.db.t").await?, 9);
    assert!(reads(&context, "select min(k) from main.db.t").await?);
    Ok(())
}

/// The cost of planning a table of many files, footers parsed: `--ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn planning_cost() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, _) = lake(Arc::default()).await?;
    let shape = shape();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        &["db"],
        "t",
        &shape,
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    for at in 0..300 {
        append(&catalog, &main, &storage, &t, at..at + 1).await?;
    }
    let context = MetaLake::new(catalog, storage).await?.context()?;
    let start = std::time::Instant::now();
    assert_eq!(long(&context, "select count(*) from main.db.t").await?, 300);
    println!("300 files: {:?}", start.elapsed());
    Ok(())
}
