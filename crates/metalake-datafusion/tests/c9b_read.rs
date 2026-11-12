//! C9b (ADR 0003 §7): DataFusion reads what the core reads. `Scan::read` is the oracle: every query is run over the
//! table through DataFusion's reader and over the core's own rows in memory, and both must answer the same, with
//! inline files, vectors inline and in Puffin, a row group wholly deleted, and a schema that changed by renames,
//! promotions, defaults and nested fields. Footers cost no request; Postgres leaves files out of a filtered plan; and
//! a join's dynamic filter spares requests.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, Date32Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use common::{Counting, fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, Storage, Table, TableWriter, WriteOptions};
use metalake_datafusion::{MetaLake, measure};
use metalake_pg::{Commit, Pool, Protocol};
use serde_json::{Value, json};

fn simple(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.map(|k| format!("s{k:03}")))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn new_table<'a>(name: &'a str, schema: &'a Schema, properties: Vec<(&'a str, &'a str)>) -> NewTable<'a> {
    NewTable {
        namespace: &["db"],
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

async fn write(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    t: &Table,
    batch: RecordBatch,
    options: WriteOptions,
) -> Result<(), Error> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), options)?;
    writer.write(&batch).await?;
    on.transaction().append(writer.finish().await?).commit().await?;
    Ok(())
}

/// `sql` over `main.db.t` through DataFusion, and over the rows `Scan::read` gives, as `main_db_t` in memory.
async fn same(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    context: &SessionContext,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let scan = Scan::new(catalog, t);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let oracle = SessionContext::new();
    let schema = t.schema().clone();
    oracle.register_table("oracle", Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    let theirs = oracle.sql(&sql.replace("main.db.t", "oracle")).await?.collect().await?;
    let ours = context.sql(sql).await?.collect().await?;
    let (ours, theirs) = (pretty_format_batches(&ours)?, pretty_format_batches(&theirs)?);
    assert_eq!(ours.to_string(), theirs.to_string(), "{sql}");
    Ok(())
}

/// The plan of `sql`, verbose: every file group, whatever the machine's cores.
async fn plan(context: &SessionContext, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let plan = context.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
}

#[tokio::test]
async fn scan_matches_core_with_vectors_and_inline() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, main, storage, _) = lake(store.clone()).await?;
    let shape = simple(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        "t",
        &shape,
        vec![("inline.max-bytes", "1024")],
    ))?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    // A file of four row groups, one small enough to be inline, and a third with no vector.
    let groups = WriteOptions {
        row_group_rows: 25,
        ..Default::default()
    };
    write(&catalog, &main, &storage, &t, simple(0..100), groups).await?;
    write(&catalog, &main, &storage, &t, simple(500..503), WriteOptions::default()).await?;
    write(
        &catalog,
        &main,
        &storage,
        &t,
        simple(1000..1100),
        WriteOptions::default(),
    )
    .await?;
    let t = main.table(&["db"], "t").await?;
    let tasks: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let stored = tasks
        .iter()
        .find(|f| f.record_count == 100 && f.footer.is_some())
        .unwrap();
    let inline = tasks.iter().find(|f| f.path.is_none()).unwrap();
    assert_eq!(tasks.len(), 3);

    // Every row of the second row group, others around it, and the middle of the inline file: over the limit in
    // Puffin, under it inline.
    let mut deletes = Deletes::new();
    for position in (25..50).chain([0, 3, 77, 99]) {
        deletes.insert(stored.file_id, position);
    }
    deletes.insert(inline.file_id, 1);
    main.transaction().delete(&t, &storage, deletes).commit().await?;
    let t = main.table(&["db"], "t").await?;

    let context = MetaLake::new(catalog.clone(), storage.clone()).await?.context()?;
    for sql in [
        "select * from main.db.t order by k",
        "select s from main.db.t order by s",
        "select count(*) as c from main.db.t",
        "select count(s) as c, min(k) as l, max(s) as h from main.db.t",
        "select k from main.db.t where k > 40 and k < 60 order by k",
        "select k from main.db.t where s = 's501'",
        "select k from main.db.t where k = 25",
        "select k from main.db.t where 60 > k order by k",
        "select sum(k) as c from main.db.t where k >= 1000",
    ] {
        same(&catalog, &t, &storage, &context, sql).await?;
    }

    // Footers come from the catalog: a read of the first file's rows asks only for its row groups.
    let cold = MetaLake::new(catalog, storage).await?.context()?;
    store.take();
    cold.sql("select k from main.db.t where k >= 1000")
        .await?
        .collect()
        .await?;
    assert_eq!(store.take(), 1, "the one row group of one file");
    Ok(())
}

fn before() -> RecordBatch {
    let keys = 0..10i64;
    let lat = Arc::new(Field::new("lat", DataType::Float64, true));
    let tags = (0..10).map(|i| Some(vec![Some(i), Some(-i)]));
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("k", Arc::new(Int64Array::from_iter_values(keys.clone()))),
        (
            "n",
            Arc::new(Int32Array::from_iter_values(keys.clone().map(|i| i as i32))),
        ),
        (
            "s",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|i| format!("s{i}")))),
        ),
        (
            "day",
            Arc::new(Date32Array::from_iter_values(keys.clone().map(|i| 18_000 + i as i32))),
        ),
        (
            "gone",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|_| "g"))),
        ),
        (
            "loc",
            Arc::new(StructArray::new(
                Fields::from(vec![lat]),
                vec![Arc::new(arrow_array::Float64Array::from_iter_values(keys.map(|i| i as f64))) as ArrayRef],
                None,
            )),
        ),
        (
            "tags",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(tags)),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn field(f: &metalake::Field, name: &str, ty: &str) -> Value {
    json!({"field_id": f.field_id, "parent_field_id": f.parent_field_id, "position": f.position, "name": name,
           "type": ty, "required": f.required, "initial_default": f.initial_default,
           "write_default": f.write_default, "doc": f.doc})
}

#[tokio::test]
async fn schema_evolution_old_files() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, schema) = lake(Arc::default()).await?;
    let first = before();
    let (tx, table_id) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        "t",
        first.schema_ref(),
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    write(&catalog, &main, &storage, &t, first, WriteOptions::default()).await?;

    // A rename, three promotions, one of them inside a list, a drop, a column with a default, one without, one with
    // the name a dropped one had, and a struct's new child.
    let by = |name: &str| t.fields().iter().find(|f| f.name == name).unwrap().clone();
    let tags = by("tags");
    let element = t
        .fields()
        .iter()
        .find(|f| f.parent_field_id == Some(tags.field_id))
        .unwrap();
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    let mut next = Vec::new();
    for _ in 0..4 {
        next.push(p.next_id(&session, table_id, "field").await?);
    }
    let added = |id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, default: Value| {
        json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name,
               "type": ty, "required": false, "initial_default": default, "write_default": default})
    };
    let changes = json!({ table_id.to_string(): {
        "fields": [
            field(&by("s"), "label", "string"),
            field(&by("n"), "n", "long"),
            field(&by("day"), "day", "timestamp"),
            field(element, &element.name, "long"),
            added(next[0], None, 11, "extra", "long", json!(7)),
            added(next[1], None, 12, "note", "string", Value::Null),
            added(next[3], Some(by("loc").field_id), 2, "lon", "double", json!(0.5)),
        ],
        "drop_fields": [by("gone").field_id],
    }});
    let commit = Commit {
        ref_id: main.id(),
        operation: "update".into(),
        changes: Some(changes),
        ..Default::default()
    };
    p.commit(&session, &commit).await?;
    let again = json!({ table_id.to_string(): {
        "fields": [added(next[2], None, 13, "gone", "long", Value::Null)],
    }});
    let again = Commit {
        changes: Some(again),
        ..commit
    };
    p.commit(&session, &again).await?;

    let now = main.table(&["db"], "t").await?;
    let context = MetaLake::new(catalog.clone(), storage.clone()).await?.context()?;
    for sql in [
        "select * from main.db.t order by k",
        "select label, extra, note from main.db.t order by k",
        "select k from main.db.t where label = 's3'",
        "select k from main.db.t where n > 7 order by k",
        "select k from main.db.t where extra = 7 order by k limit 2",
        "select gone from main.db.t limit 1",
        "select count(*) as c from main.db.t where extra = 8",
        "select max(n) as n, max(day) as d, min(label) as l from main.db.t",
    ] {
        same(&catalog, &now, &storage, &context, sql).await?;
    }
    Ok(())
}

#[tokio::test]
async fn filter_pushdown_and_dynamic_join_filter() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, main, storage, _) = lake(store.clone()).await?;
    let shape = simple(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        "big",
        &shape,
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let (tx, _) = main
        .transaction()
        .create_table(new_table("small", &shape, vec![("inline.max-bytes", "0")]))?;
    tx.commit().await?;
    let big = main.table(&["db"], "big").await?;
    for at in 0..6 {
        let keys = at * 100..at * 100 + 50;
        write(&catalog, &main, &storage, &big, simple(keys), WriteOptions::default()).await?;
    }
    let small = main.table(&["db"], "small").await?;
    write(
        &catalog,
        &main,
        &storage,
        &small,
        simple(210..212),
        WriteOptions::default(),
    )
    .await?;

    // A range on the cluster key: the plan holds the files it can match, before anything is read.
    let context = MetaLake::new(catalog, storage).await?.context()?;
    let filtered = plan(&context, "select s from main.db.big where k >= 210 and k < 220").await?;
    assert!(filtered.contains("file_groups={1 group"), "{filtered}");
    let on_s = plan(&context, "select k from main.db.big where s = 's210'").await?;
    // Every file, in as many groups as the machine has cores.
    assert_eq!(on_s.matches(".parquet").count(), 6, "only the cluster key: {on_s}");
    assert!(
        plan(&context, "select s from main.db.big where 210 = k")
            .await?
            .contains("{1 group")
    );

    // The small side tells the big one what to skip.
    let join = "select big.k, big.s from main.db.big join main.db.small on big.k = small.k order by 1";
    store.take();
    let rows = context.sql(join).await?.collect().await?;
    let with = store.take();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    context
        .sql("set datafusion.optimizer.enable_join_dynamic_filter_pushdown = false")
        .await?
        .collect()
        .await?;
    store.take();
    context.sql(join).await?.collect().await?;
    let without = store.take();
    println!("join: {with} requests with a dynamic filter, {without} without");
    assert!(with < without, "{with} requests with it, {without} without");
    Ok(())
}

/// What a plan reads, measured before it runs: the bytes and rows of the files its scan reads after pruning, and
/// whether it shuffles. Six files of fifty rows; a range on the cluster key keeps one.
#[tokio::test]
async fn measure_after_pruning() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, main, storage, _) = lake(Arc::new(Counting::default())).await?;
    let shape = simple(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(new_table(
        "big",
        &shape,
        vec![("inline.max-bytes", "0")],
    ))?;
    tx.commit().await?;
    let (tx, _) = main
        .transaction()
        .create_table(new_table("small", &shape, vec![("inline.max-bytes", "0")]))?;
    tx.commit().await?;
    let big = main.table(&["db"], "big").await?;
    for at in 0..6 {
        let keys = at * 100..at * 100 + 50;
        write(&catalog, &main, &storage, &big, simple(keys), WriteOptions::default()).await?;
    }
    let small = main.table(&["db"], "small").await?;
    write(&catalog, &main, &storage, &small, simple(210..212), WriteOptions::default()).await?;
    let context = MetaLake::new(catalog, storage).await?.context()?;
    for set in [
        "set datafusion.execution.target_partitions = 4",
        // Joins by hash on both sides, whatever the size of the small one.
        "set datafusion.optimizer.hash_join_single_partition_threshold = 0",
        "set datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
    ] {
        context.sql(set).await?.collect().await?;
    }
    let measured = |sql: &'static str| {
        let context = context.clone();
        async move {
            let plan = context.sql(sql).await?.create_physical_plan().await?;
            Ok::<_, Box<dyn std::error::Error>>(measure(&plan))
        }
    };

    let all = measured("select s from main.db.big").await?;
    assert_eq!((all.scans, all.rows, all.shuffle), (1, Some(300), false));
    assert!(all.bytes > 0);
    let one = measured("select s from main.db.big where k >= 210 and k < 220").await?;
    assert_eq!((one.rows, one.shuffle), (Some(50), false));
    assert!(one.bytes * 4 < all.bytes, "{one:?} of {all:?}");

    let grouped = measured("select k % 3, count(*) from main.db.big group by 1").await?;
    assert!(grouped.shuffle, "{grouped:?}");
    let joined = measured("select b.s from main.db.big b join main.db.small s on b.k = s.k").await?;
    assert_eq!((joined.scans, joined.rows, joined.shuffle), (2, Some(302), true));
    Ok(())
}
