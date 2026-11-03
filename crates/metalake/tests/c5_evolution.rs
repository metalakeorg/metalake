//! C5b (ADR 0003 §7): reading under a later schema and in the past. A file written before its table changed reads
//! under the table as it is now: promoted types widened (TYP-3), a renamed column by its id, a dropped one never
//! asked for, added columns as their initial defaults or null (TYP-5), a struct's new child too, and a list's element
//! and a map's value promoted; a range on a field promoted from date prunes that file's row groups (STA-7). A read
//! at an earlier sequence sees the files and the schema of then; a branch, its own files.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::builder::{Int32Builder, Int64Builder, MapBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use futures_util::{StreamExt, TryStreamExt};
use metalake::partition::Scalar;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, Storage, Table, TableWriter, WriteOptions};
use metalake_pg::{Commit, CreateRef, Pool, Protocol};
use serde_json::{Value, json};

const DAY: i64 = 86_400_000_000;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("evolve_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

fn int_list(rows: impl Iterator<Item = i64>, long: bool) -> ArrayRef {
    let values: Vec<Option<Vec<Option<i64>>>> = rows.map(|i| Some(vec![Some(i), Some(-i)])).collect();
    if long {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(values))
    } else {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            values
                .into_iter()
                .map(|l| l.map(|l| l.into_iter().map(|v| v.map(|v| v as i32)).collect::<Vec<_>>())),
        ))
    }
}

fn map_of(rows: impl Iterator<Item = i64>, long: bool) -> ArrayRef {
    if long {
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for i in rows {
            m.keys().append_value("a");
            m.values().append_value(i);
            m.append(true).unwrap();
        }
        Arc::new(m.finish())
    } else {
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for i in rows {
            m.keys().append_value("a");
            m.values().append_value(i as i32);
            m.append(true).unwrap();
        }
        Arc::new(m.finish())
    }
}

/// Rows 0..1000 under the first schema: `day` one value for each 100 rows.
fn before() -> RecordBatch {
    let keys = 0..1000i64;
    let lat = Arc::new(Field::new("lat", DataType::Float64, true));
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("k", Arc::new(Int64Array::from_iter_values(keys.clone()))),
        (
            "n",
            Arc::new(Int32Array::from_iter_values(keys.clone().map(|i| i as i32 - 500))),
        ),
        (
            "f",
            Arc::new(Float32Array::from_iter_values(keys.clone().map(|i| i as f32 / 4.0))),
        ),
        (
            "day",
            Arc::new(Date32Array::from_iter_values(
                keys.clone().map(|i| 18_000 + (i / 100) as i32),
            )),
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from_iter_values(keys.clone().map(|i| i128::from(i) * 7))
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
        ),
        (
            "s",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|i| format!("s{i}")))),
        ),
        (
            "gone",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|_| "g"))),
        ),
        (
            "loc",
            // Null every 5th row.
            Arc::new(StructArray::new(
                vec![lat].into(),
                vec![Arc::new(Float64Array::from_iter_values(keys.clone().map(|i| i as f64))) as ArrayRef],
                Some(keys.clone().map(|i| i % 5 != 0).collect::<Vec<_>>().into()),
            )),
        ),
        ("tags", int_list(keys.clone(), false)),
        ("attrs", map_of(keys, false)),
    ];
    batch(columns)
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, c)| Field::new(*name, c.data_type().clone(), *name != "k"))
        .collect();
    let arrays = columns.into_iter().map(|(_, c)| c).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// Rows `keys` under the table after its change, in its columns' order.
fn after(keys: std::ops::Range<i64>) -> RecordBatch {
    let lat_lon: Fields = vec![
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
    ]
    .into();
    let n = keys.clone().count();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("k", Arc::new(Int64Array::from_iter_values(keys.clone()))),
        ("n", Arc::new(Int64Array::from_iter_values(keys.clone()))),
        (
            "f",
            Arc::new(Float64Array::from_iter_values(keys.clone().map(|i| i as f64 / 4.0))),
        ),
        (
            "day",
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                keys.clone().map(|i| 20_000 * DAY + i),
            )),
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from_iter_values(keys.clone().map(|i| i128::from(i) * 7))
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
        ),
        (
            "label",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|i| format!("s{i}")))),
        ),
        (
            "loc",
            Arc::new(StructArray::new(
                lat_lon,
                vec![
                    Arc::new(Float64Array::from_iter_values(keys.clone().map(|i| i as f64))),
                    Arc::new(Float64Array::from(vec![1.5; n])),
                ],
                None,
            )),
        ),
        ("tags", int_list(keys.clone(), true)),
        ("attrs", map_of(keys.clone(), true)),
        ("extra", Arc::new(Int64Array::from_iter_values(keys.clone()))),
        ("flag", Arc::new(BooleanArray::from(vec![false; n]))),
        (
            "note",
            Arc::new(StringArray::from_iter_values(keys.clone().map(|_| "w"))),
        ),
        (
            "place",
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("city", DataType::Utf8, true)),
                Arc::new(StringArray::from_iter_values(keys.map(|_| "Madrid"))) as ArrayRef,
            )])),
        ),
    ];
    batch(columns)
}

async fn write(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    b: RecordBatch,
    options: WriteOptions,
) -> Result<i64, Error> {
    let t = on.table(&["db"], "t").await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), options)?;
    writer.write(&b).await?;
    let files = writer.finish().await?;
    Ok(on.transaction().append(files).commit().await?.seq())
}

/// The table's rows, sorted by `k`.
async fn rows(catalog: &Catalog, storage: &Storage, table: &Table) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let scan = Scan::new(catalog, table);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let all = arrow_select::concat::concat_batches(scan.schema(), &batches)?;
    let keys = all.column(0).as_primitive::<Int64Type>();
    let mut order: Vec<u32> = (0..all.num_rows() as u32).collect();
    order.sort_by_key(|i| keys.value(*i as usize));
    Ok(arrow_select::take::take_record_batch(
        &all,
        &arrow_array::UInt32Array::from(order),
    )?)
}

/// A field as the protocol takes it in a change, from the table's.
fn field(f: &metalake::Field) -> Value {
    json!({"field_id": f.field_id, "parent_field_id": f.parent_field_id, "position": f.position, "name": f.name,
           "type": f.ty, "required": f.required, "initial_default": f.initial_default,
           "write_default": f.write_default, "doc": f.doc})
}

#[tokio::test]
async fn files_written_before_a_change_read_under_the_table_after_it() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    let (tx, table_id) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &before().schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let groups = WriteOptions {
        row_group_rows: 100,
        ..Default::default()
    };
    let written = write(&catalog, &main, &storage, before(), groups).await?;

    // The change: four promotions, a rename, a drop, three columns added, a struct's child added, and a list's
    // element and a map's value promoted.
    let t = main.table(&["db"], "t").await?;
    let by = |name: &str, parent: Option<i32>| {
        t.fields()
            .iter()
            .find(|f| f.name == name && f.parent_field_id == parent)
            .cloned()
            .unwrap_or_else(|| panic!("a field {name}"))
    };
    let retyped = |name: &str, ty: &str| {
        let mut f = by(name, None);
        f.ty = ty.into();
        field(&f)
    };
    let mut s = by("s", None);
    s.name = "label".into();
    let (loc, tags, attrs) = (by("loc", None), by("tags", None), by("attrs", None));
    let mut element = t
        .fields()
        .iter()
        .find(|f| f.parent_field_id == Some(tags.field_id))
        .cloned()
        .unwrap();
    element.ty = "long".into();
    let mut value = by("value", Some(attrs.field_id));
    value.ty = "long".into();
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    let mut next = Vec::new();
    for _ in 0..6 {
        next.push(p.next_id(&session, table_id, "field").await?);
    }
    let added = |id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, required: bool, default: Value| {
        json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name,
               "type": ty, "required": required, "initial_default": default, "write_default": default})
    };
    let changes = json!({ table_id.to_string(): {
        "fields": [
            retyped("n", "long"), retyped("f", "double"), retyped("day", "timestamp"),
            retyped("amount", "decimal(18,2)"), field(&s), field(&element), field(&value),
            added(next[0], None, 11, "extra", "long", false, Value::Null),
            added(next[1], None, 12, "flag", "boolean", true, json!(true)),
            added(next[2], None, 13, "note", "string", false, json!("n/a")),
            added(next[3], Some(loc.field_id), 2, "lon", "double", false, json!(0.5)),
            added(next[4], None, 14, "place", "struct", false, json!({})),
            added(next[5], Some(next[4]), 1, "city", "string", false, json!("?")),
        ],
        "drop_fields": [by("gone", None).field_id],
    }});
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
    let now = main.table(&["db"], "t").await?;
    let names: Vec<&str> = now.schema().fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "k", "n", "f", "day", "amount", "label", "loc", "tags", "attrs", "extra", "flag", "note", "place"
        ]
    );
    write(&catalog, &main, &storage, after(1000..1100), WriteOptions::default()).await?;
    // A table is a read point: the write after it is seen by a read taken after it.
    let now = main.table(&["db"], "t").await?;

    // Every row under the table as it is now: the first file's widened and defaulted, the second's as written.
    let all = rows(&catalog, &storage, &now).await?;
    assert_eq!(all.num_rows(), 1100);
    assert_eq!(all.schema(), *now.schema());
    let col = |name: &str| all.column_by_name(name).unwrap().clone();
    let (n, f, day) = (col("n"), col("f"), col("day"));
    let (n, f, day) = (
        n.as_primitive::<Int64Type>(),
        f.as_primitive::<Float64Type>(),
        day.as_primitive::<TimestampMicrosecondType>(),
    );
    let amount = col("amount");
    let amount = amount.as_primitive::<Decimal128Type>();
    assert_eq!(amount.data_type(), &DataType::Decimal128(18, 2));
    let label = col("label");
    let label = label.as_string::<i32>();
    let loc = col("loc");
    let loc = loc.as_struct();
    let (lat, lon) = (
        loc.column(0).as_primitive::<Float64Type>(),
        loc.column(1).as_primitive::<Float64Type>(),
    );
    let tags = col("tags");
    let tags = tags.as_list::<i32>();
    let attrs = col("attrs");
    let attrs = attrs.as_map();
    let (extra, flag, note) = (col("extra"), col("flag"), col("note"));
    let (extra, flag, note) = (
        extra.as_primitive::<Int64Type>(),
        flag.as_boolean(),
        note.as_string::<i32>(),
    );
    let place = col("place");
    let place = place.as_struct();
    let city = place.column(0).as_string::<i32>();
    for row in 0..all.num_rows() {
        let i = row as i64;
        let old = i < 1000;
        assert_eq!(n.value(row), if old { i - 500 } else { i }, "n of {i}");
        assert_eq!(f.value(row), i as f64 / 4.0, "f of {i}");
        let want_day = if old {
            (18_000 + i / 100) * DAY
        } else {
            20_000 * DAY + i
        };
        assert_eq!(day.value(row), want_day, "day of {i}");
        assert_eq!(amount.value(row), i128::from(i) * 7);
        assert_eq!(label.value(row), format!("s{i}"));
        assert_eq!(loc.is_null(row), old && i % 5 == 0, "loc of {i}");
        if loc.is_valid(row) {
            assert_eq!(lat.value(row), i as f64);
            assert_eq!(lon.value(row), if old { 0.5 } else { 1.5 }, "lon of {i}");
        }
        assert_eq!(tags.value(row).as_primitive::<Int64Type>().values(), &[i, -i]);
        assert_eq!(attrs.value(row).column(1).as_primitive::<Int64Type>().value(0), i);
        assert_eq!(extra.is_null(row), old, "extra of {i}");
        assert_eq!(flag.value(row), old, "flag of {i}");
        assert_eq!(note.value(row), if old { "n/a" } else { "w" }, "note of {i}");
        assert!(place.is_valid(row), "place of {i}");
        assert_eq!(city.value(row), if old { "?" } else { "Madrid" }, "city of {i}");
    }

    // A range on day, promoted from date: Postgres keeps the first file only, and its reader one row group of 100.
    let scan = Scan::new(&catalog, &now).filter(
        "day",
        Some(Scalar::Long(18_003 * DAY)),
        Some(Scalar::Long(18_003 * DAY + 1)),
    )?;
    let tasks: Vec<_> = scan.plan().await?.try_collect().await?;
    assert_eq!(tasks.len(), 1);
    let batches: Vec<RecordBatch> = scan
        .read(futures_util::stream::iter(tasks.into_iter().map(Ok)).boxed(), &storage)
        .try_collect()
        .await?;
    let keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    assert_eq!(keys, (300..400).collect::<Vec<_>>());

    // At the sequence of the first write: its one file, under the schema of then.
    let then = main.at(written).table(&["db"], "t").await?;
    let old = rows(&catalog, &storage, &then).await?;
    assert_eq!(old.num_rows(), 1000);
    assert_eq!(
        old.column_by_name("n").unwrap().as_primitive::<Int32Type>().value(0),
        -500
    );
    assert_eq!(
        old.column_by_name("day")
            .unwrap()
            .as_primitive::<Date32Type>()
            .value(999),
        18_009
    );
    assert_eq!(old.column_by_name("gone").unwrap().as_string::<i32>().value(0), "g");
    assert!(old.column_by_name("label").is_none() && old.column_by_name("extra").is_none());

    // A branch: its own file beside the parent's, which the parent does not see.
    p.create_ref(&session, &CreateRef::branch("dev", main.id())).await?;
    let dev = catalog.branch("dev").await?;
    write(&catalog, &dev, &storage, after(5000..5010), WriteOptions::default()).await?;
    let on_dev = rows(&catalog, &storage, &dev.table(&["db"], "t").await?).await?;
    assert_eq!(on_dev.num_rows(), 1110);
    assert_eq!(on_dev.column(0).as_primitive::<Int64Type>().value(1109), 5009);
    assert_eq!(
        rows(&catalog, &storage, &main.table(&["db"], "t").await?)
            .await?
            .num_rows(),
        1100
    );
    Ok(())
}
