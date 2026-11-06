//! C4b (ADR 0003 §7): the writer. A small batch becomes an inline file, never uploaded; a large one, several files
//! rolled at the target size and uploaded, each with its intent registered first and its footer size that of the
//! object stored; the files commit and the protocol plans them; files finished but never committed are orphans; and
//! batches that are not the table's are refused.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Storage, Table, TableWriter, WriteOptions};
use metalake_pg::{Pool, Protocol};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("core_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

fn columns() -> Schema {
    Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("d", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
    ])
}

/// Rows `from..to`: `d` is NaN every 10th row, `s` long and varied enough not to compress away, `at` in a zone of
/// the batch's own, which the writer takes as the instant it is.
fn batch(from: i64, to: i64) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("d", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let keys: Vec<i64> = (from..to).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.clone())),
        Arc::new(Float64Array::from_iter(
            keys.iter()
                .map(|k| Some(if k % 10 == 0 { f64::NAN } else { *k as f64 / 3.0 })),
        )),
        Arc::new(StringArray::from_iter_values(
            keys.iter()
                .map(|k| format!("{:x}-{}", k.wrapping_mul(2_654_435_761), k * 7)),
        )),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(keys.iter().map(|k| 1_700_000_000_000_000 + k))
                .with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn setup() -> Result<(String, Catalog, Table, Storage), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "events",
        schema: &columns(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let table = main.table(&["db"], "events").await?;
    Ok((schema, catalog, table, Storage::in_memory()))
}

#[tokio::test]
async fn writer_inline_threshold() -> Result<(), Box<dyn std::error::Error>> {
    let (schema, catalog, table, storage) = setup().await?;
    let main = catalog.branch("main").await?;
    let pool = Pool::new(&url(), 1)?;
    let s = pool.get().await?;
    let p = Protocol::new(&schema);
    let later = SystemTime::now() + Duration::from_secs(7200);

    // Ten rows: one inline file, no object and no intent.
    let mut writer = TableWriter::new(&catalog, &table, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(0, 10)).await?;
    let small = writer.finish().await?;
    assert_eq!(small.len(), 1);
    let inline = &small[0];
    assert!(inline.path.is_none() && inline.content.is_some());
    assert_eq!(inline.file_size, inline.content.as_ref().unwrap().len() as i64);
    assert_eq!(
        metalake::stats::footer_size(inline.content.as_ref().unwrap()),
        Some(inline.footer_size)
    );
    assert_eq!((inline.record_count, inline.stat_field_ids[0]), (10, 1));
    assert_eq!(inline.nan_count[1], Some(1), "row 0 of d is NaN");
    assert!(p.orphans(&s, Some(later)).await?.is_empty());
    let first = main.transaction().append(small).commit().await?;

    // 200,000 rows, rolled at 256 KiB: several files, each uploaded, its intent registered before.
    let options = WriteOptions {
        row_group_rows: 20_000,
        target_file_size: Some(256 * 1024),
        ..Default::default()
    };
    let mut writer = TableWriter::new(&catalog, &table, storage.clone(), options)?;
    for start in (10..200_010).step_by(20_000) {
        writer.write(&batch(start, start + 20_000)).await?;
    }
    let large = writer.finish().await?;
    assert!(large.len() > 1, "{} files", large.len());
    assert_eq!(large.iter().map(|f| f.record_count).sum::<i64>(), 200_000);
    let mut paths = Vec::new();
    for f in &large {
        let path = f.path.clone().expect("an uploaded file has a path");
        assert!(path.starts_with(table.location()) && f.content.is_none(), "{path}");
        let object = storage.get(&path).await?;
        assert_eq!(object.len() as i64, f.file_size);
        assert_eq!(metalake::stats::footer_size(&object), Some(f.footer_size));
        paths.push(path);
    }
    let mut orphans = p.orphans(&s, Some(later)).await?;
    orphans.sort();
    paths.sort();
    assert_eq!(orphans, paths, "uploaded, not yet committed");
    let second = main.transaction().append(large).commit().await?;
    assert!(second.seq() > first.seq());
    assert!(
        p.orphans(&s, Some(later)).await?.is_empty(),
        "a commit takes its files' intents"
    );

    let planned = p
        .plan(&s, 1, table.id(), None, None)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(planned.len(), paths.len() + 1);
    assert_eq!(
        planned
            .iter()
            .filter(|f| f.path.is_none() && f.content.is_some())
            .count(),
        1
    );
    assert_eq!(planned.iter().map(|f| f.record_count).sum::<i64>(), 200_010);

    // Finished and never committed: its files are orphans once their intents expire.
    let short = WriteOptions {
        intent_ttl: Duration::from_secs(1),
        target_file_size: Some(256 * 1024),
        ..Default::default()
    };
    let mut writer = TableWriter::new(&catalog, &table, storage.clone(), short)?;
    writer.write(&batch(0, 50_000)).await?;
    let abandoned = writer.finish().await?;
    let abandoned: Vec<String> = abandoned.into_iter().filter_map(|f| f.path).collect();
    assert!(!abandoned.is_empty());
    let mut orphans = p.orphans(&s, Some(SystemTime::now() + Duration::from_secs(5))).await?;
    orphans.sort();
    let mut abandoned = abandoned;
    abandoned.sort();
    assert_eq!(orphans, abandoned);
    Ok(())
}

#[tokio::test]
async fn writer_schema_mismatch_refused() -> Result<(), Box<dyn std::error::Error>> {
    let (_schema, catalog, table, storage) = setup().await?;
    let mut writer = TableWriter::new(&catalog, &table, storage, WriteOptions::default())?;
    let good = batch(0, 3);

    let reordered = good.project(&[1, 0, 2, 3])?;
    assert!(matches!(writer.write(&reordered).await, Err(Error::Schema(m)) if m.contains("columns")));

    let nullable = Schema::new(
        good.schema()
            .fields()
            .iter()
            .map(|f| {
                if f.name() == "k" {
                    Field::new("k", DataType::Int64, true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    );
    let mut columns = good.columns().to_vec();
    columns[0] = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let with_null = RecordBatch::try_new(Arc::new(nullable), columns)?;
    assert!(matches!(writer.write(&with_null).await, Err(Error::Schema(m)) if m.contains("required")));

    let wrong = Schema::new(
        good.schema()
            .fields()
            .iter()
            .map(|f| {
                if f.name() == "k" {
                    Field::new("k", DataType::Int32, false)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    );
    let mut columns = good.columns().to_vec();
    columns[0] = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
    let wrong = RecordBatch::try_new(Arc::new(wrong), columns)?;
    assert!(matches!(writer.write(&wrong).await, Err(Error::Schema(m)) if m.starts_with("k: ")));

    writer.write(&good).await?;
    assert_eq!(writer.finish().await?.len(), 1);
    Ok(())
}

/// C4c: a partitioned table's rows go to one file for each partition value, which the file carries, nulls
/// included; the files commit.
#[tokio::test]
async fn writer_partition_fanout() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeMap;

    use metalake::partition::{Scalar, Transform};
    use serde_json::{Value, json};

    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let columns = Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new("k", DataType::Int64, false),
    ]);
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "visits",
        schema: &columns,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![("region", "identity"), ("at", "day"), ("k", "bucket[4]")],
        properties: vec![],
    })?;
    tx.commit().await?;
    let table = main.table(&["db"], "visits").await?;
    let spec = &table.specs()[&table.spec_id().unwrap()];
    assert_eq!(spec.as_array().map(Vec::len), Some(3), "{spec}");
    assert_eq!(spec[1]["name"], json!("at_day"));

    let regions = [Some("eu"), Some("us"), None];
    let rows: Vec<(Option<&str>, i64, i64)> = (0..1000i64)
        .map(|k| {
            (
                regions[(k % 3) as usize],
                1_700_000_000_000_000 + (k % 4) * 86_400_000_000,
                k,
            )
        })
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(columns.clone()),
        vec![
            Arc::new(StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
            Arc::new(TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.1)).with_timezone("+00:00")),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )?;
    let mut writer = TableWriter::new(&catalog, &table, Storage::in_memory(), WriteOptions::default())?;
    writer.write(&batch.slice(0, 500)).await?;
    writer.write(&batch.slice(500, 500)).await?;
    let files = writer.finish().await?;

    // What each row's partition value is, as the transforms give it, and how many rows each value has.
    let mut want: BTreeMap<String, i64> = BTreeMap::new();
    for (region, at, k) in &rows {
        let value = json!([
            Transform::Identity.apply("string", region.map_or(Scalar::Null, Scalar::String))?,
            Transform::Day.apply("timestamptz", Scalar::Long(*at))?,
            Transform::Bucket(4).apply("long", Scalar::Long(*k))?,
        ]);
        *want.entry(value.to_string()).or_default() += 1;
    }
    let got: BTreeMap<String, i64> = files
        .iter()
        .map(|f| (f.partition.to_string(), f.record_count))
        .collect();
    assert_eq!(files.len(), want.len(), "one file for each value");
    assert_eq!(got, want);
    assert!(files.iter().all(|f| f.spec_id == table.spec_id().unwrap()));
    assert!(
        files.iter().any(|f| f.partition[0] == Value::Null),
        "nulls have their partition"
    );
    main.transaction().append(files).commit().await?;
    Ok(())
}
