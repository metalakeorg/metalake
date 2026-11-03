//! C3b (ADR 0003 §7): a file's statistics from its Parquet footer. A file written by arrow-rs in three row groups,
//! with nulls, NaN, -0.0, long multi-byte strings and a struct, gives the bounds the reference computes from the same
//! values (`encode_bound`, then `truncate_lower` and `truncate_upper` on the least and the greatest); the protocol
//! accepts the file, and `plan` prunes on its cluster key where it should.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Fields};
use bytes::Bytes;
use futures_util::TryStreamExt;
use metalake::stats::{self, FileFacts};
use metalake::{Catalog, CatalogConfig, Table};
use metalake_pg::{Commit, Pool, Protocol, Prune};
use metalake_protocol_tests::{Error, connect, fresh_schema, quote_ident};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{Value, json};
use uuid::Uuid;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run sql/test.sh")
}

fn field(id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, required: bool) -> Value {
    json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name, "type": ty,
           "required": required})
}

/// A catalog with table `db.t` of every kind of column the test writes, clustered by `i`, its second column.
async fn table(schema: &str) -> Result<(Catalog, Table), Error> {
    let catalog = Catalog::init(CatalogConfig::new(url(), schema), None).await?;
    let pool = Pool::new(&url(), 1)?;
    let t = Uuid::from_u128(0xc3);
    let create = Commit {
        ref_id: 1,
        operation: "append".into(),
        namespaces: Some(json!([{"namespace": ["db"], "create": true}])),
        changes: Some(json!({ t.to_string(): {
            "create": {"namespace": ["db"], "name": "t", "location": "memory:///lake/t"},
            "fields": [
                field(1, None, 1, "k", "long", true),
                field(2, None, 2, "i", "int", false),
                field(3, None, 3, "d", "double", false),
                field(4, None, 4, "s", "string", false),
                field(5, None, 5, "b", "boolean", false),
                field(6, None, 6, "dt", "date", false),
                field(7, None, 7, "ts", "timestamp", false),
                field(8, None, 8, "dec", "decimal(10,2)", false),
                field(9, None, 9, "u", "uuid", false),
                field(10, None, 10, "st", "struct", false),
                field(11, Some(10), 1, "x", "int", false),
            ],
            "cluster_key": [2],
        }})),
        ..Default::default()
    };
    Protocol::new(schema).commit(&pool.get().await?, &create).await?;
    let table = catalog.branch("main").await?.table(&["db"], "t").await?;
    Ok((catalog, table))
}

/// The values of row `n` of the file: (k, i, d, s, b, dt, ts, dec, u, x), nulls where `None`.
#[allow(clippy::type_complexity)]
fn row(
    n: i64,
) -> (
    i64,
    Option<i32>,
    Option<f64>,
    Option<String>,
    Option<bool>,
    Option<i32>,
    Option<i64>,
    Option<i128>,
    Option<[u8; 16]>,
    Option<i32>,
) {
    let d = [
        Some(1.5),
        Some(-0.0),
        Some(f64::NAN),
        None,
        Some(-3.5),
        Some(9.75),
        Some(f64::NAN),
    ][n as usize % 7];
    let s = [
        Some("año".to_string()),
        None,
        Some("zzzzzzzzzzzzzzzzzzzz-long-and-last".to_string()),
        Some("ñññññññññññññññññññññ".to_string()),
        Some("a".to_string()),
    ][n as usize % 5]
        .clone();
    (
        n,
        // `i` is null in the whole second row group.
        (n % 4 != 0 && !(5..10).contains(&n)).then_some(n as i32 * 7 - 30),
        d,
        s,
        (n % 3 != 0).then_some(n % 2 == 0),
        (n % 5 != 1).then_some(18_000 + n as i32 * 400),
        (n % 6 != 2).then_some(1_700_000_000_000_000 + n * 3_600_000_123),
        (n % 4 != 3).then_some(n as i128 * 12_345 - 99_999),
        (n % 7 != 4).then_some([n as u8 * 17; 16]),
        (n % 3 != 1).then_some(100 - n as i32),
    )
}

fn batch(table: &Table, rows: std::ops::Range<i64>) -> RecordBatch {
    let r: Vec<_> = rows.map(row).collect();
    let schema = table.schema().clone();
    let DataType::Struct(inner) = schema.field(9).data_type().clone() else {
        panic!("st is a struct")
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.0))),
        Arc::new(Int32Array::from(r.iter().map(|x| x.1).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(r.iter().map(|x| x.2).collect::<Vec<_>>())),
        Arc::new(StringArray::from(r.iter().map(|x| x.3.clone()).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(r.iter().map(|x| x.4).collect::<Vec<_>>())),
        Arc::new(Date32Array::from(r.iter().map(|x| x.5).collect::<Vec<_>>())),
        Arc::new(TimestampMicrosecondArray::from(
            r.iter().map(|x| x.6).collect::<Vec<_>>(),
        )),
        Arc::new(
            Decimal128Array::from(r.iter().map(|x| x.7).collect::<Vec<_>>())
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(r.iter().map(|x| x.8), 16).unwrap()),
        Arc::new(StructArray::new(
            Fields::from(inner.iter().cloned().collect::<Vec<_>>()),
            vec![Arc::new(Int32Array::from(r.iter().map(|x| x.9).collect::<Vec<_>>())) as ArrayRef],
            None,
        )),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

/// A Parquet file of `rows`, in row groups of `group` rows.
fn write(table: &Table, rows: i64, group: usize) -> Bytes {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, table.schema().clone(), Some(properties)).unwrap();
    for start in (0..rows).step_by(group) {
        writer
            .write(&batch(table, start..(start + group as i64).min(rows)))
            .unwrap();
    }
    writer.close().unwrap();
    Bytes::from(out)
}

/// Days from 1970-01-01 as an ISO date (Howard Hinnant's civil_from_days).
fn iso_date(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn iso_timestamp(micros: i64) -> String {
    let (days, rest) = (micros.div_euclid(86_400_000_000), micros.rem_euclid(86_400_000_000));
    let (secs, frac) = (rest / 1_000_000, rest % 1_000_000);
    format!(
        "{} {:02}:{:02}:{:02}.{frac:06}",
        iso_date(days),
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

fn decimal_text(unscaled: i128, scale: u32) -> String {
    let digits = format!("{:0>width$}", unscaled.unsigned_abs(), width = scale as usize + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
    format!("{}{whole}.{fraction}", if unscaled < 0 { "-" } else { "" })
}

/// The values of each field in the file as the reference reads them, nulls and NaN left out (STA-6).
fn values_by_field(rows: i64) -> Vec<(i32, &'static str, Vec<String>, i64)> {
    let r: Vec<_> = (0..rows).map(row).collect();
    let texts = |f: &dyn Fn(&_) -> Option<String>| -> (Vec<String>, i64) {
        let all: Vec<Option<String>> = r.iter().map(f).collect();
        (
            all.iter().flatten().cloned().collect(),
            all.iter().filter(|v| v.is_none()).count() as i64,
        )
    };
    let mut out = Vec::new();
    let mut add = |id, ty, (values, nulls): (Vec<String>, i64)| out.push((id, ty, values, nulls));
    add(1, "long", texts(&|x| Some(x.0.to_string())));
    add(2, "int", texts(&|x| x.1.map(|v| v.to_string())));
    let (d, _) = texts(&|x| x.2.filter(|v| !v.is_nan()).map(|v| format!("{v:?}")));
    add(3, "double", (d, r.iter().filter(|x| x.2.is_none()).count() as i64));
    add(4, "string", texts(&|x| x.3.clone()));
    add(5, "boolean", texts(&|x| x.4.map(|v| v.to_string())));
    add(6, "date", texts(&|x| x.5.map(|v| iso_date(i64::from(v)))));
    add(7, "timestamp", texts(&|x| x.6.map(iso_timestamp)));
    add(8, "decimal(10,2)", texts(&|x| x.7.map(|v| decimal_text(v, 2))));
    add(9, "uuid", texts(&|x| x.8.map(|v| Uuid::from_bytes(v).to_string())));
    add(11, "int", texts(&|x| x.9.map(|v| v.to_string())));
    out
}

#[tokio::test]
async fn statistics_from_the_footer_are_the_references() -> Result<(), Error> {
    let schema = fresh_schema("c3");
    let (_catalog, table) = table(&schema).await?;
    let rows = 15;
    let file = write(&table, rows, 5);
    let reader = SerializedFileReader::new(file.clone())?;
    assert_eq!(reader.metadata().num_row_groups(), 3);
    let nan_counts = HashMap::from([(3, 4)]);
    let got = stats::of_footer(&table, reader.metadata(), &nan_counts);

    assert_eq!(got.record_count, rows);
    assert_eq!(
        got.field_ids,
        [2, 1, 3, 4, 5, 6, 7, 8, 9, 11],
        "the cluster key first, then the file's order"
    );
    let on = connect().await?;
    let reference = fresh_schema("c3r");
    metalake_protocol_tests::install_reference(&on, &reference).await?;
    on.batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
    let mut fields = values_by_field(rows);
    fields.swap(0, 1);
    for (position, (id, ty, values, nulls)) in fields.into_iter().enumerate() {
        let want: (Option<Vec<u8>>, Option<Vec<u8>>) = on
            .query_one(
                "select truncate_lower($1, (array_agg(e order by e))[1]), truncate_upper($1, (array_agg(e order by e desc))[1])
                   from (select encode_bound($1, v) as e from unnest($2::text[]) v) x",
                &[&ty, &values],
            )
            .await
            .map(|r| (r.get(0), r.get(1)))?;
        assert_eq!(got.field_ids[position], id);
        assert_eq!(
            (&got.lower[position], &got.upper[position]),
            (&want.0, &want.1),
            "bounds of field {id} ({ty})"
        );
        assert_eq!(got.null_count[position], Some(nulls), "nulls of field {id}");
        assert_eq!(got.value_count[position], Some(rows), "values of field {id}");
        let nans = (id == 3).then_some(4);
        assert_eq!(got.nan_count[position], nans, "NaN of field {id}");
    }
    on.batch_execute(&format!(
        "reset search_path; drop schema {} cascade",
        quote_ident(&reference)
    ))
    .await?;

    // The protocol takes the file, and prunes on k, which holds 0 to 14, at its second position.
    let footer_size = stats::footer_size(&file).expect("a Parquet tail");
    let add = got.into_file_add(
        table.id(),
        FileFacts {
            path: Some("memory:///lake/t/a.parquet".into()),
            content: None,
            file_size: file.len() as i64,
            footer_size,
            footer: None,
            spec_id: table.spec_id().unwrap_or(0),
            partition: json!([]),
            sort_order_id: None,
        },
    );
    let pool = Pool::new(&url(), 1)?;
    let s = pool.get().await?;
    let p = Protocol::new(&schema);
    p.commit(
        &s,
        &Commit {
            ref_id: 1,
            operation: "append".into(),
            files: vec![add],
            ..Default::default()
        },
    )
    .await?;
    let id = table.id();
    let planned = |lo: i64, hi: i64| {
        let prune = Prune {
            field_id: 1,
            lo: Some(metalake::bounds::long(lo)),
            hi: Some(metalake::bounds::long(hi)),
        };
        let (p, s) = (&p, &s);
        async move {
            p.plan(s, 1, id, None, Some(&prune))
                .await?
                .try_collect::<Vec<_>>()
                .await
        }
    };
    assert_eq!(planned(5, 7).await?.len(), 1);
    assert_eq!(planned(14, 20).await?.len(), 1);
    assert_eq!(planned(15, 30).await?.len(), 0);
    assert_eq!(planned(-9, -1).await?.len(), 0);
    Ok(())
}

/// The target of §7: a footer of 10⁶ rows in 8 row groups gives its statistics in at most 1 ms. A measurement: run
/// it with `--ignored`, in release.
#[tokio::test]
#[ignore]
async fn statistics_of_a_large_file_cost_little() -> Result<(), Error> {
    let schema = fresh_schema("c3");
    let (_catalog, table) = table(&schema).await?;
    let file = write(&table, 1_000_000, 125_000);
    let reader = SerializedFileReader::new(file.clone())?;
    assert_eq!(reader.metadata().num_row_groups(), 8);
    let mut times = Vec::new();
    for _ in 0..101 {
        let start = Instant::now();
        let s = stats::of_footer(&table, reader.metadata(), &HashMap::new());
        times.push(start.elapsed());
        assert_eq!(s.record_count, 1_000_000);
    }
    times.sort();
    eprintln!(
        "statistics of 10^6 rows, 8 row groups, 10 columns, {} bytes, footer {} bytes: median {:?}, max {:?}",
        file.len(),
        stats::footer_size(&file).unwrap(),
        times[50],
        times[100]
    );
    Ok(())
}
