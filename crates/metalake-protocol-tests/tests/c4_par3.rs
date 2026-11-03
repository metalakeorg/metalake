//! PAR-3 in the protocol: a commit refuses a file whose partition values its statistics contradict, as the
//! reference's does. A table partitioned by identity, day, bucket and truncate; files that agree with their values,
//! and one file for each way to contradict them, each committed to both catalogs, which must answer alike; then the
//! files the core's writer makes, which the protocol takes.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use metalake::bounds;
use metalake::partition::{Scalar, Transform};
use metalake::{Catalog, CatalogConfig, Storage, TableWriter, WriteOptions};
use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog};
use metalake_protocol_tests::records::FileAdd;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use serde_json::{Value, json};
use uuid::Uuid;

const T: u128 = 0x9a3;
const DAY: i64 = 86_400_000_000;

fn table() -> Uuid {
    Uuid::from_u128(T)
}

/// The statistics of a file: the bounds of k, r, at and s, and their null counts.
struct Stats {
    k: (i64, i64),
    r: Option<(&'static str, &'static str)>,
    at: (i64, i64),
    s: (&'static str, &'static str),
    r_nulls: i64,
}

impl Stats {
    fn good() -> Self {
        Self {
            k: (5, 5),
            r: Some(("eu", "eu")),
            at: (20_000 * DAY + 1, 20_000 * DAY + DAY - 1),
            s: ("abcdef", "abczzz"),
            r_nulls: 0,
        }
    }

    /// The partition values that agree with these statistics.
    fn partition(&self) -> Value {
        let r = self.r.map_or(Scalar::Null, |(lo, _)| Scalar::String(lo));
        json!([
            Transform::Identity.apply("string", r).unwrap(),
            Transform::Day.apply("timestamp", Scalar::Long(self.at.0)).unwrap(),
            Transform::Bucket(4).apply("long", Scalar::Long(self.k.0)).unwrap(),
            Transform::Truncate(3)
                .apply("string", Scalar::String(self.s.0))
                .unwrap(),
        ])
    }

    fn file(&self, spec_id: i32, name: &str, partition: Value) -> FileAdd {
        let (r_lo, r_hi) = (self.r.map(|b| bounds::string(b.0)), self.r.map(|b| bounds::string(b.1)));
        FileAdd {
            table_id: table(),
            file_id: None,
            path: Some(format!("gs://lake/p/{name}.parquet")),
            spec_id,
            partition,
            sort_order_id: None,
            record_count: 10,
            file_size: 1000,
            footer_size: 100,
            first_row_id: None,
            stat_field_ids: vec![1, 2, 3, 4],
            lower: vec![
                Some(bounds::long(self.k.0)),
                r_lo,
                Some(bounds::long(self.at.0)),
                Some(bounds::string(self.s.0)),
            ],
            upper: vec![
                Some(bounds::long(self.k.1)),
                r_hi,
                Some(bounds::long(self.at.1)),
                Some(bounds::string(self.s.1)),
            ],
            null_count: vec![Some(0), Some(self.r_nulls), Some(0), Some(0)],
            nan_count: vec![None; 4],
            value_count: None,
            content: None,
            footer: None,
        }
    }
}

fn append(file: FileAdd) -> Commit {
    Commit {
        ref_id: 1,
        operation: "append".into(),
        tables: vec![TableChange {
            table_id: table(),
            adds: vec![file],
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    }
}

#[tokio::test]
async fn a_partition_its_statistics_contradict_is_refused_as_the_reference_refuses_it() -> Result<(), Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let on_protocol = connect().await?;
    let on_reference = connect().await?;
    install_protocol(&on_protocol, &protocol).await?;
    install_reference(&on_protocol, &reference).await?;
    on_reference
        .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
    clone_catalog(&on_protocol, &reference, &protocol).await?;
    let pair = Pair {
        protocol: protocol.clone(),
        reference,
        on_protocol,
        on_reference,
    };

    let field = |id: i32, name: &str, ty: &str| json!({"field_id": id, "new": true, "position": id, "name": name, "type": ty, "required": false});
    let create = Commit {
        ref_id: 1,
        operation: "update".into(),
        tables: vec![TableChange {
            table_id: table(),
            definition: Some(json!({
                "create": {"namespace": ["db"], "name": "p", "location": "memory:///lake/p"},
                "fields": [field(1, "k", "long"), field(2, "r", "string"), field(3, "at", "timestamp"),
                           field(4, "s", "string")],
                "cluster_key": [1],
                "partition_spec": [
                    {"source_ids": [2], "field_id": 1000, "name": "r", "transform": "identity"},
                    {"source_ids": [3], "field_id": 1001, "name": "at_day", "transform": "day"},
                    {"source_ids": [1], "field_id": 1002, "name": "k_bucket", "transform": "bucket[4]"},
                    {"source_ids": [4], "field_id": 1003, "name": "s_trunc", "transform": "truncate[3]"},
                ],
            })),
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    };
    assert!(matches!(pair.commit(&create).await?, Outcome::Committed(_)));
    let spec: i32 = pair
        .on_protocol
        .query_one(
            &format!(
                "select spec_id from {}.partition_spec where table_id = $1 and jsonb_array_length(fields) = 4",
                quote_ident(&protocol)
            ),
            &[&table()],
        )
        .await?
        .get(0);

    let good = Stats::good();
    let committed = |o: &Outcome| matches!(o, Outcome::Committed(_));
    let refused = |o: &Outcome| o == &Outcome::Refused("22023".into());

    // Files whose values agree: one value of k, and a range of k, whose bucket is not checked.
    assert!(committed(
        &pair
            .commit(&append(good.file(spec, "agrees", good.partition())))
            .await?
    ));
    let range = Stats {
        k: (0, 99),
        ..Stats::good()
    };
    let any_bucket = {
        let mut p = range.partition();
        p[2] = json!(3);
        p
    };
    assert!(committed(
        &pair.commit(&append(range.file(spec, "a-range", any_bucket))).await?
    ));
    let no_r = Stats {
        r: None,
        r_nulls: 10,
        ..Stats::good()
    };
    assert!(committed(
        &pair
            .commit(&append(no_r.file(spec, "r-null", no_r.partition())))
            .await?
    ));

    // One file for each way to contradict them.
    let mut null_with_values = good.partition();
    null_with_values[0] = Value::Null;
    let nulls_in_a_value = Stats {
        r_nulls: 2,
        ..Stats::good()
    };
    let two_days = Stats {
        at: (20_000 * DAY, 20_001 * DAY),
        ..Stats::good()
    };
    let mut wrong_bucket = good.partition();
    wrong_bucket[2] = json!((wrong_bucket[2].as_i64().unwrap() + 1) % 4);
    let past_its_prefix = Stats {
        s: ("abdxxx", "abdyyy"),
        ..Stats::good()
    };
    let before_its_prefix = Stats {
        s: ("aaa111", "aab999"),
        ..Stats::good()
    };
    let cases = [
        (
            "a null value for a field with values",
            good.file(spec, "x1", null_with_values),
        ),
        (
            "a value for a field with nulls",
            nulls_in_a_value.file(spec, "x2", good.partition()),
        ),
        (
            "one day for bounds over two",
            two_days.file(spec, "x3", good.partition()),
        ),
        ("another bucket for its one value", good.file(spec, "x4", wrong_bucket)),
        (
            "a prefix its strings do not have",
            past_its_prefix.file(spec, "x5", good.partition()),
        ),
        (
            "a prefix past its strings",
            before_its_prefix.file(spec, "x6", good.partition()),
        ),
    ];
    for (what, file) in cases {
        let answer = pair.commit(&append(file)).await?;
        assert!(refused(&answer), "{what}: {answer:?}");
    }

    // The core's writer, on the protocol's catalog: its files agree with their values, and the protocol takes them.
    let url = std::env::var("DATABASE_URL")?;
    let catalog = Catalog::connect(CatalogConfig::new(url, &protocol)).await?;
    let main = catalog.branch("main").await?;
    let t = main.table(&["db"], "p").await?;
    let n = 200i64;
    let batch = RecordBatch::try_new(
        t.schema().clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..n)),
            Arc::new(StringArray::from_iter(
                (0..n).map(|i| [Some("eu"), Some("us"), None][(i % 3) as usize]),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..n).map(|i| 20_000 * DAY + (i % 5) * DAY + i),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("{}{}", ["abc", "abd", "xyz"][(i % 3) as usize], i)),
            )),
        ],
    )?;
    let mut writer = TableWriter::new(&catalog, &t, Storage::in_memory(), WriteOptions::default())?;
    writer.write(&batch).await?;
    let files = writer.finish().await?;
    assert!(files.len() > 1);
    main.transaction().append(files).commit().await?;
    Ok(())
}
