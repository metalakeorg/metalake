//! C4c (ADR 0003 §7): the core's partition transforms (PAR-1) against the reference's `transform_value`, value for
//! value, on random values of each type and every transform that applies to it.

use metalake::partition::{Scalar, Transform};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::Value;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

fn cases() -> usize {
    std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(256)
}

fn draw<T: std::fmt::Debug>(strategy: impl Strategy<Value = T>, edges: Vec<T>) -> Vec<T> {
    let mut runner = TestRunner::deterministic();
    let mut values: Vec<T> = (0..cases())
        .map(|_| strategy.new_tree(&mut runner).unwrap().current())
        .collect();
    values.extend(edges);
    values
}

/// The reference's partition value of each value: `text` maps `$1`'s element `x` to the text the reference takes.
async fn reference<T: ToSql + Sync>(
    on: &Client,
    ty: &str,
    transform: &str,
    array: &str,
    text: &str,
    values: &[T],
) -> Result<Vec<Value>, Error> {
    let query = format!(
        "select array_agg(transform_value($2, $3, {text}) order by n) from unnest($1::{array}) with ordinality u(x, n)"
    );
    Ok(on.query_one(&query, &[&values, &ty, &transform]).await?.get(0))
}

/// Holds the core's transform of every value to the reference's.
async fn check<T: ToSql + Sync + std::fmt::Debug>(
    on: &Client,
    ty: &str,
    transforms: &[&str],
    array: &str,
    text: &str,
    values: &[T],
    scalar: impl Fn(&T) -> Scalar<'_>,
) -> Result<(), Error> {
    for transform in transforms {
        let want = reference(on, ty, transform, array, text, values).await?;
        let t = Transform::parse(transform)?;
        for (v, w) in values.iter().zip(&want) {
            let got = t.apply(ty, scalar(v))?;
            assert_eq!(&got, w, "{transform} of the {ty} {v:?}");
        }
    }
    Ok(())
}

const DAY_MICROS: &str = "(x / 86400000000) * interval '1 day' + (x % 86400000000) * interval '1 microsecond'";

#[tokio::test]
async fn transforms_match_reference() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c4");
    install_reference(&on, &schema).await?;
    on.batch_execute(&format!("set search_path = {}", quote_ident(&schema)))
        .await?;
    let numbers = [
        "identity",
        "bucket[1]",
        "bucket[16]",
        "bucket[1000]",
        "truncate[1]",
        "truncate[7]",
        "truncate[10]",
    ];

    let longs = draw(-(1i64 << 62)..(1i64 << 62), vec![-1, 0, 1, -10, 10]);
    check(&on, "long", &numbers, "bigint[]", "x::text", &longs, |v| {
        Scalar::Long(*v)
    })
    .await?;
    let ints = draw(any::<i32>(), vec![i32::MIN, -1, 0, 34, i32::MAX - 10]);
    check(&on, "int", &numbers, "int[]", "x::text", &ints, |v| {
        Scalar::Long(i64::from(*v))
    })
    .await?;

    // Days Postgres can hold, from 4713 BC.
    let days = draw(-2_440_000i32..2_900_000, vec![-1, 0, 1, 17_486]);
    let calendar = ["year", "month", "day", "bucket[16]"];
    check(
        &on,
        "date",
        &calendar,
        "int[]",
        "(date '1970-01-01' + x)::text",
        &days,
        |v| Scalar::Long(i64::from(*v)),
    )
    .await?;

    // Postgres multiplies an interval in float8: the value goes in as whole days and microseconds, both exact.
    let micros = draw(
        -100_000_000_000_000_000i64..100_000_000_000_000_000,
        vec![-1, 0, 1, 1_510_871_468_000_000],
    );
    let clock = ["year", "month", "day", "hour", "bucket[16]"];
    let ts = format!("(timestamp '1970-01-01' + {DAY_MICROS})::text");
    check(&on, "timestamp", &clock, "bigint[]", &ts, &micros, |v| Scalar::Long(*v)).await?;
    let tstz = format!("(timestamptz '1970-01-01 00:00:00+00' + {DAY_MICROS})::text");
    check(&on, "timestamptz", &clock, "bigint[]", &tstz, &micros, |v| {
        Scalar::Long(*v)
    })
    .await?;

    // Decimals of 38 digits at every scale, as text.
    let limit = 10i128.pow(37);
    for scale in [0u32, 2, 9, 38] {
        let unscaled = draw(-limit..limit, vec![-1, 0, 1, 1420]);
        let texts: Vec<String> = unscaled.iter().map(|u| decimal_text(*u, scale)).collect();
        let ty = format!("decimal(38,{scale})");
        let pairs: Vec<(String, i128)> = texts.iter().cloned().zip(unscaled.iter().copied()).collect();
        for transform in ["bucket[16]", "truncate[3]", "truncate[100]"] {
            let want = reference(&on, &ty, transform, "text[]", "x", &texts).await?;
            let t = Transform::parse(transform)?;
            for ((text, u), w) in pairs.iter().zip(&want) {
                assert_eq!(&t.apply(&ty, Scalar::Decimal(*u))?, w, "{transform} of the {ty} {text}");
            }
        }
    }

    let chars = any::<char>().prop_filter("Postgres text holds no NUL", |c| *c != '\0');
    let strings = draw(
        proptest::collection::vec(chars, 0..24).prop_map(|cs| cs.into_iter().collect::<String>()),
        vec!["iceberg".into(), String::new(), "ñandú".into()],
    );
    let text = ["identity", "bucket[16]", "truncate[3]", "truncate[16]"];
    check(&on, "string", &text, "text[]", "x", &strings, |v| Scalar::String(v)).await?;

    let binaries = draw(
        proptest::collection::vec(any::<u8>(), 0..24),
        vec![vec![0, 1, 2, 3], vec![]],
    );
    check(
        &on,
        "binary",
        &["bucket[16]", "truncate[3]"],
        "bytea[]",
        "encode(x, 'hex')",
        &binaries,
        |v| Scalar::Bytes(v),
    )
    .await?;

    let uuids: Vec<Uuid> = draw(any::<[u8; 16]>(), vec![])
        .into_iter()
        .map(Uuid::from_bytes)
        .collect();
    check(
        &on,
        "uuid",
        &["bucket[16]", "bucket[1000]"],
        "uuid[]",
        "x::text",
        &uuids,
        |v| Scalar::Bytes(v.as_bytes()),
    )
    .await?;

    let booleans = vec![false, true];
    check(&on, "boolean", &["identity"], "boolean[]", "x::text", &booleans, |v| {
        Scalar::Boolean(*v)
    })
    .await?;

    on.batch_execute(&format!(
        "reset search_path; drop schema {} cascade",
        quote_ident(&schema)
    ))
    .await?;
    Ok(())
}

fn decimal_text(unscaled: i128, scale: u32) -> String {
    let digits = format!("{:0>width$}", unscaled.unsigned_abs(), width = scale as usize + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
    let sign = if unscaled < 0 { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}
