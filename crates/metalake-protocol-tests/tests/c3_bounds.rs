//! C3a (ADR 0003 §7): the core's bound encoding (STA-5) and truncation (STA-8) against the reference's
//! `encode_bound`, `truncate_lower` and `truncate_upper`, byte for byte, on random values of each type and on fixed
//! edges; and the property the encoding exists for, that bounds order as their values.

use std::cmp::Ordering;

use metalake::bounds::{self, Truncation};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

fn cases() -> usize {
    std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(256)
}

/// `n` values of `strategy`, drawn deterministically, followed by `edges`.
fn draw<T: std::fmt::Debug>(strategy: impl Strategy<Value = T>, edges: Vec<T>) -> Vec<T> {
    let mut runner = TestRunner::deterministic();
    let mut values: Vec<T> = (0..cases())
        .map(|_| strategy.new_tree(&mut runner).unwrap().current())
        .collect();
    values.extend(edges);
    values
}

/// The reference's answer for each value, in order: `sql` maps the array `$1`, aliased `x`, to one `bytea` each.
async fn reference<T: ToSql + Sync>(
    on: &Client,
    array: &str,
    sql: &str,
    values: &[T],
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    let query = format!("select array_agg(({sql}) order by n) from unnest($1::{array}) with ordinality u(x, n)");
    Ok(on
        .query_one(&query, &[&values])
        .await?
        .get::<_, Option<Vec<Option<Vec<u8>>>>>(0)
        .unwrap_or_default())
}

fn same(name: &str, got: Vec<Option<Vec<u8>>>, want: Vec<Option<Vec<u8>>>, values: &[impl std::fmt::Debug]) {
    assert_eq!(got.len(), want.len(), "{name}");
    for ((g, w), v) in got.iter().zip(&want).zip(values) {
        assert_eq!(g, w, "{name} of {v:?}");
    }
}

#[tokio::test]
async fn bound_encoding_matches_reference() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c3");
    install_reference(&on, &schema).await?;
    on.batch_execute(&format!("set search_path = {}", quote_ident(&schema)))
        .await?;

    let longs = draw(any::<i64>(), vec![i64::MIN, -1, 0, 1, i64::MAX]);
    let want = reference(&on, "bigint[]", "encode_bound('long', x::text)", &longs).await?;
    same(
        "long",
        longs.iter().map(|v| Some(bounds::long(*v))).collect(),
        want,
        &longs,
    );

    let ints = draw(any::<i32>(), vec![i32::MIN, 0, i32::MAX]);
    let want = reference(&on, "int[]", "encode_bound('int', x::text)", &ints).await?;
    same(
        "int",
        ints.iter().map(|v| Some(bounds::long(i64::from(*v)))).collect(),
        want,
        &ints,
    );

    // Days Postgres can hold, from 4713 BC.
    let days = draw(-2_440_000i32..2_900_000, vec![-2_440_000, -1, 0, 1]);
    let want = reference(
        &on,
        "int[]",
        "encode_bound('date', (date '1970-01-01' + x)::text)",
        &days,
    )
    .await?;
    same(
        "date",
        days.iter().map(|v| Some(bounds::date(*v))).collect(),
        want,
        &days,
    );

    // Postgres multiplies an interval in float8: the value goes in as whole days and microseconds, both exact.
    let micros = draw(-100_000_000_000_000_000i64..100_000_000_000_000_000, vec![-1, 0, 1]);
    let want = reference(
        &on,
        "bigint[]",
        "encode_bound('timestamp', (timestamp '1970-01-01' + (x / 86400000000) * interval '1 day' + (x % 86400000000) * interval '1 microsecond')::text)",
        &micros,
    )
    .await?;
    same(
        "timestamp",
        micros.iter().map(|v| Some(bounds::long(*v))).collect(),
        want,
        &micros,
    );
    let want = reference(
        &on,
        "bigint[]",
        "encode_bound('timestamptz', (timestamptz '1970-01-01 00:00:00+00' + (x / 86400000000) * interval '1 day' + (x % 86400000000) * interval '1 microsecond')::text)",
        &micros,
    )
    .await?;
    same(
        "timestamptz",
        micros.iter().map(|v| Some(bounds::long(*v))).collect(),
        want,
        &micros,
    );

    let times = draw(0i64..86_400_000_000, vec![0, 86_399_999_999]);
    let want = reference(
        &on,
        "bigint[]",
        "encode_bound('time', (time '00:00' + x * interval '1 microsecond')::text)",
        &times,
    )
    .await?;
    same(
        "time",
        times.iter().map(|v| Some(bounds::long(*v))).collect(),
        want,
        &times,
    );

    let doubles = draw(
        any::<f64>().prop_filter("NaN is never a bound", |v| !v.is_nan()),
        vec![f64::NEG_INFINITY, -0.0, 0.0, f64::MIN_POSITIVE, f64::MAX, f64::INFINITY],
    );
    let want = reference(&on, "float8[]", "encode_bound('double', x::text)", &doubles).await?;
    same(
        "double",
        doubles.iter().map(|v| bounds::double(*v)).collect(),
        want,
        &doubles,
    );

    let floats = draw(
        any::<f32>().prop_filter("NaN is never a bound", |v| !v.is_nan()),
        vec![f32::NEG_INFINITY, -0.0, 0.0, f32::MIN_POSITIVE, f32::MAX],
    );
    let want = reference(&on, "float4[]", "encode_bound('float', x::text)", &floats).await?;
    same(
        "float",
        floats.iter().map(|v| bounds::double(f64::from(*v))).collect(),
        want,
        &floats,
    );

    // Decimals of 38 digits at every scale, as the reference reads them: text.
    let limit = 10i128.pow(38) - 1;
    let decimals = draw(
        (-limit..=limit, 0u32..=38),
        vec![(-limit, 0), (limit, 38), (-1, 38), (0, 5)],
    );
    let texts: Vec<String> = decimals.iter().map(|(u, s)| decimal_text(*u, *s)).collect();
    let scales: Vec<i32> = decimals.iter().map(|(_, s)| *s as i32).collect();
    let want: Vec<Option<Vec<u8>>> = on
        .query_one(
            "select array_agg(encode_bound(format('decimal(38,%s)', s), x) order by n)
               from unnest($1::text[], $2::int[]) with ordinality u(x, s, n)",
            &[&texts, &scales],
        )
        .await?
        .get(0);
    same(
        "decimal",
        decimals.iter().map(|(u, _)| Some(bounds::decimal(*u))).collect(),
        want,
        &texts,
    );

    let uuids: Vec<Uuid> = draw(any::<[u8; 16]>(), vec![[0; 16], [0xFF; 16]])
        .into_iter()
        .map(Uuid::from_bytes)
        .collect();
    let want = reference(&on, "uuid[]", "encode_bound('uuid', x::text)", &uuids).await?;
    same(
        "uuid",
        uuids.iter().map(|v| Some(bounds::bytes(v.as_bytes()))).collect(),
        want,
        &uuids,
    );

    // Strings over every code point Postgres takes (all but NUL), long enough to truncate, with the edges of STA-8.
    let chars = any::<char>().prop_filter("Postgres text holds no NUL", |c| *c != '\0');
    let strings = draw(
        proptest::collection::vec(chars, 0..24).prop_map(|cs| cs.into_iter().collect::<String>()),
        vec![
            "\u{10FFFF}".repeat(17),
            format!("a{}x", "\u{10FFFF}".repeat(15)),
            format!("{}\u{D7FF}x", "a".repeat(15)),
            "exactly sixteen!".into(),
        ],
    );
    let want = reference(&on, "text[]", "encode_bound('string', x)", &strings).await?;
    same(
        "string",
        strings.iter().map(|v| Some(bounds::string(v))).collect(),
        want,
        &strings,
    );
    let want = reference(
        &on,
        "text[]",
        "truncate_lower('string', convert_to(x, 'UTF8'))",
        &strings,
    )
    .await?;
    let got = strings
        .iter()
        .map(|v| Some(bounds::truncate_lower(Truncation::String, v.as_bytes())))
        .collect();
    same("truncated string lower", got, want, &strings);
    let want = reference(
        &on,
        "text[]",
        "truncate_upper('string', convert_to(x, 'UTF8'))",
        &strings,
    )
    .await?;
    let got = strings
        .iter()
        .map(|v| bounds::truncate_upper(Truncation::String, v.as_bytes()))
        .collect();
    same("truncated string upper", got, want, &strings);

    let binaries = draw(
        proptest::collection::vec(prop_oneof![any::<u8>(), Just(0xFF)], 0..24),
        vec![vec![0xFF; 17], vec![0x00; 17], [vec![7; 15], vec![0xFF, 1]].concat()],
    );
    let want = reference(&on, "bytea[]", "encode_bound('binary', encode(x, 'hex'))", &binaries).await?;
    same(
        "binary",
        binaries.iter().map(|v| Some(bounds::bytes(v))).collect(),
        want,
        &binaries,
    );
    let want = reference(&on, "bytea[]", "truncate_lower('binary', x)", &binaries).await?;
    let got = binaries
        .iter()
        .map(|v| Some(bounds::truncate_lower(Truncation::Binary, v)))
        .collect();
    same("truncated binary lower", got, want, &binaries);
    let want = reference(&on, "bytea[]", "truncate_upper('binary', x)", &binaries).await?;
    let got = binaries
        .iter()
        .map(|v| bounds::truncate_upper(Truncation::Binary, v))
        .collect();
    same("truncated binary upper", got, want, &binaries);

    let booleans = vec![false, true];
    let want = reference(&on, "boolean[]", "encode_bound('boolean', x::text)", &booleans).await?;
    same(
        "boolean",
        booleans.iter().map(|v| Some(bounds::boolean(*v))).collect(),
        want,
        &booleans,
    );

    on.batch_execute(&format!(
        "reset search_path; drop schema {} cascade",
        quote_ident(&schema)
    ))
    .await?;
    Ok(())
}

/// The text of a decimal of scale `scale` whose unscaled value is `unscaled`.
fn decimal_text(unscaled: i128, scale: u32) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let digits = format!("{digits:0>width$}", width = scale as usize + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
    let sign = if unscaled < 0 { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}

/// Bounds order as their values: for each type, `encode(a) < encode(b)` exactly when `a < b`.
#[test]
fn bound_order_matches_values() {
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(cases() as u32));
    runner
        .run(&(any::<i64>(), any::<i64>()), |(a, b)| {
            prop_assert_eq!(bounds::long(a).cmp(&bounds::long(b)), a.cmp(&b));
            Ok(())
        })
        .unwrap();
    runner
        .run(&(any::<i32>(), any::<i32>()), |(a, b)| {
            prop_assert_eq!(bounds::date(a).cmp(&bounds::date(b)), a.cmp(&b));
            Ok(())
        })
        .unwrap();
    let finite = || any::<f64>().prop_filter("not NaN", |v| !v.is_nan());
    runner
        .run(&(finite(), finite()), |(a, b)| {
            let (ea, eb) = (bounds::double(a).unwrap(), bounds::double(b).unwrap());
            prop_assert_eq!(ea.cmp(&eb), a.total_cmp(&b));
            Ok(())
        })
        .unwrap();
    runner
        .run(&(any::<i128>(), any::<i128>()), |(a, b)| {
            prop_assert_eq!(bounds::decimal(a).cmp(&bounds::decimal(b)), a.cmp(&b));
            Ok(())
        })
        .unwrap();
    runner
        .run(&(any::<String>(), any::<String>()), |(a, b)| {
            let by_code_point: Ordering = a.chars().cmp(b.chars());
            prop_assert_eq!(bounds::string(&a).cmp(&bounds::string(&b)), by_code_point);
            Ok(())
        })
        .unwrap();
}
