//! Z4 (ADR 0002 §22.7): expiring costs what was closed, not the size of the catalog. Two catalogs with 10⁴ closed,
//! collectable rows each and 10⁵ and 10⁶ live rows; three batches of `expire(1000)` in each, timed, and whether they
//! scanned `data_file`. A measurement: run it with `--ignored`.

use std::time::Instant;

use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, commit_protocol};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::json;
use tokio_postgres::Client;
use uuid::Uuid;

const CLOSED: i64 = 10_000;
const BATCH: i64 = 1_000;

/// Seeds `live` live rows and CLOSED rows closed below main's floor, all of one table; returns the schema.
async fn seed(on: &Client, live: i64) -> Result<String, Error> {
    let schema = fresh_schema("p");
    install_protocol(on, &schema).await?;
    let p = quote_ident(&schema);
    let t = Uuid::from_u128(0x24);
    for create in [true, false] {
        let c = Commit {
            ref_id: 1,
            operation: "append".into(),
            tables: vec![TableChange {
                table_id: t,
                adds: vec![stored_file(t, format!("gs://lake/seed-{create}.parquet"), 10, 1, 0, 9)],
                definition: create.then(|| {
                    json!({
                        "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
                        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long",
                                    "required": true}],
                        "cluster_key": [1],
                    })
                }),
                ..TableChange::default()
            }],
            app: None,
            namespaces: None,
        };
        if !matches!(commit_protocol(on, &schema, &c).await?.outcome, Outcome::Committed(_)) {
            return Err("seeding was refused".into());
        }
    }
    // Main keeps its latest snapshot only: its floor is the last commit, s1; the closed rows end at s1.
    let (s0, s1): (i64, i64) = {
        let row = on
            .query_one(
                &format!("select min(snapshot_id), max(snapshot_id) from {p}.snapshot"),
                &[],
            )
            .await?;
        (row.get(0), row.get(1))
    };
    on.batch_execute(&format!(
        "insert into {p}.data_file (table_id, file_id, ref_id, begin_seq, end_seq, path, spec_id, partition,
                                    record_count, file_size, footer_size)
         select '{t}', 1000 + g, 1, {s0}, case when g <= {CLOSED} then {s1} else 9223372036854775807 end,
                'gs://lake/f' || g || '.parquet', 0, '[]', 10, 1000, 100
           from generate_series(1, {CLOSED} + {live}) g;
         select {p}.set_retention(1, min_snapshots => 1);
         analyze {p}.data_file"
    ))
    .await?;
    Ok(schema)
}

#[tokio::test]
#[ignore]
async fn expire_cost_proportional_to_closed_rows() -> Result<(), Error> {
    let on = connect().await?;
    let mut lines = Vec::new();
    for live in std::env::var("LIVE").map_or(vec![100_000i64, 1_000_000], |v| vec![v.parse().unwrap()]) {
        let schema = seed(&on, live).await?;
        let p = quote_ident(&schema);
        // Three batches of the default size, each in its own transaction, as the worker runs them.
        let (mut times, mut seq, mut deleted) = (Vec::new(), 0i64, 0i64);
        for _ in 0..3 {
            on.batch_execute("begin").await?;
            let start = Instant::now();
            let k: i64 = on.query_one(&format!("select {p}.expire({BATCH})"), &[]).await?.get(0);
            times.push(start.elapsed());
            let s: i64 = on
                .query_one(
                    "select coalesce(seq_scan, 0) from pg_stat_xact_user_tables where relid = $1::text::regclass",
                    &[&format!("{p}.data_file")],
                )
                .await?
                .get(0);
            on.batch_execute("commit").await?;
            deleted += k;
            seq += s;
        }
        let queued: i64 = on
            .query_one(&format!("select count(*) from {p}.deletion_queue"), &[])
            .await?
            .get(0);
        times.sort();
        let median = times[1];
        lines.push(format!(
            "live {live:>8}: 3 batches of expire({BATCH}) deleted {deleted} rows, queued {queued}; median {median:?}              ({:.0} µs a row; all {times:?}); sequential scans of data_file: {seq}",
            median.as_secs_f64() * 1e6 / BATCH as f64
        ));
        on.batch_execute(&format!("drop schema {p} cascade")).await?;
    }
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
