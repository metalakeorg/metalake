//! C1c (ADR 0003 §7): what a call of the SDK costs over its function's own time, which `pg_stat_statements` measures
//! in the server, and over the round trip. Commits of one file to one table and reads of a table's snapshot, from a pool's client, which
//! prepares each statement once. A measurement: run it with `--ignored`, and
//! `PGARGS="-c shared_preload_libraries=pg_stat_statements"`.

use std::time::Instant;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use metalake_pg::{Commit, Protocol};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

const CALLS: usize = 300;

#[tokio::test]
#[ignore]
async fn a_call_costs_little_over_its_function() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    on.batch_execute("create extension if not exists pg_stat_statements")
        .await?;

    let config: tokio_postgres::Config = std::env::var("DATABASE_URL")?.parse()?;
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager).max_size(1).build()?;
    let client = pool.get().await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc0);
    let commit = |i: usize| Commit {
        ref_id: 1,
        operation: "append".into(),
        files: vec![stored_file(t, format!("gs://lake/f{i}.parquet"), 10, 1, 0, 9)],
        changes: (i == 0).then(|| {
            json!({ t.to_string(): {
                "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
                "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
                "cluster_key": [1],
            }})
        }),
        ..Default::default()
    };
    // The table, and a warm-up: the statements are prepared and the server's caches filled.
    for i in 0..20 {
        p.commit(&client, &commit(i)).await?;
        p.snapshot_at(&client, 1, t, None).await?;
    }
    on.batch_execute("select pg_stat_statements_reset()").await?;

    // The round trip alone: a prepared `select 1` on the same connection.
    let one = client.prepare_cached("select 1").await?;
    let start = Instant::now();
    for _ in 0..CALLS {
        client.query_one(&one, &[]).await?;
    }
    let trip_ms = start.elapsed().as_secs_f64() * 1e3 / CALLS as f64;
    // A transaction that writes, and nothing else: the cost of committing it.
    let write = client.prepare_cached("select pg_current_xact_id()::text").await?;
    let start = Instant::now();
    for _ in 0..CALLS {
        client.query_one(&write, &[]).await?;
    }
    let write_ms = start.elapsed().as_secs_f64() * 1e3 / CALLS as f64 - trip_ms;
    let mut lines = vec![
        format!("  round trip: {trip_ms:.3} ms (a prepared select 1)"),
        format!("  committing: {write_ms:.3} ms over it (a transaction that only takes an xid)"),
    ];
    for call in ["commit", "snapshot_at"] {
        let start = Instant::now();
        for i in 0..CALLS {
            if call == "commit" {
                p.commit(&client, &commit(1000 + i)).await?;
            } else {
                p.snapshot_at(&client, 1, t, None).await?;
            }
        }
        let client_ms = start.elapsed().as_secs_f64() * 1e3 / CALLS as f64;
        let server_ms: f64 = on
            .query_one(
                "select coalesce(sum(total_exec_time + total_plan_time) / nullif(sum(calls), 0), 0)::float8 from pg_stat_statements
                  where query like '%.' || $1 || '(%' and query not like '%pg_stat_statements%'",
                &[&call],
            )
            .await?
            .get(0);
        lines.push(format!(
            "{call:>12}: {client_ms:.3} ms a call, {server_ms:.3} ms in the function, {:.3} ms over it and the              round trip",
            client_ms - server_ms - trip_ms
        ));
    }
    on.batch_execute(&format!("drop schema {} cascade", quote_ident(&schema)))
        .await?;
    for line in &lines {
        eprintln!("{line}");
    }
    Ok(())
}
