//! Z6: what following a table costs (CHG-1, CHG-3, RET-1), as the table and its history grow. Catalogs with up to 10⁶ files
//! and up to 10⁶ commits, seeded in bulk, and real commits on top: each call timed and its blocks counted. A measurement,
//! to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, commit_protocol};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

const TABLE: u128 = 0x26;
/// Runs of a call, of which the median counts.
const RUNS: usize = 7;
/// Real commits on top of what is seeded.
const REAL: usize = 5;

fn commit_of(t: Uuid, adds: usize, removes: Vec<i64>, definition: Option<Value>) -> Commit {
    let adds = (0..adds)
        .map(|i| stored_file(t, format!("gs://lake/real-{}-{i}.parquet", Uuid::new_v4()), 10, 1, 0, 9))
        .collect();
    Commit {
        ref_id: 1,
        operation: if removes.is_empty() { "append" } else { "delete" }.into(),
        tables: vec![TableChange {
            table_id: t,
            adds,
            removes,
            definition,
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    }
}

async fn real(on: &Client, schema: &str, commit: &Commit) -> Result<i64, Error> {
    match commit_protocol(on, schema, commit).await?.outcome {
        Outcome::Committed(seq) => Ok(seq),
        other => Err(format!("refused: {other:?}").into()),
    }
}

/// A table with `files` files and `history` commits older than the last few.
async fn seed(on: &Client, files: i64, history: i64) -> Result<String, Error> {
    let schema = fresh_schema("z");
    install_protocol(on, &schema).await?;
    let p = quote_ident(&schema);
    let t = Uuid::from_u128(TABLE);
    let definition = json!({
        "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
        "cluster_key": [1],
    });
    let s0 = real(on, &schema, &commit_of(t, 1, vec![], Some(definition))).await?;
    on.batch_execute(&format!(
        "insert into {p}.commit (commit_seq, ref_id, xid, committed_at, author, actor, operation)
         select {s0} + g, 1, pg_current_xact_id(), now() - interval '1 day' + g * interval '1 millisecond', 'z', 'z', 'append'
           from generate_series(1, {history}) g;
         insert into {p}.snapshot (table_id, snapshot_id, ref_id, parent_snapshot_id, schema_seq, spec_id, sort_order_id, summary)
         select s.table_id, {s0} + g, 1, {s0} + g - 1, s.schema_seq, s.spec_id, s.sort_order_id, s.summary
           from {p}.snapshot s, generate_series(1, {history}) g where s.snapshot_id = {s0};
         update {p}.head set snapshot_id = {s0} + {history} where table_id = '{t}';
         update {p}.commit set committed_at = now() - interval '2 days' where commit_seq <= {s0};
         select setval('{p}.commit_seq_gen', {s0} + {history});
         insert into {p}.data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size, footer_size)
         select '{t}', 1000000 + g, 1, {s0}, 'gs://lake/f' || g || '.parquet', 0, '[]', 10, 1000, 100
           from generate_series(1, {files}) g;
         analyze {p}.data_file; analyze {p}.snapshot; analyze {p}.commit;"
    ))
    .await?;
    Ok(schema)
}

/// The median time of a statement, in ms, and the blocks it touched.
async fn cost(on: &Client, sql: &str) -> Result<(f64, i64), Error> {
    let mut times = Vec::new();
    let mut blocks = 0;
    for _ in 0..RUNS {
        let plan = on
            .query_one(&format!("explain (analyze, buffers, format json) {sql}"), &[])
            .await?;
        let plan: Value = plan.get(0);
        let top = &plan[0];
        times.push(top["Execution Time"].as_f64().unwrap_or(0.0));
        let of = |key: &str| top["Plan"][key].as_i64().unwrap_or(0);
        blocks = of("Shared Hit Blocks") + of("Shared Read Blocks");
    }
    times.sort_by(f64::total_cmp);
    Ok((times[RUNS / 2], blocks))
}

#[tokio::test]
#[ignore]
async fn stream_cost_by_table_and_history() -> Result<(), Error> {
    let on = connect().await?;
    let t = Uuid::from_u128(TABLE);
    let mut lines = vec![format!(
        "{:>9} {:>9} | {:<30} {:>10} {:>9}",
        "files", "commits", "call", "ms", "blocks"
    )];
    for (files, history) in [(1_000i64, 1_000i64), (1_000_000, 1_000), (1_000, 1_000_000)] {
        let schema = seed(&on, files, history).await?;
        let p = quote_ident(&schema);
        let mut last = 0;
        for _ in 0..REAL {
            last = real(&on, &schema, &commit_of(t, 1, vec![], None)).await?;
        }
        let removed = real(&on, &schema, &commit_of(t, 0, vec![1_000_001], None)).await?;
        on.batch_execute(&format!("select {p}.set_retention(1, min_seq => 0)"))
            .await?;
        let t_sql = format!("'{t}'::uuid");
        let calls = [
            (
                "commit_changes, one added",
                format!("select * from {p}.commit_changes(1, {t_sql}, {last})"),
            ),
            (
                "commit_changes, one removed",
                format!("select * from {p}.commit_changes(1, {t_sql}, {removed})"),
            ),
            (
                "the feed, nothing new",
                format!("select * from {p}.changes('infinity', 0, 1000, {t_sql})"),
            ),
            (
                "the feed, 1000 behind",
                format!("select * from {p}.changes('-infinity', 0, 1000, {t_sql})"),
            ),
        ];
        for (name, sql) in calls {
            let (ms, blocks) = cost(&on, &sql).await?;
            lines.push(format!("{files:>9} {history:>9} | {name:<30} {ms:>10.3} {blocks:>9}"));
        }
        for (name, retention) in [
            ("floor, min_seq", "min_seq => 0"),
            ("floor, min_snapshots 10", "min_snapshots => 10"),
            ("floor, max_age 7 days", "max_age => interval '7 days'"),
            ("floor, max_age 1 hour", "max_age => interval '1 hour'"),
        ] {
            on.batch_execute(&format!(
                "delete from {p}.retention; select {p}.set_retention(1, {retention})"
            ))
            .await?;
            let (ms, blocks) = cost(&on, &format!("select {p}.retention_floor(1)")).await?;
            lines.push(format!("{files:>9} {history:>9} | {name:<30} {ms:>10.3} {blocks:>9}"));
        }
        on.batch_execute(&format!("drop schema {p} cascade")).await?;
    }
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
