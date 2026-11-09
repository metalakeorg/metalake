//! Z6: what following a table costs (CHG-1, CHG-3, RET-1), as the table and its history grow. Catalogs with up to 10⁶ files
//! and up to 10⁶ commits, seeded in bulk, and real commits on top: each call timed and its blocks counted. A measurement,
//! to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::cost::{TABLE, commit_of, cost, real, seed};
use metalake_protocol_tests::{Error, connect, quote_ident};
use uuid::Uuid;

/// Real commits on top of what is seeded.
const REAL: usize = 5;

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
