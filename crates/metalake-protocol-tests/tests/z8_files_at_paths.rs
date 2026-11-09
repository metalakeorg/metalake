//! Z8: what an Iceberg commit pays to find the files it names (`files_at_paths`, ICE-6), as the table grows. Tables of
//! 10⁴ to 10⁶ files, seeded in bulk; ten paths and ten ids asked, each call timed and its blocks counted. A
//! measurement, to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::cost::{TABLE, cost, seed};
use metalake_protocol_tests::{Error, connect, quote_ident};
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn files_at_paths_cost_by_table() -> Result<(), Error> {
    let on = connect().await?;
    let t = Uuid::from_u128(TABLE);
    let mut lines = vec![format!("{:>9} | {:<24} {:>10} {:>9}", "files", "call", "ms", "blocks")];
    let sizes: &[i64] = if std::env::var("Z8_PLAN").is_ok() {
        &[100_000]
    } else {
        &[10_000, 100_000, 1_000_000]
    };
    for &files in sizes {
        let schema = seed(&on, files, 1_000).await?;
        let p = quote_ident(&schema);
        let paths: Vec<String> = (1..=10)
            .map(|i| format!("'gs://lake/f{}.parquet'", i * files / 10))
            .collect();
        let ids: Vec<String> = (1..=10).map(|i| (1_000_000 + i * files / 10).to_string()).collect();
        let calls = [
            (
                "ten paths",
                format!("array[{}]::text[], '{{}}'::bigint[]", paths.join(",")),
            ),
            ("ten ids", format!("'{{}}'::text[], array[{}]::bigint[]", ids.join(","))),
        ];
        for (name, arguments) in calls {
            let sql = format!("select * from {p}.files_at_paths(1, '{t}'::uuid, {arguments})");
            let (ms, blocks) = cost(&on, &sql).await?;
            lines.push(format!("{files:>9} | {name:<24} {ms:>10.3} {blocks:>9}"));
            if std::env::var("Z8_PLAN").is_ok() {
                for row in on.query(&format!("explain {sql}"), &[]).await? {
                    lines.push(format!("      {}", row.get::<_, String>(0)));
                }
            }
        }
        on.batch_execute(&format!("drop schema {p} cascade")).await?;
    }
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
