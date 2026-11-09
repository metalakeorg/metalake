//! Z11: what reading one table's definition costs (`describe`), as its history grows: `Ref::table` asks it on every
//! open, and a first load through Iceberg twice a snapshot. The call whole, and each of its parts alone, at the latest
//! sequence and at an old one. A measurement, to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::cost::{TABLE, cost, seed};
use metalake_protocol_tests::{Error, connect, quote_ident};
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn describe_cost_by_history() -> Result<(), Error> {
    let on = connect().await?;
    let t = Uuid::from_u128(TABLE);
    let mut lines = vec![format!(
        "{:>7} | {:<28} {:>10} {:>9}",
        "history", "call", "ms", "blocks"
    )];
    for history in [10i64, 2_000, 100_000] {
        let schema = seed(&on, 10, history).await?;
        let p = quote_ident(&schema);
        let head: i64 = on
            .query_one(&format!("select max(snapshot_id) from {p}.snapshot"), &[])
            .await?
            .get(0);
        let old = head - history / 2;
        // the parts name the protocol's tables unqualified, as describe's own search_path has them
        on.batch_execute(&format!("set search_path = {p}, pg_catalog")).await?;
        let parts = |at: i64| {
            vec![
                ("describe", format!("select {p}.describe(1, '{t}', {at})")),
                (
                    "table_named",
                    format!("select * from {p}.table_named(1, array['db'], 't', {at})"),
                ),
                ("snapshot_at", format!("select {p}.snapshot_at(1, '{t}', {at})")),
                (
                    "visible_table_names",
                    format!("select * from {p}.visible_table_names(1, {at})"),
                ),
                (
                    "visible_fields",
                    format!("select * from {p}.visible_fields(1, '{t}', {at})"),
                ),
                (
                    "default_spec",
                    format!("select * from {p}.default_spec(1, '{t}', {at})"),
                ),
                (
                    "default_sort_order",
                    format!("select * from {p}.default_sort_order(1, '{t}', {at})"),
                ),
                (
                    "visible_cluster_key",
                    format!("select * from {p}.visible_cluster_key(1, '{t}', {at})"),
                ),
                (
                    "visible_properties",
                    format!("select * from {p}.visible_properties(1, '{t}', {at})"),
                ),
                ("select 1 (floor)", "select 1".to_string()),
            ]
        };
        // describe's body as a statement of its own: what planning it takes, apart from running it
        let body: String = on
            .query_one(
                "select p.prosrc from pg_proc p join pg_namespace n on n.oid = p.pronamespace
                  where p.proname = 'describe' and n.nspname = $1",
                &[&schema],
            )
            .await?
            .get(0);
        let body = body
            .replace("$1", "1")
            .replace("$2", &format!("'{t}'::uuid"))
            .replace("$3", &head.to_string());
        for _ in 0..3 {
            let plan: serde_json::Value = on
                .query_one(&format!("explain (analyze, format json) {body}"), &[])
                .await?
                .get(0);
            let (planning, execution) = (&plan[0]["Planning Time"], &plan[0]["Execution Time"]);
            lines.push(format!(
                "{history:>7} | describe's body: planning {planning} ms, execution {execution} ms"
            ));
        }
        for (when, at) in [("head", head), ("old", old)] {
            for (name, sql) in parts(at) {
                let label = format!("{name} @{when}");
                match cost(&on, &sql).await {
                    Ok((ms, blocks)) => lines.push(format!("{history:>7} | {label:<28} {ms:>10.3} {blocks:>9}")),
                    Err(e) => lines.push(format!("{history:>7} | {label:<28} failed: {e}")),
                }
            }
        }
        on.batch_execute(&format!("reset search_path; drop schema {p} cascade"))
            .await?;
    }
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
