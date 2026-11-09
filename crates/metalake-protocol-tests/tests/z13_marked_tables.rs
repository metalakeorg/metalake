//! Z13: what a follower of the feed pays to know what to do with a step of it, as a catalog's tables grow: the tables
//! each commit made a snapshot of (`commit_tables`), and the tables marked (`tables_with_property`). Catalogs of 10 to
//! 10⁴ tables on main, each with a property of its own, two of them marked; each call timed and its blocks counted. A
//! measurement, to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::cost::cost;
use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, commit_protocol};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::json;
use uuid::Uuid;

/// Tables a commit creates at most.
const PER_COMMIT: usize = 500;
const KEY: &str = "metalake.iceberg.metadata";

fn table(i: usize) -> TableChange {
    let value = if i < 2 { "on-commit" } else { "on-load" };
    TableChange {
        table_id: Uuid::from_u128(0x9000_0000 + i as u128),
        definition: Some(json!({
            "create": {"namespace": ["db"], "name": format!("t{i}"), "location": format!("gs://lake/t{i}")},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
            "properties": {KEY: value, "owner": format!("o{i}")},
        })),
        ..TableChange::default()
    }
}

#[tokio::test]
#[ignore]
async fn marked_tables_cost_by_tables() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("z");
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    let mut lines = vec![format!("{:>7} | {:<34} {:>10} {:>9}", "tables", "call", "ms", "blocks")];
    let mut made = 0;
    for tables in [10usize, 100, 1_000, 10_000] {
        while made < tables {
            let next = (made + PER_COMMIT).min(tables);
            let commit = Commit {
                ref_id: 1,
                operation: "update".into(),
                tables: (made..next).map(table).collect(),
                app: None,
                namespaces: None,
            };
            match commit_protocol(&on, &schema, &commit).await?.outcome {
                Outcome::Committed(_) => {}
                other => return Err(format!("refused: {other:?}").into()),
            }
            made = next;
        }
        on.batch_execute(&format!(
            "analyze {p}.table_property; analyze {p}.table_name; analyze {p}.snapshot"
        ))
        .await?;
        let calls = [
            (
                "tables_with_property",
                format!("select * from {p}.tables_with_property('{KEY}', 'on-commit')"),
            ),
            (
                "commit_tables, the last 1000",
                format!(
                    "select * from {p}.commit_tables(array(select snapshot_id from {p}.snapshot order by snapshot_id desc limit 1000))"
                ),
            ),
        ];
        for (name, sql) in calls {
            let (ms, blocks) = cost(&on, &sql).await?;
            lines.push(format!("{tables:>7} | {name:<34} {ms:>10.3} {blocks:>9}"));
        }
    }
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
