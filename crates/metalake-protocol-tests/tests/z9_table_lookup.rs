//! Z9: what finding one table by its name costs (`Ref::table`), as a catalog's tables grow: every path that opens a table
//! by name pays it — the Iceberg service on each request, DataFusion on each query, a sink's commit, and maintenance once
//! per table. Catalogs of 10 to 10⁴ tables on main, created through the protocol; each call timed and its blocks counted.
//! A measurement, to read: run it with `--ignored --nocapture`.

use metalake_protocol_tests::cost::cost;
use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, commit_protocol};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::json;
use uuid::Uuid;

/// Tables a commit creates at most.
const PER_COMMIT: usize = 500;

fn table(i: usize) -> TableChange {
    TableChange {
        table_id: Uuid::from_u128(0x9000_0000 + i as u128),
        definition: Some(json!({
            "create": {"namespace": ["db"], "name": format!("t{i}"), "location": format!("gs://lake/t{i}")},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
        })),
        ..TableChange::default()
    }
}

async fn create(on: &tokio_postgres::Client, schema: &str, tables: std::ops::Range<usize>) -> Result<(), Error> {
    let commit = Commit {
        ref_id: 1,
        operation: "update".into(),
        tables: tables.map(table).collect(),
        app: None,
        namespaces: None,
    };
    let answer = commit_protocol(on, schema, &commit).await?;
    match answer.outcome {
        Outcome::Committed(_) => Ok(()),
        other => Err(format!("refused: {other:?} {:?}", answer.message).into()),
    }
}

#[tokio::test]
#[ignore]
async fn table_lookup_cost_by_tables() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("z");
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    let named = on
        .query_opt(
            "select 1 from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.proname = 'table_named' and n.nspname = $1",
            &[&schema],
        )
        .await?
        .is_some();
    let mut lines = vec![format!(
        "{:>7} | {:<34} {:>10} {:>9} {:>14}",
        "tables", "call", "ms", "blocks", "maintenance s"
    )];
    let mut made = 0;
    for tables in [10usize, 100, 1_000, 10_000] {
        while made < tables {
            let next = (made + PER_COMMIT).min(tables);
            create(&on, &schema, made..next).await?;
            made = next;
        }
        on.batch_execute(&format!("analyze {p}.table_name; analyze {p}.snapshot"))
            .await?;
        let last = tables - 1;
        let mut calls = vec![(
            "all tables, then one (today)",
            format!("select * from {p}.tables_latest(array[1]) where name = 't{last}'"),
        )];
        if named {
            calls.push((
                "table_named",
                format!("select * from {p}.table_named(1, array['db'], 't{last}')"),
            ));
        }
        for (name, sql) in calls {
            let (ms, blocks) = cost(&on, &sql).await?;
            // a pass of maintenance opens every table by name: as many lookups as tables
            let pass = ms * tables as f64 / 1000.0;
            lines.push(format!("{tables:>7} | {name:<34} {ms:>10.3} {blocks:>9} {pass:>14.2}"));
        }
    }
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
