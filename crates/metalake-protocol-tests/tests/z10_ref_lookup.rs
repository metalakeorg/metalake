//! Z10: what finding one ref by its name costs (`Catalog::branch`, `Catalog::tag`), as a catalog's refs grow: every
//! call that names a ref pays it, and a load through Iceberg once per ref. Catalogs of 10 to 10³ branches; each call
//! timed and its blocks counted. A measurement, to read: run it with `--ignored --nocapture`.

use metalake_pg::{CreateRef, Protocol};
use metalake_protocol_tests::cost::cost;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};

#[tokio::test]
#[ignore]
async fn ref_lookup_cost_by_refs() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("z");
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    let protocol = Protocol::new(&schema);
    let named = on
        .query_opt(
            "select 1 from pg_proc p join pg_namespace n on n.oid = p.pronamespace
              where p.proname = 'ref_named' and n.nspname = $1",
            &[&schema],
        )
        .await?
        .is_some();
    let mut lines = vec![format!(
        "{:>6} | {:<30} {:>10} {:>9} {:>16}",
        "refs", "call", "ms", "blocks", "a load, every ref"
    )];
    let mut made = 0;
    for refs in [10usize, 100, 1_000] {
        while made < refs {
            protocol
                .create_ref(&on, &CreateRef::branch(format!("b{made}"), 1))
                .await?;
            made += 1;
        }
        on.batch_execute(&format!("analyze {p}.ref; analyze {p}.ref_base"))
            .await?;
        let last = refs - 1;
        // what Protocol::refs asks, which Catalog::branch and tag filter in Rust
        let today = format!(
            "select ref_id, name, kind, protected, parent, fork_seq, created_seq from {p}.refs order by ref_id"
        );
        let mut calls = vec![("all refs, then one (today)", today)];
        if named {
            calls.push(("ref_named", format!("select * from {p}.ref_named('b{last}')")));
        }
        for (name, sql) in calls {
            let (ms, blocks) = cost(&on, &sql).await?;
            // a load through Iceberg finds each ref by its name
            let load = ms * refs as f64;
            lines.push(format!("{refs:>6} | {name:<30} {ms:>10.3} {blocks:>9} {load:>13.1} ms"));
        }
    }
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    for l in &lines {
        eprintln!("{l}");
    }
    Ok(())
}
