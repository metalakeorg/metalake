//! R6: time and changes (§14 of the text) against the reference. One protocol catalog takes real commits: on two
//! tables of main and one of a branch, one that draws its sequence early and commits late (TT-2), some filled by the
//! filler and some not (TT-1). The reference then gets a copy of it, with the same transactions, and both answer
//! the feed from every cursor and a read as of every instant around a commit alike.

use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, clone_catalog, commit_protocol};
use metalake_protocol_tests::records::{VectorSet, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use serde_json::json;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

const TABLES: [u128; 3] = [0xc001, 0xc002, 0xc003];

async fn commit(client: &Client, schema: &str, r: i32, t: usize, n: usize, create: bool) -> Result<(), Error> {
    let table_id = Uuid::from_u128(TABLES[t]);
    let definition = create.then(|| {
        json!({
            "create": {"namespace": ["db"], "name": format!("t{t}"), "location": "gs://lake"},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
            "cluster_key": [1],
        })
    });
    let c = Commit {
        ref_id: r,
        operation: if create { "create" } else { "append" }.into(),
        tables: vec![TableChange {
            table_id,
            adds: vec![stored_file(table_id, format!("gs://lake/{t}-{n}.parquet"), 10, 1, 0, 9)],
            definition,
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    };
    match commit_protocol(client, schema, &c).await?.outcome {
        Outcome::Committed(_) => Ok(()),
        refused => Err(format!("commit {n} on table {t}: {refused:?}").into()),
    }
}

#[tokio::test]
async fn the_feed_and_reads_as_of_a_time_match_the_reference() -> Result<(), Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let (on, slow) = (connect().await?, connect().await?);
    install_protocol(&on, &protocol).await?;
    install_reference(&on, &reference).await?;
    let p = quote_ident(&protocol);
    for t in 0..2 {
        commit(&on, &protocol, 1, t, 0, true).await?;
    }
    let b: i64 = on
        .query_one(&format!("select {p}.create_ref(name => 'b', parent => 1)::bigint"), &[])
        .await?
        .get(0);
    let b = b as i32;
    commit(&on, &protocol, b, 2, 0, true).await?;
    // The slow commit draws its sequence now and commits after the others.
    slow.batch_execute("begin").await?;
    commit(&slow, &protocol, 1, 1, 1, false).await?;
    for n in 1..6 {
        commit(&on, &protocol, 1, 0, n, false).await?;
        commit(&on, &protocol, b, 2, n, false).await?;
    }
    slow.batch_execute("commit").await?;
    // Filled after the slow commit, so that the filled commits are out of commit order; two of them share a time,
    // which only the cursor's sequence tells apart; the last two are left to the transaction's timestamp.
    on.query_one(&format!("select {p}.fill_committed_at()"), &[]).await?;
    on.batch_execute(&format!(
        "update {p}.commit set committed_at = (select committed_at from {p}.commit where commit_seq = 2)
          where commit_seq = 3"
    ))
    .await?;
    commit(&on, &protocol, 1, 1, 2, false).await?;
    commit(&on, &protocol, 1, 0, 6, false).await?;
    // CHG-2: on table 0, a file removed and a vector on another.
    let t0 = Uuid::from_u128(TABLES[0]);
    let ids: Vec<i64> = on
        .query(&format!("select file_id from {p}.plan(1, $1) order by file_id"), &[&t0])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let change = |c: TableChange| Commit {
        ref_id: 1,
        operation: "delete".into(),
        tables: vec![c],
        app: None,
        namespaces: None,
    };
    for c in [
        TableChange {
            table_id: t0,
            removes: vec![ids[0]],
            ..TableChange::default()
        },
        TableChange {
            table_id: t0,
            vectors: vec![VectorSet {
                table_id: t0,
                file_id: ids[1],
                base_seq: None,
                path: Some("gs://dv/0.puffin".into()),
                content_offset: Some(4),
                content_size: 40,
                cardinality: 1,
                inline: None,
            }],
            ..TableChange::default()
        },
    ] {
        if !matches!(
            commit_protocol(&on, &protocol, &change(c)).await?.outcome,
            Outcome::Committed(_)
        ) {
            return Err("the changes of table 0 were refused".into());
        }
    }
    on.batch_execute("select pg_sleep(1.1)").await?;
    clone_catalog(&on, &protocol, &reference).await?;
    let r = quote_ident(&reference);
    // The reference resolves its functions through the search_path; the protocol's are qualified.
    on.batch_execute(&format!("set search_path = {r}")).await?;

    // The slow commit is out of commit order, and the feed has filled commits and commits not yet filled.
    let row = on
        .query_one(
            &format!(
                "select exists (select 1 from {p}.commit x, {p}.commit y where x.commit_seq < y.commit_seq
                                  and {r}.committed_time(x.commit_seq) > {r}.committed_time(y.commit_seq)),
                        (select count(*) from {p}.commit where committed_at is null)"
            ),
            &[],
        )
        .await?;
    let (late, unfilled): (bool, i64) = (row.get(0), row.get(1));
    if !late || unfilled == 0 {
        return Err(format!("the scene lacks a late commit ({late}) or unfilled commits ({unfilled})").into());
    }

    // CHG-1: from the start and from every commit delivered, with every limit, both answer alike.
    let feed = |s: &str| format!("select commit_seq, ref_id, committed_at, operation from {s}.changes($1, $2, $3)");
    let all = on.query(&feed(&r), &[&chrono_min(), &0i64, &1000i32]).await?;
    let mut cursors = vec![(chrono_min(), 0i64)];
    cursors.extend(all.iter().map(|row| (row.get::<_, Timestamp>(2), row.get::<_, i64>(0))));
    for (at, seq) in &cursors {
        for lim in [1i32, 2, 1000] {
            let want = rows(on.query(&feed(&r), &[at, seq, &lim]).await?);
            let got = rows(on.query(&feed(&p), &[at, seq, &lim]).await?);
            if want != got {
                return Err(format!("changes({at:?}, {seq}, {lim}): reference {want:?}, protocol {got:?}").into());
            }
        }
    }
    let total: i64 = on
        .query_one(&format!("select count(*) from {p}.commit"), &[])
        .await?
        .get(0);
    if all.len() as i64 != total {
        return Err(format!("the feed delivered {} of {total} commits", all.len()).into());
    }

    // TT-3: every table on both refs, as of each commit's time and the instants either side of it.
    let at = |s: &str| format!("select {s}.as_of_time($1, $2, $3)");
    for row in &all {
        let when: Timestamp = row.get(2);
        for instant in [when - micro(), when, when + micro()] {
            for r_id in [1, b] {
                for t in TABLES {
                    let t = Uuid::from_u128(t);
                    let want: Option<i64> = on.query_one(&at(&r), &[&r_id, &t, &instant]).await?.get(0);
                    let got: Option<i64> = on.query_one(&at(&p), &[&r_id, &t, &instant]).await?.get(0);
                    if want != got {
                        return Err(format!("as_of_time({r_id}, {t}, {instant:?}): {want:?} and {got:?}").into());
                    }
                }
            }
        }
    }
    // CHG-1 by table, CHG-2 on every pair of snapshots, then both again under a retention, and TT-1.
    for t in TABLES {
        let t = Uuid::from_u128(t);
        for lim in [1i32, 1000] {
            let feed = "select commit_seq, ref_id, committed_at, operation from {s}.changes('-infinity', 0, $1, $2)";
            same(&on, &r, &p, feed, &[&lim, &t]).await?;
        }
    }
    let pairs: Vec<(i32, Uuid, i64, i64)> = on
        .query(
            &format!(
                "select x.ref_id, x.table_id, x.snapshot_id, y.snapshot_id from {p}.snapshot x join {p}.snapshot y
                   on y.table_id = x.table_id and y.ref_id = x.ref_id
                  order by 1, 2, 3, 4"
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    let changes = "select change, file_id, path, vector_before, vector_after from {s}.table_changes($1, $2, $3, $4)";
    for retained in [false, true] {
        if retained {
            for s in [&r, &p] {
                on.batch_execute(&format!("select {s}.set_retention(1, min_snapshots => 3)"))
                    .await?;
            }
            for row in &all {
                let when: Timestamp = row.get(2);
                for t in TABLES {
                    let at = "select {s}.as_of_time(1, $1, $2)";
                    same(&on, &r, &p, at, &[&Uuid::from_u128(t), &when]).await?;
                }
            }
        }
        for (rf, t, a, b) in &pairs {
            same(&on, &r, &p, changes, &[rf, t, a, b]).await?;
        }
    }
    same(&on, &r, &p, "select {s}.fill_committed_at(1)", &[]).await?;
    same(
        &on,
        &r,
        &p,
        "select committed_at from {s}.commit order by commit_seq",
        &[],
    )
    .await?;
    // TT-1: a commit whose transaction left no timestamp, as one that rolled back.
    on.batch_execute("begin").await?;
    let dead: String = on.query_one("select pg_current_xact_id()::text", &[]).await?.get(0);
    on.batch_execute("rollback").await?;
    for s in [&r, &p] {
        on.batch_execute(&format!(
            "update {s}.commit set committed_at = null, xid = '{dead}'
              where commit_seq = (select max(commit_seq) from {s}.commit)"
        ))
        .await?;
    }
    for sql in [
        "select {s}.fill_committed_at()",
        "select * from {s}.changes('-infinity', 0)",
    ] {
        if same(&on, &r, &p, sql, &[]).await? != "0A000" {
            return Err(format!("{sql}: a commit without a time was not refused").into());
        }
    }

    // Too recent a time is refused by both.
    let now = "select {s}.as_of_time(1, $1, now())";
    for s in [&r, &p] {
        let refused = on
            .query_one(&now.replace("{s}", s), &[&Uuid::from_u128(TABLES[0])])
            .await;
        if refused.as_ref().err().and_then(|e| e.code()).map(|c| c.code()) != Some("22023") {
            return Err(format!("{s} did not refuse a time less than one second ago: {refused:?}").into());
        }
    }
    on.batch_execute(&format!(
        "reset search_path; drop schema {p} cascade; drop schema {r} cascade"
    ))
    .await?;
    Ok(())
}

/// CHG-1 reads in catalog order through the index, from the cursor on: at 10⁵ commits, the plan of a call scans
/// `commit_by_time` and `commit_unfilled`, and not the table. Commits of less than one second ago, filled or not,
/// are not delivered yet.
#[tokio::test]
async fn the_feed_reads_through_its_indexes() -> Result<(), Error> {
    let protocol = fresh_schema("p");
    let on = connect().await?;
    install_protocol(&on, &protocol).await?;
    let p = quote_ident(&protocol);
    on.batch_execute(&format!(
        "insert into {p}.commit (commit_seq, ref_id, xid, committed_at, author, actor, operation)
         select g, 1, pg_current_xact_id(), case when g < 99900 then '2026-01-01'::timestamptz + g * interval '1 ms' end,
                'a', 'a', 'append'
           from generate_series(1, 100000) g;
         analyze {p}.commit"
    ))
    .await?;
    // In a transaction of its own: the one above may take more than a second.
    on.batch_execute(&format!(
        "insert into {p}.commit (commit_seq, ref_id, xid, committed_at, author, actor, operation)
         values (100001, 1, pg_current_xact_id(), now(), 'a', 'a', 'append')"
    ))
    .await?;
    let delivered: i64 = on
        .query_one(
            &format!("select count(*) from {p}.changes('-infinity', 0, 1000000)"),
            &[],
        )
        .await?
        .get(0);
    if delivered != 99899 {
        on.batch_execute(&format!("drop schema {p} cascade")).await?;
        return Err(format!("the feed delivered {delivered} commits, not the 99899 of more than a second ago").into());
    }
    let plan: Vec<String> = on
        .query(
            &format!("explain select * from {p}.changes('2026-01-01'::timestamptz + interval '99 s', 99000, 100)"),
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let plan = plan.join("\n");
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    if !plan.contains("commit_by_time") || !plan.contains("commit_unfilled") || plan.contains("Seq Scan") {
        return Err(format!("the feed does not read through its indexes:\n{plan}").into());
    }
    Ok(())
}

type Timestamp = std::time::SystemTime;

fn chrono_min() -> Timestamp {
    std::time::UNIX_EPOCH - std::time::Duration::from_secs(86400 * 365 * 100)
}

fn micro() -> std::time::Duration {
    std::time::Duration::from_micros(1)
}

fn rows(rows: Vec<tokio_postgres::Row>) -> Vec<(i64, i32, Timestamp, String)> {
    rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect()
}

/// What a statement answers in one catalog, as text: its rows, or the SQLSTATE of its refusal.
async fn answer(on: &Client, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<String, Error> {
    match on
        .query(
            &format!("select coalesce(string_agg(x::text, ' | ' order by x::text), '') from ({sql}) x"),
            params,
        )
        .await
    {
        Ok(rows) => Ok(rows[0].get(0)),
        Err(e) => match e.code() {
            Some(code) => Ok(code.code().to_string()),
            None => Err(e.into()),
        },
    }
}

/// Runs `sql`, with `{s}` the catalog, in the reference and in the protocol, and requires the same answer.
async fn same(on: &Client, r: &str, p: &str, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<String, Error> {
    let want = answer(on, &sql.replace("{s}", r), params).await?;
    let got = answer(on, &sql.replace("{s}", p), params).await?;
    if want != got {
        return Err(format!("{sql}: the reference answered {want}, the protocol {got}").into());
    }
    Ok(want)
}
