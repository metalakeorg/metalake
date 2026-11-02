//! G2: the locks of the protocol under real concurrency, on two connections to one catalog (ADR 0002 §21.4).
//! - Two commits that give two tables one name: the second waits for the first, then is refused (TBL-1).
//! - A branch created while a commit of its parent is in flight waits for it, and sees what it committed.
//! - A merge waits for the commits in flight on the parent, and a commit that arrives after the merge asked waits
//!   behind it: the exclusive request is not starved.

use std::time::Duration;

use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{connect, fresh_schema, install_protocol, quote_ident, Error};
use tokio_postgres::Client;
use uuid::Uuid;

const T: Uuid = Uuid::from_u128(0xa000);

async fn catalog() -> Result<(String, Client, Client), Error> {
    let schema = fresh_schema("p");
    let a = connect().await?;
    install_protocol(&a, &schema).await?;
    let b = connect().await?;
    for c in [&a, &b] {
        c.batch_execute(&format!("set search_path = {}", quote_ident(&schema)))
            .await?;
    }
    create(&a, T, "t").await?;
    Ok((schema, a, b))
}

fn create_sql(t: Uuid, name: &str) -> String {
    format!(
        "select commit_seq from commit(ref_id => 1, operation => 'update', changes => jsonb_build_object('{t}',
           jsonb_build_object('create', jsonb_build_object('namespace', '[\"db\"]'::jsonb, 'name', '{name}',
                                                           'location', 'gs://lake'),
                              'fields', '[{{\"field_id\": 1, \"new\": true, \"position\": 1, \"name\": \"k\",
                                           \"type\": \"long\", \"required\": true}}]'::jsonb,
                              'cluster_key', '[1]'::jsonb)))"
    )
}

async fn create(c: &Client, t: Uuid, name: &str) -> Result<i64, Error> {
    Ok(c.query_one(&create_sql(t, name), &[]).await?.get(0))
}

async fn append(c: &Client, r: i32, path: &str) -> Result<i64, Error> {
    let files = vec![stored_file(T, path.into(), 10, 1, 0, 9)];
    let row = c
        .query_one(
            "select commit_seq from commit(ref_id => $1, operation => 'append', files => $2)",
            &[&r, &files],
        )
        .await?;
    Ok(row.get(0))
}

async fn pid(c: &Client) -> Result<i32, Error> {
    Ok(c.query_one("select pg_backend_pid()", &[]).await?.get(0))
}

/// How many of these backends wait on an advisory lock (tests run in parallel, so only their own count).
async fn waiting(c: &Client, pids: &[i32]) -> Result<i64, Error> {
    let sql = "select count(*) from pg_locks where locktype = 'advisory' and not granted and pid = any ($1)";
    Ok(c.query_one(sql, &[&pids]).await?.get(0))
}

async fn until_waiting(c: &Client, pid: i32) -> Result<(), Error> {
    for _ in 0..100 {
        if waiting(c, &[pid]).await? > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("nothing waits on a lock".into())
}

async fn drop_schema(c: &Client, schema: &str) -> Result<(), Error> {
    c.batch_execute(&format!("drop schema {} cascade", quote_ident(schema)))
        .await?;
    Ok(())
}

#[tokio::test]
async fn one_name_given_twice_at_once_is_refused_to_the_second() -> Result<(), Error> {
    let (schema, a, b) = catalog().await?;
    let watcher = connect().await?;
    let waiter = pid(&b).await?;
    a.batch_execute("begin").await?;
    create(&a, Uuid::from_u128(0xa001), "same").await?;
    let second = tokio::spawn(async move {
        let result = b.query_one(&create_sql(Uuid::from_u128(0xa002), "same"), &[]).await;
        (b, result)
    });
    until_waiting(&watcher, waiter).await?;
    a.batch_execute("commit").await?;
    let (_b, result) = second.await?;
    match result {
        Err(e) if e.code().map(|c| c.code()) == Some("40001") => {}
        other => return Err(format!("the second commit answered {other:?}").into()),
    }
    drop_schema(&a, &schema).await
}

#[tokio::test]
async fn a_branch_waits_for_its_parents_commit_in_flight_and_sees_it() -> Result<(), Error> {
    let (schema, a, b) = catalog().await?;
    let watcher = connect().await?;
    let waiter = pid(&b).await?;
    a.batch_execute("begin").await?;
    append(&a, 1, "gs://lake/in-flight.parquet").await?;
    let branch = tokio::spawn(async move {
        let result = b.query_one("select create_ref(name => 'b', parent => 1)", &[]).await;
        (b, result)
    });
    until_waiting(&watcher, waiter).await?;
    a.batch_execute("commit").await?;
    let (b, result) = branch.await?;
    let id: i32 = result?.get(0);
    let seen: i64 = b
        .query_one(
            "select count(*) from plan($1, $2) where path = 'gs://lake/in-flight.parquet'",
            &[&id, &T],
        )
        .await?
        .get(0);
    if seen != 1 {
        return Err("the branch does not see the commit it waited for".into());
    }
    drop_schema(&a, &schema).await
}

#[tokio::test]
async fn a_merge_is_not_starved_by_commits_that_arrive_after_it() -> Result<(), Error> {
    let (schema, a, b) = catalog().await?;
    let watcher = connect().await?;
    let merger = pid(&b).await?;
    let branch: i32 = a
        .query_one("select create_ref(name => 'b', parent => 1)", &[])
        .await?
        .get(0);
    append(&a, branch, "gs://lake/on-branch.parquet").await?;
    // A commit of main in flight; the merge asks for main and waits.
    a.batch_execute("begin").await?;
    append(&a, 1, "gs://lake/first.parquet").await?;
    let merge = tokio::spawn(async move {
        let result = b
            .query_one("select commit_seq from merge(branch => $1)", &[&branch])
            .await;
        (b, result)
    });
    until_waiting(&watcher, merger).await?;
    // A commit that arrives now waits behind the merge.
    let late = connect().await?;
    let latecomer = pid(&late).await?;
    late.batch_execute(&format!("set search_path = {}", quote_ident(&schema)))
        .await?;
    let after = tokio::spawn(async move {
        let result = append(&late, 1, "gs://lake/late.parquet").await;
        (late, result)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let waiters = waiting(&watcher, &[merger, latecomer]).await?;
    if waiters != 2 {
        return Err(format!("{waiters} requests wait, not the merge and the late commit").into());
    }
    a.batch_execute("commit").await?;
    let (_b, merged) = merge.await?;
    let (_late, late_seq) = after.await?;
    let merged: Option<i64> = merged?.get(0);
    let late_seq = late_seq?;
    if merged.is_none_or(|m| m > late_seq) {
        return Err(format!("the merge ({merged:?}) did not go before the late commit ({late_seq})").into());
    }
    drop_schema(&a, &schema).await
}
