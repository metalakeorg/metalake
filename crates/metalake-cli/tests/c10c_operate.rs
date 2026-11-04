//! C10c (ADR 0003 §7): operating a lake through the binary: upgrading its catalog and keeping it healthy.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::{lake, ok, run, sql, text};
use serde_json::Value;

/// A statement in the catalog's Postgres, past the protocol, as an older or newer catalog would have it. The tests'
/// Postgres is in the container `sql/test.sh` starts, and the command line may not depend on the driver.
fn postgres(statement: &str) {
    let output = Command::new("docker")
        .args([
            "exec",
            "metalake-protocol-pg",
            "psql",
            "-U",
            "postgres",
            "-c",
            statement,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", text(&output.stderr));
}

fn rows(dir: &Path) -> String {
    sql(dir, "select k from t order by k", "csv")
}

#[test]
fn migrating_a_catalog_up_to_date_changes_nothing() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    let before: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();

    for _ in 0..2 {
        let migrated: Value = serde_json::from_str(&ok(&dir, &["migrate", "--format", "json"])).unwrap();
        assert!(migrated["format_migration"].is_null());
        assert_eq!(migrated["schema"], schema.as_str());
        assert_eq!(migrated["warehouse"], before["warehouse"]);
        assert_eq!(migrated["protocol_version"], before["protocol_version"]);
    }
    assert_eq!(rows(&dir), "k\n1\n");
}

#[test]
fn a_catalog_behind_this_binary_asks_for_migrate() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    postgres(&format!("delete from {schema}.meta where key = 'protocol_version'"));

    let refused = run(&dir, &["status"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(
        text(&refused.stderr).contains("run `metalake migrate`"),
        "{}",
        text(&refused.stderr)
    );

    ok(&dir, &["migrate"]);
    assert!(ok(&dir, &["status"]).contains("protocol   0"));
    assert_eq!(rows(&dir), "k\n1\n");
}

#[test]
fn a_catalog_ahead_of_this_binary_asks_for_an_upgrade() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    postgres(&format!(
        "update {schema}.meta set value = '99' where key = 'protocol_version'"
    ));

    // `migrate` cannot help here, and the message must not send anyone to it.
    let refused = run(&dir, &["status"]);
    assert_eq!(refused.status.code(), Some(1));
    let message = text(&refused.stderr);
    assert!(
        message.contains("upgrade") && !message.contains("metalake migrate"),
        "{message}"
    );

    let refused = run(&dir, &["migrate"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(text(&refused.stderr).contains("newer"), "{}", text(&refused.stderr));

    // It touched nothing.
    postgres(&format!(
        "update {schema}.meta set value = '0' where key = 'protocol_version'"
    ));
    assert_eq!(rows(&dir), "k\n1\n");
}

/// A lake whose table `db.t` holds two Parquet files of 100,000 rows each.
fn split() -> PathBuf {
    let (dir, _) = lake();
    sql(&dir, "create schema main.db; create table main.db.t (k bigint)", "csv");
    for from in [1, 100_001] {
        let insert = format!(
            "insert into main.db.t select value from generate_series({from}, {})",
            from + 99_999
        );
        sql(&dir, &insert, "csv");
    }
    assert_eq!(parquet(&dir), 2);
    dir
}

/// How many Parquet files the warehouse holds.
fn parquet(dir: &Path) -> usize {
    fn under(dir: &Path) -> usize {
        fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| match entry.path() {
                path if path.is_dir() => under(&path),
                path => usize::from(path.extension().is_some_and(|e| e == "parquet")),
            })
            .sum()
    }
    under(&dir.join("data"))
}

fn count(dir: &Path, on: &str) -> String {
    let select = format!("select count(*) as n, max(k) as m from \"{on}\".db.t");
    sql(dir, &select, "csv")
}

fn maintain(dir: &Path, args: &[&str]) -> Vec<Value> {
    let lines = ok(dir, &[&["maintain"], args, &["--format", "json"]].concat());
    lines.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

const ALL: &str = "n,m\n200000,200000\n";

#[test]
fn with_no_policy_nothing_is_taken_away() {
    let dir = split();
    let done = maintain(&dir, &[]);
    let tasks: Vec<&str> = done.iter().map(|d| d["task"].as_str().unwrap()).collect();
    assert_eq!(tasks, ["times", "flush", "compact", "expire", "sweep", "orphans"]);
    assert!(done[2]["result"].is_i64(), "a compaction's commit: {:?}", done[2]);

    // The rewrite added a file, and every read of the past still needs the old ones.
    assert_eq!(count(&dir, "main"), ALL);
    assert_eq!(parquet(&dir), 3);
}

#[test]
fn what_no_ref_keeps_leaves_the_disk() {
    let dir = split();
    ok(&dir, &["retain", "main", "--min-snapshots", "1"]);
    ok(&dir, &["retain", "--grace", "0s"]);
    maintain(&dir, &[]);
    assert_eq!(count(&dir, "main"), ALL);
    assert_eq!(parquet(&dir), 1, "one run compacts, expires and sweeps");

    // There is nothing left to do.
    let again = maintain(&dir, &[]);
    assert!(again[2]["result"].is_null(), "{:?}", again[2]);
    assert_eq!(parquet(&dir), 1);
}

#[test]
fn a_tag_keeps_what_it_reads() {
    let dir = split();
    ok(&dir, &["tag", "create", "q3"]);
    ok(&dir, &["retain", "main", "--min-snapshots", "1"]);
    ok(&dir, &["retain", "--grace", "0s"]);
    sql(&dir, "delete from main.db.t where k > 100000", "csv");
    maintain(&dir, &[]);

    assert_eq!(count(&dir, "main"), "n,m\n100000,100000\n");
    assert_eq!(count(&dir, "q3"), ALL, "the tag reads what main no longer keeps");

    // Without the tag, nothing holds them.
    ok(&dir, &["tag", "delete", "q3", "--yes"]);
    maintain(&dir, &[]);
    assert_eq!(parquet(&dir), 1);
    assert_eq!(count(&dir, "main"), "n,m\n100000,100000\n");
}

#[test]
fn a_rewrite_can_be_narrowed() {
    let dir = split();
    ok(&dir, &["branch", "create", "dev"]);
    ok(&dir, &["tag", "create", "q3"]);

    let done = maintain(&dir, &["compact", "--ref", "dev", "--table", "db.t"]);
    assert_eq!(done.len(), 1);
    assert_eq!(done[0]["ref"], "dev");
    assert_eq!(done[0]["table"], "db.t");
    assert!(done[0]["result"].is_i64());
    assert_eq!(count(&dir, "dev"), ALL);

    // A run over everything goes over branches, and a tag takes no commits.
    let every: Vec<Value> = maintain(&dir, &["compact"]).iter().map(|d| d["ref"].clone()).collect();
    assert_eq!(every, ["main", "dev"]);

    let tag = run(&dir, &["maintain", "compact", "--ref", "q3"]);
    assert_eq!(tag.status.code(), Some(1));
    let wide = run(&dir, &["maintain", "expire", "--table", "db.t"]);
    assert_eq!(wide.status.code(), Some(2));
    let all = run(&dir, &["maintain", "--ref", "dev"]);
    assert_eq!(all.status.code(), Some(2));
}

#[test]
fn a_policy_is_checked_before_it_is_set() {
    let (dir, _) = lake();
    ok(&dir, &["retain", "main", "--max-age", "30d", "--min-snapshots", "10"]);
    ok(&dir, &["retain", "--grace", "12h"]);
    assert_eq!(run(&dir, &["retain"]).status.code(), Some(2));
    assert_eq!(run(&dir, &["retain", "main"]).status.code(), Some(2));
    assert_eq!(run(&dir, &["retain", "--max-age", "1d"]).status.code(), Some(2));
    assert_eq!(
        run(&dir, &["retain", "main", "--max-age", "soon"]).status.code(),
        Some(2)
    );
    assert_eq!(run(&dir, &["retain", "main", "--max-age", "é"]).status.code(), Some(2));
    assert_eq!(
        run(&dir, &["retain", "nothing", "--max-age", "1d"]).status.code(),
        Some(1)
    );
}
