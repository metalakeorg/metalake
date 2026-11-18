//! C10m: a lake whose warehouse is on HDFS lives its whole cycle through the binary.
//! - It is made, and its table written in files on HDFS, then read.
//! - Deleted from, by vectors.
//! - Branched, written on the branch apart from main, and merged.
//! - Kept to its last snapshot and maintained: compacted, expired and swept, leaving fewer files on HDFS and the same
//!   rows.
//! - `strays` lists the table's directory as the catalog spells its paths, and takes nothing.
//!
//! Run against the HDFS that `METALAKE_TEST_HDFS` names, whose NameNode runs in the container `namenode` (as
//! `.github/workflows/hdfs.yml` starts it); skipped without it.

mod common;

use std::path::Path;
use std::process::Command;

use common::{directory, ok, sql, sql_on, text, unique, url};
use serde_json::Value;

/// How many Parquet files lie under `location` on HDFS, as the NameNode lists them.
fn parquet(location: &str) -> usize {
    let output = Command::new("docker")
        .args(["exec", "namenode", "hdfs", "dfs", "-ls", "-R", location])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", text(&output.stderr));
    text(&output.stdout).lines().filter(|l| l.ends_with(".parquet")).count()
}

fn count(dir: &Path, on: &str) -> String {
    sql_on(dir, on, "select count(*) as n from db.t", "csv")
}

fn insert(dir: &Path, from: u64) {
    let to = from + 99_999;
    sql(
        dir,
        &format!("insert into db.t select value from generate_series({from}, {to})"),
        "csv",
    );
}

fn maintain(dir: &Path, args: &[&str]) -> Vec<Value> {
    let lines = ok(dir, &[&["maintain"], args, &["--format", "json"]].concat());
    lines.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

#[test]
fn c10m_hdfs_lake_cycle() {
    let Ok(namenode) = std::env::var("METALAKE_TEST_HDFS") else {
        eprintln!("skipped: no HDFS named by METALAKE_TEST_HDFS");
        return;
    };
    let warehouse = format!("{}/c10m/{}", namenode.trim_end_matches('/'), unique());
    let dir = directory();
    ok(&dir, &["init", &url(), &warehouse, "--schema", &unique()]);
    let status: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();
    assert_eq!(status["warehouse"], warehouse.as_str(), "{status}");

    // Written in files on HDFS, too large for the catalog, and read back.
    sql(&dir, "create schema db; create table db.t (k bigint)", "csv");
    insert(&dir, 1);
    insert(&dir, 100_001);
    assert_eq!(parquet(&warehouse), 2);
    assert_eq!(count(&dir, "use branch main"), "n\n200000\n");

    // A delete marks rows by vectors and rewrites no file.
    sql(&dir, "delete from db.t where k > 150000", "csv");
    assert_eq!(count(&dir, "use branch main"), "n\n150000\n");

    // A branch writes apart from main, and its merge brings what it wrote.
    ok(&dir, &["branch", "create", "dev"]);
    sql_on(
        &dir,
        "use branch dev",
        "insert into db.t select value from generate_series(200001, 300000)",
        "csv",
    );
    assert_eq!(count(&dir, "use branch dev"), "n\n250000\n");
    assert_eq!(count(&dir, "use branch main"), "n\n150000\n");
    ok(&dir, &["merge", "dev", "-m", "the third batch"]);
    assert_eq!(count(&dir, "use branch main"), "n\n250000\n");
    let before = parquet(&warehouse);
    assert!(before >= 3, "{before} files");

    // Kept to its last snapshot, with no grace: compacted, expired, swept from HDFS.
    ok(&dir, &["branch", "delete", "dev", "--yes"]);
    ok(&dir, &["retain", "main", "--min-snapshots", "1"]);
    ok(&dir, &["retain", "--grace", "0s"]);
    let done = maintain(&dir, &[]);
    let tasks: Vec<&str> = done.iter().map(|d| d["task"].as_str().unwrap()).collect();
    assert_eq!(
        tasks,
        ["times", "flush", "compact", "footers", "expire", "sweep", "orphans"]
    );
    assert_eq!(count(&dir, "use branch main"), "n\n250000\n");
    let after = parquet(&warehouse);
    assert!(after < before, "maintenance left {after} of {before} files");

    // `strays` lists the table's directory: every path the catalog holds is spelled as HDFS lists it, and nothing
    // young enough to be a write in progress is taken.
    let strays = maintain(&dir, &["strays", "--table", "db.t"]);
    assert_eq!(strays[0]["result"], 0, "{strays:?}");
    assert_eq!(parquet(&warehouse), after);
    assert_eq!(count(&dir, "use branch main"), "n\n250000\n");
}
