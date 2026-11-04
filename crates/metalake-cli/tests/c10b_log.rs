//! C10b (ADR 0003 §7): what happened on a ref, and the sequence that joins it to reading the past.

mod common;

use std::path::Path;

use common::{ok, run, sql, table};
use serde_json::Value;

fn log(dir: &Path, args: &[&str]) -> Vec<Value> {
    let mut args = [&["log"], args].concat();
    args.extend(["--format", "json"]);
    let lines = ok(dir, &args);
    lines.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn count(dir: &Path, on: &str) -> String {
    let select = format!("select count(*) as n from \"{on}\".db.t");
    sql(dir, &select, "csv").lines().nth(1).unwrap().to_string()
}

#[test]
fn a_log_lists_the_latest_first() {
    let dir = table();
    let all = log(&dir, &[]);
    assert!(all.len() >= 3, "{all:?}");
    let seqs: Vec<i64> = all.iter().map(|c| c["commit_seq"].as_i64().unwrap()).collect();
    assert!(seqs.windows(2).all(|w| w[0] > w[1]), "{seqs:?}");
    assert_eq!(all[0]["operation"], "append");

    let time = all[0]["committed_at"].as_str().unwrap();
    assert!(
        time.len() == 27 && time.ends_with('Z') && &time[10..11] == "T",
        "{time}"
    );

    assert_eq!(log(&dir, &["--limit", "1"]).len(), 1);
    assert_eq!(log(&dir, &["--limit", "1"])[0], all[0]);
    assert_eq!(run(&dir, &["log", "--limit", "-1"]).status.code(), Some(2));
    assert_eq!(run(&dir, &["log", "nothing"]).status.code(), Some(1));

    // For a person, under the same columns.
    let text = ok(&dir, &["log", "--format", "csv"]);
    assert!(
        text.starts_with("commit_seq,committed_at,author,operation,message\n"),
        "{text}"
    );
}

#[test]
fn a_sequence_of_the_log_reads_and_starts_from_the_past() {
    let dir = table();
    let two_rows = log(&dir, &["--limit", "1"])[0]["commit_seq"].as_i64().unwrap();
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");
    assert_eq!(count(&dir, "main"), "3");

    let past = format!("main@{two_rows}");
    assert_eq!(count(&dir, &past), "2");
    ok(&dir, &["branch", "create", "old", "--from", &past]);
    assert_eq!(count(&dir, "old"), "2");
}

#[test]
fn a_merge_is_signed_and_a_branch_shows_its_own_commits() {
    let dir = table();
    ok(&dir, &["branch", "create", "dev"]);
    sql(&dir, "insert into dev.db.t values (3, 'c', 2.5)", "csv");

    let own = log(&dir, &["dev"]);
    assert_eq!(own.len(), 2, "its creation and its insert: {own:?}");
    assert_eq!(own[0]["operation"], "append");
    assert_eq!(own[1]["operation"], "branch");

    ok(&dir, &["merge", "dev", "-m", "add a row", "--author", "ana"]);
    let merge = &log(&dir, &["--limit", "1"])[0];
    assert_eq!(merge["operation"], "merge");
    assert_eq!(merge["author"], "ana");
    assert_eq!(merge["message"], "add a row");

    // The environment names the author when the flag does not.
    ok(&dir, &["branch", "create", "next"]);
    sql(&dir, "insert into next.db.t values (4, 'd', 3.5)", "csv");
    let output = common::metalake(&dir)
        .env("METALAKE_AUTHOR", "bea")
        .args(["merge", "next"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(log(&dir, &["--limit", "1"])[0]["author"], "bea");
}

#[test]
fn a_tables_log_shows_only_that_table() {
    let dir = table();
    sql(
        &dir,
        "create table main.db.u (k int); insert into main.db.u values (1)",
        "csv",
    );
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");

    let of_u = log(&dir, &["--table", "db.u"]);
    assert_eq!(of_u.len(), 2, "its creation and its insert: {of_u:?}");
    assert_eq!(of_u[0]["parent_snapshot_id"], of_u[1]["snapshot_id"]);
    let of_t = log(&dir, &["--table", "db.t"]);
    assert_eq!(of_t.len(), 3, "{of_t:?}");

    assert_eq!(run(&dir, &["log", "--table", "plain"]).status.code(), Some(2));
    assert_eq!(run(&dir, &["log", "--table", "db.none"]).status.code(), Some(1));
}
