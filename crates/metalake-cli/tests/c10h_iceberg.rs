//! C10h: `metalake iceberg`, through the binary: setting tables on-commit and back on-load on a ref, and where their
//! Iceberg metadata stands.

mod common;

use common::{lake, ok, run, sql, text};
use serde_json::Value;

fn json(out: &str) -> Vec<Value> {
    out.lines().map(|line| serde_json::from_str(line).unwrap()).collect()
}

#[test]
fn rest9_iceberg_on_commit_on_load_status() {
    let (dir, _) = lake();
    sql(&dir, "create table a (k int); create table b (k int)", "csv");

    let set = json(&ok(&dir, &["iceberg", "on-commit", "default.a", "default.b", "--format", "json"]));
    assert_eq!(set[0]["metadata"], "on-commit");
    assert!(set[0]["commit"].is_i64(), "{set:?}");
    let again = json(&ok(&dir, &["iceberg", "on-commit", "default.a", "--format", "json"]));
    assert!(again[0]["commit"].is_null(), "nothing changed: {again:?}");

    let status = json(&ok(&dir, &["iceberg", "status", "--format", "json"]));
    let tables: Vec<&str> = status.iter().map(|t| t["table"].as_str().unwrap()).collect();
    assert_eq!(tables, ["default.a", "default.b"]);
    assert!(status.iter().all(|t| t["written"].is_null() && t["behind"].as_i64() > Some(0)), "{status:?}");

    let unset = json(&ok(&dir, &["iceberg", "on-load", "default.b", "--format", "json"]));
    assert_eq!(unset[0]["metadata"], "on-load");
    let status = json(&ok(&dir, &["iceberg", "status", "--format", "json"]));
    assert_eq!(status.len(), 1);
}

#[test]
fn rest9_iceberg_on_commit_missing_table_refuses() {
    let (dir, _) = lake();
    sql(&dir, "create table a (k int)", "csv");
    let refused = run(&dir, &["iceberg", "on-commit", "default.a", "default.nowhere"]);
    assert!(!refused.status.success());
    assert!(text(&refused.stderr).contains("default.nowhere"), "{}", text(&refused.stderr));
    let status = ok(&dir, &["iceberg", "status", "--format", "json"]);
    assert!(status.trim().is_empty(), "nothing set: {status}");
    let none = run(&dir, &["iceberg", "on-commit"]);
    assert!(!none.status.success(), "a table is required");
}
