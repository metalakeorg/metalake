//! C10b (ADR 0003 §7): the life of a ref, through the binary. Run by `CRATE=metalake-cli bash sql/test.sh`.

mod common;

use std::path::Path;

use common::{ok, run, sql, table, text};
use serde_json::Value;

/// The rows `args` prints, in `json`.
fn objects(dir: &Path, args: &[&str]) -> Vec<Value> {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let lines = ok(dir, &args);
    lines.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn count(dir: &Path, on: &str) -> String {
    let select = format!("select count(*) as n from \"{on}\".db.t");
    sql(dir, &select, "csv").lines().nth(1).unwrap().to_string()
}

#[test]
fn a_branch_writes_alone() {
    let dir = table();
    let created = objects(&dir, &["branch", "create", "dev"]);
    assert_eq!(created.len(), 1);
    assert_eq!(created[0]["name"], "dev");
    assert_eq!(created[0]["kind"], "branch");
    assert_eq!(created[0]["parent"], "main");

    sql(&dir, "insert into dev.db.t values (3, 'c', 2.5)", "csv");
    assert_eq!(count(&dir, "dev"), "3");
    assert_eq!(count(&dir, "main"), "2");

    // From another branch, which has what that one has.
    assert_eq!(
        objects(&dir, &["branch", "create", "sub", "--from", "dev"])[0]["parent"],
        "dev"
    );
    assert_eq!(count(&dir, "sub"), "3");

    let names: Vec<Value> = objects(&dir, &["branch", "list"])
        .iter()
        .map(|r| r["name"].clone())
        .collect();
    assert_eq!(names, ["main", "dev", "sub"]);
}

#[test]
fn a_tag_holds_a_state() {
    let dir = table();
    let tag = &objects(&dir, &["tag", "create", "q3"])[0];
    assert_eq!(tag["kind"], "tag");
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");
    assert_eq!(count(&dir, "main"), "3");
    assert_eq!(count(&dir, "q3"), "2");

    let refused = run(&dir, &["sql", "insert into q3.db.t values (4, 'd', 3.5)"]);
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(count(&dir, "q3"), "2");

    assert_eq!(objects(&dir, &["tag", "list"]).len(), 1);
    let kinds: Vec<Value> = objects(&dir, &["refs"]).iter().map(|r| r["kind"].clone()).collect();
    assert_eq!(kinds, ["branch", "tag"]);
}

#[test]
fn a_ref_starts_from_a_point() {
    let dir = table();
    let mark = objects(&dir, &["tag", "create", "mark"])[0]["created_seq"]
        .as_i64()
        .unwrap();
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");

    // A ref starts only from a snapshot its parent keeps (RET-6): a tag's own sequence is none, and is refused.
    let refused = run(&dir, &["branch", "create", "old", "--from", &format!("main@{mark}")]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(text(&refused.stderr).contains("RET-6"), "{}", text(&refused.stderr));

    // The latest one before the tag holds the two rows.
    let started = (1..mark).rev().find(|seq| {
        let from = format!("main@{seq}");
        run(&dir, &["branch", "create", "old", "--from", &from])
            .status
            .success()
    });
    assert!(started.is_some());
    assert_eq!(count(&dir, "old"), "2");

    // A time has no one sequence, and a name that is nothing is nothing.
    let time = run(&dir, &["branch", "create", "x", "--from", "main@2026-01-01"]);
    assert_eq!(time.status.code(), Some(2));
    assert!(text(&time.stderr).contains("metalake log"), "{}", text(&time.stderr));
    let none = run(&dir, &["branch", "create", "x", "--from", "nothing"]);
    assert_eq!(none.status.code(), Some(1));
}

#[test]
fn a_ref_is_deleted_on_purpose_and_for_good() {
    let dir = table();
    ok(&dir, &["branch", "create", "dev"]);

    let unconfirmed = run(&dir, &["branch", "delete", "dev"]);
    assert_eq!(unconfirmed.status.code(), Some(2));
    assert!(text(&unconfirmed.stderr).contains("--yes"));
    assert_eq!(objects(&dir, &["branch", "list"]).len(), 2);

    // A verb of one kind does not reach the other.
    assert_eq!(run(&dir, &["tag", "delete", "dev", "--yes"]).status.code(), Some(1));
    assert_eq!(objects(&dir, &["branch", "list"]).len(), 2);

    assert_eq!(
        objects(&dir, &["branch", "delete", "dev", "--yes"])[0]["deleted"],
        "dev"
    );
    assert_eq!(objects(&dir, &["branch", "list"]).len(), 1);

    assert_eq!(
        run(&dir, &["branch", "create", "dev"]).status.code(),
        Some(1),
        "a name is not reused"
    );
    assert_eq!(run(&dir, &["branch", "delete", "main", "--yes"]).status.code(), Some(1));
}

#[test]
fn a_list_prints_in_each_format() {
    let dir = table();
    // An empty list keeps its header.
    assert_eq!(
        ok(&dir, &["tag", "list", "--format", "csv"]),
        "name,kind,parent,fork_seq,created_seq,protected\n"
    );
    let csv = ok(&dir, &["branch", "list", "--format", "csv"]);
    assert!(csv.lines().nth(1).unwrap().starts_with("main,branch,,,"), "{csv}");
    let table = ok(&dir, &["branch", "list", "--format", "table"]);
    assert!(table.contains("| name") && table.contains("| main"), "{table}");

    // In `json` a value keeps its type.
    let main = &objects(&dir, &["branch", "list"])[0];
    assert!(main["created_seq"].is_i64() && main["protected"].is_boolean() && main["parent"].is_null());
}
