//! C10b (ADR 0003 §7): merging a branch into its parent, and what a refusal looks like to a program.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;

use common::{ok, run, sql, table, text};
use serde_json::Value;

fn answer(dir: &Path, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    serde_json::from_str(&ok(dir, &args)).unwrap()
}

fn rows(dir: &Path, table: &str) -> String {
    let select = format!("select k from {table} order by k");
    sql(dir, &select, "csv")
}

/// Every file under `dir`.
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        match entry.path() {
            path if path.is_dir() => found.extend(files(&path)),
            path => found.push(path),
        }
    }
    found
}

#[test]
fn merge_branch() {
    let dir = table();
    ok(&dir, &["branch", "create", "dev"]);
    sql(&dir, "insert into dev.db.t values (3, 'c', 2.5)", "csv");

    // A check changes nothing, and says what would happen.
    let check = answer(&dir, &["merge", "dev", "--check"]);
    assert_eq!(check["outcome"], "would merge");
    assert_eq!(check["merged"][0], "db.t");
    assert!(check["commit"].is_null());
    assert_eq!(rows(&dir, "main.db.t"), "k\n1\n2\n");

    let merged = answer(&dir, &["merge", "dev", "-m", "add a row"]);
    assert_eq!(merged["outcome"], "merged");
    assert!(merged["commit"].is_i64());
    assert_eq!(rows(&dir, "main.db.t"), "k\n1\n2\n3\n");

    // Merging again has nothing to do, and that is not a failure.
    assert_eq!(answer(&dir, &["merge", "dev"])["outcome"], "nothing");
    assert_eq!(answer(&dir, &["merge", "dev", "--check"])["outcome"], "nothing");

    // A tag is no branch.
    ok(&dir, &["tag", "create", "q3"]);
    assert_eq!(run(&dir, &["merge", "q3"]).status.code(), Some(1));
}

#[test]
fn merge_conflict_exit_code() {
    let dir = table();
    ok(&dir, &["branch", "create", "dev"]);
    // The branch sets a vector on a file the parent removes, which is a conflict (MRG-3, by MRG-5's rule).
    sql(&dir, "delete from dev.db.t where k = 1", "csv");
    sql(&dir, "insert overwrite main.db.t values (7, 'z', 9.5)", "csv");

    for args in [&["merge", "dev", "--check"][..], &["merge", "dev"][..]] {
        let mut args = args.to_vec();
        args.extend(["--format", "json"]);
        let refused = run(&dir, &args);
        assert_eq!(refused.status.code(), Some(3), "{args:?}: {}", text(&refused.stderr));
        assert!(refused.stdout.is_empty(), "{args:?}");
        let failure: Value = serde_json::from_slice(&refused.stderr).unwrap();
        assert_eq!(failure["error"], "conflict", "{args:?}");
        assert_eq!(failure["rule"], "MRG-5", "{args:?}: {failure}");
    }
    assert_eq!(rows(&dir, "main.db.t"), "k\n7\n");
}

#[test]
fn sql_conflict_exit_code() {
    let dir = table();
    let update = "update main.db.t set f = f + 1 where k = 1";
    let start = || {
        common::metalake(&dir)
            .args(["sql", update, "--format", "json"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut refusal = None;
    for _ in 0..20 {
        let racing = [start(), start()];
        for child in racing {
            let output = child.wait_with_output().unwrap();
            match output.status.code() {
                Some(0) => {}
                Some(3) => refusal = Some(output.stderr),
                other => panic!("{other:?}: {}", text(&output.stderr)),
            }
        }
        if refusal.is_some() {
            break;
        }
    }
    let failure: Value = serde_json::from_slice(&refusal.expect("two updates of one row raced")).unwrap();
    assert_eq!(failure["error"], "conflict");
    assert_eq!(failure["code"], "40001");
}

#[test]
fn merge_unions_vectors() {
    let dir = table();
    sql(
        &dir,
        "insert into main.db.t select value, 'row', 0.5 from generate_series(3, 1000000)",
        "csv",
    );
    ok(&dir, &["branch", "create", "dev"]);
    sql(&dir, "delete from dev.db.t where k % 2 = 0", "csv");
    sql(&dir, "delete from main.db.t where k % 3 = 0", "csv");
    assert!(
        files(&dir.join("data"))
            .iter()
            .any(|f| f.extension().is_some_and(|e| e == "puffin")),
        "vectors in Puffin, which only the core can union"
    );

    assert_eq!(answer(&dir, &["merge", "dev", "--check"])["outcome"], "would merge");
    assert_eq!(answer(&dir, &["merge", "dev"])["outcome"], "merged");
    let left = "select count(*) as n from main.db.t";
    assert_eq!(sql(&dir, left, "csv"), "n\n333333\n");
    let wrong = "select count(*) as n from main.db.t where k % 2 = 0 or k % 3 = 0";
    assert_eq!(sql(&dir, wrong, "csv"), "n\n0\n");
}

#[test]
fn merge_selected_table() {
    let dir = table();
    sql(
        &dir,
        "create table main.db.u (k int); insert into main.db.u values (1)",
        "csv",
    );
    ok(&dir, &["branch", "create", "dev"]);
    sql(&dir, "insert into dev.db.t values (3, 'c', 2.5)", "csv");
    sql(&dir, "insert into dev.db.u values (2)", "csv");

    let merged = answer(&dir, &["merge", "dev", "--table", "db.t"]);
    assert_eq!(merged["merged"], serde_json::json!(["db.t"]));
    assert_eq!(merged["rebased"], serde_json::json!(["db.u"]));
    assert_eq!(rows(&dir, "main.db.t"), "k\n1\n2\n3\n");
    assert_eq!(rows(&dir, "main.db.u"), "k\n1\n");
    assert_eq!(rows(&dir, "dev.db.u"), "k\n1\n2\n");

    assert_eq!(run(&dir, &["merge", "dev", "--table", "plain"]).status.code(), Some(2));
    assert_eq!(
        run(&dir, &["merge", "dev", "--table", "db.nothing"]).status.code(),
        Some(1)
    );
}

#[test]
fn mrg7_cross_table_commit_requires_split() {
    let dir = table();
    sql(
        &dir,
        "create table main.db.u (k int); insert into main.db.u values (1)",
        "csv",
    );
    ok(&dir, &["branch", "create", "dev"]);
    ok(&dir, &["branch", "create", "sub", "--from", "dev"]);
    sql(&dir, "insert into sub.db.t values (3, 'c', 2.5)", "csv");
    sql(&dir, "insert into sub.db.u values (2)", "csv");
    // Merging `sub` writes one commit of `dev` that crosses both tables.
    ok(&dir, &["merge", "sub"]);

    let refused = run(&dir, &["merge", "dev", "--table", "db.t", "--format", "json"]);
    assert_eq!(refused.status.code(), Some(3), "{}", text(&refused.stderr));
    let failure: Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert_eq!(failure["rule"], "MRG-7", "{failure}");
    assert_eq!(rows(&dir, "main.db.t"), "k\n1\n2\n");

    answer(&dir, &["merge", "dev", "--table", "db.t", "--split"]);
    assert_eq!(rows(&dir, "main.db.t"), "k\n1\n2\n3\n");
    assert_eq!(rows(&dir, "main.db.u"), "k\n1\n");
}
