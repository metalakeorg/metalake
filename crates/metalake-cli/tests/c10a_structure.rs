//! C10a (ADR 0003 §7): the command line, as a person or an agent runs it: the real binary, with its stdout, its stderr
//! and its exit code. Run by `CRATE=metalake-cli bash sql/test.sh`.

mod common;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use common::{directory, lake, metalake, ok, run, sql, table, text, unique, url};
use serde_json::Value;

#[test]
fn status_from_subdirectory() {
    let (dir, schema) = lake();
    let below = dir.join("a/b");
    fs::create_dir_all(&below).unwrap();

    // The file holds where the catalog is, and nothing of what the catalog holds.
    let file = fs::read_to_string(dir.join("metalake.toml")).unwrap();
    assert!(
        file.contains("postgres") && file.contains(&schema) && !file.contains("warehouse"),
        "{file}"
    );

    let status: Value = serde_json::from_str(&ok(&below, &["status", "--format", "json"])).unwrap();
    assert_eq!(status["schema"], schema.as_str());
    assert_eq!(status["format_version"], "0");
    assert_eq!(status["refs"][0]["name"], "main");
    let warehouse = status["warehouse"].as_str().unwrap();
    assert!(
        warehouse.starts_with("file:///") && warehouse.ends_with("/data/"),
        "{warehouse}"
    );
    assert!(dir.join("data").is_dir());

    // In text, for a person, and through a directory named.
    let elsewhere = directory();
    let lines = ok(&elsewhere, &["status", "--lake", dir.to_str().unwrap()]);
    assert!(lines.contains(&format!("schema     {schema}")), "{lines}");
    assert!(lines.contains("refs       main (branch)"), "{lines}");
}

#[test]
fn status_from_environment() {
    let (_, schema) = lake();
    let empty = directory();
    let output = metalake(&empty)
        .env("METALAKE_POSTGRES", url())
        .env("METALAKE_SCHEMA", &schema)
        .args(["status", "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", text(&output.stderr));
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["schema"], schema.as_str());
}

#[test]
fn init_password_refused() {
    let dir = directory();
    let output = run(&dir, &["init", "postgresql://u:secret@host/db", "data"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let message = text(&output.stderr);
    assert!(
        message.contains("PGPASSWORD") && !message.contains("secret"),
        "{message}"
    );
    assert!(!dir.join("metalake.toml").exists());
}

#[test]
fn init_existing_lake_refused() {
    let (dir, schema) = lake();
    let before = fs::read(dir.join("metalake.toml")).unwrap();
    let output = run(&dir, &["init", &url(), "data", "--schema", &unique()]);
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output.stderr).contains("already exists"));
    assert_eq!(fs::read(dir.join("metalake.toml")).unwrap(), before);
    assert!(ok(&dir, &["status"]).contains(&schema));

    // A schema that is taken leaves no file behind.
    let other = directory();
    let output = run(&other, &["init", &url(), "data", "--schema", &schema]);
    assert_eq!(output.status.code(), Some(1));
    assert!(!other.join("metalake.toml").exists());
}

#[test]
fn failure_stderr_and_exit_code() {
    let empty = directory();

    let wrong = run(&empty, &["nonsense"]);
    assert_eq!(wrong.status.code(), Some(2));
    assert!(wrong.stdout.is_empty());

    let help = run(&empty, &["--help"]);
    assert_eq!(help.status.code(), Some(0));
    assert!(text(&help.stdout).contains("Usage"));

    let none = run(&empty, &["status"]);
    assert_eq!(none.status.code(), Some(1));
    assert!(none.stdout.is_empty());
    assert!(text(&none.stderr).contains("metalake init"));

    // For a program, one object.
    let none = run(&empty, &["status", "--format", "json"]);
    assert_eq!(none.status.code(), Some(1));
    assert!(none.stdout.is_empty());
    let failure: Value = serde_json::from_slice(&none.stderr).unwrap();
    assert_eq!(failure["error"], "failed");
    assert_eq!(failure["retryable"], false);
    assert!(failure["message"].as_str().unwrap().contains("metalake init"));

    // The catalog's own refusal, which names a schema that is not there.
    let missing = metalake(&empty)
        .env("METALAKE_POSTGRES", url())
        .env("METALAKE_SCHEMA", "no_such_lake")
        .args(["status", "--format", "json"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(1));
    let failure: Value = serde_json::from_slice(&missing.stderr).unwrap();
    assert_eq!(failure["code"], "3F000");
}

#[test]
fn sql_output_formats() {
    let dir = table();
    let select = "select k, s, f from main.db.t order by k";
    assert_eq!(sql(&dir, select, "csv"), "k,s,f\n1,a,0.5\n2,,1.5\n");
    let lines: Vec<Value> = sql(&dir, select, "json")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["s"], "a");
    assert!(lines[1].get("s").unwrap().is_null(), "a null is a key: {:?}", lines[1]);
    let table = sql(&dir, select, "table");
    assert!(table.contains("| k | s | f"), "{table}");

    // In a pipe, what a program reads.
    assert!(ok(&dir, &["sql", select]).starts_with("k,s,f\n"));
    // A result with no rows keeps its header.
    assert_eq!(sql(&dir, "select k from main.db.t where k = 9", "csv"), "k\n");
}

/// Every file under `dir`.
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        match entry.path() {
            path if path.is_dir() => found.extend(files(&path)),
            path => found.push(path),
        }
    }
    found
}

#[test]
fn local_warehouse_round_trip() {
    let dir = table();
    assert!(files(&dir.join("data")).is_empty(), "two rows stay in the catalog");
    sql(
        &dir,
        "insert into main.db.t select value, 'row', 0.5 from generate_series(1, 200000)",
        "csv",
    );
    let written = files(&dir.join("data"));
    assert!(
        written.iter().any(|f| f.extension().is_some_and(|e| e == "parquet")),
        "{written:?}"
    );
    assert_eq!(sql(&dir, "select count(*) as n from main.db.t", "csv"), "n\n200002\n");
    // A write answers its row: the count, and what came of its commit.
    let deleted = sql(&dir, "delete from main.db.t where k > 10", "csv");
    let row = deleted
        .strip_prefix("count,commit,applied,inserted,updated\n199990,")
        .unwrap_or_default();
    assert!(row.ends_with(",true,0,0\n"), "{deleted}");
    assert_eq!(sql(&dir, "select count(*) as n from main.db.t", "csv"), "n\n12\n");
}

#[test]
fn unqualified_table_resolution() {
    let (dir, _) = lake();
    sql(&dir, "create table t (k int); insert into t values (7)", "csv");
    assert_eq!(sql(&dir, "select k from main.default.t", "csv"), "k\n7\n");
}

#[test]
fn sql_input_file_and_stdin() {
    let dir = table();
    let file = dir.join("q.sql");
    fs::write(&file, "select count(*) as n from main.db.t;").unwrap();
    assert_eq!(ok(&dir, &["sql", "-f", file.to_str().unwrap()]), "n\n2\n");

    let mut child = metalake(&dir)
        .arg("sql")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"select max(k) as m from main.db.t")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(text(&output.stdout), "m\n2\n");

    let nothing = run(&dir, &["sql", "  "]);
    assert_eq!(nothing.status.code(), Some(2));
}

#[test]
fn sql_script_stops_on_failure() {
    let dir = table();
    let output = run(
        &dir,
        &[
            "sql",
            "select nope from main.db.t; insert into main.db.t values (3, 'c', 2.5)",
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(sql(&dir, "select count(*) as n from main.db.t", "csv"), "n\n2\n");
}

#[test]
fn unsupported_table_options_refused() {
    let dir = table();
    for statement in [
        "create table main.db.a (k bigint primary key)",
        "create table main.db.b (k bigint default 1)",
        "create or replace table main.db.t (k bigint)",
        "create schema if not exists main.other",
    ] {
        let output = run(&dir, &["sql", statement]);
        assert_eq!(output.status.code(), Some(1), "{statement}");
        assert!(
            text(&output.stderr).contains("unsupported"),
            "{statement}: {}",
            text(&output.stderr)
        );
    }
    assert!(run(&dir, &["sql", "select * from main.db.a"]).status.code() == Some(1));
    sql(&dir, "create table if not exists main.db.t (other int)", "csv");
    assert!(sql(&dir, "select * from main.db.t limit 0", "csv").starts_with("k,s,f"));
    // A table by a query's columns, with its rows.
    sql(&dir, "create table main.db.c as select 1 as k", "csv");
    assert_eq!(sql(&dir, "select k from main.db.c", "csv"), "k\n1\n");
}

#[test]
fn session_builtin_functions() {
    let (dir, _) = lake();
    let select = "select lower('A') as a, date_trunc('year', timestamp '2026-11-04T10:00:00') as b,
                  regexp_like('abc', 'b') as c, abs(-1) as d, array_length(make_array(1, 2)) as e";
    assert_eq!(sql(&dir, select, "csv"), "a,b,c,d,e\na,2026-01-01T00:00:00,true,1,2\n");
}

#[test]
fn time_travel_by_catalog_name() {
    let dir = table();
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");
    assert_eq!(sql(&dir, "select count(*) as n from main.db.t", "csv"), "n\n3\n");
    let at = |seq: i64| {
        let select = format!("select count(*) as n from \"main@{seq}\".db.t");
        let output = run(&dir, &["sql", &select]);
        (output.status.success(), text(&output.stdout))
    };
    let two = (1..30).any(|seq| at(seq) == (true, "n\n2\n".to_string()));
    assert!(two, "a sequence where the table had its first two rows");
}
