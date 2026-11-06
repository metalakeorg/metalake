//! C10e (ADR 0003 §7): a lake with a Postgres of its own, through the binary, with no other Postgres anywhere. Run by
//! `cargo test -p metalake-cli --test c10e_local`.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use common::{directory, ok, run, sql, text};
use serde_json::Value;

/// A directory holding a new lake in `./lake`, whose server ends with the test.
struct Lake(PathBuf);

impl Lake {
    fn new() -> Self {
        let dir = directory();
        ok(&dir, &["init", "./lake"]);
        Self(dir)
    }
}

impl Drop for Lake {
    fn drop(&mut self) {
        let _ = run(&self.0, &["stop"]);
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn parquet(dir: &Path) -> usize {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| match entry.path() {
            path if path.is_dir() => parquet(&path),
            path => usize::from(path.extension().is_some_and(|e| e == "parquet")),
        })
        .sum()
}

#[test]
fn local_lake_dedicated_postgres() {
    let began = Instant::now();
    let lake = Lake::new();
    println!("init {:?}", began.elapsed());
    let dir = &lake.0;
    assert_eq!(
        fs::read_to_string(dir.join("metalake.toml")).unwrap(),
        "local = \"./lake\"\n"
    );

    let status: Value = serde_json::from_str(&ok(dir, &["status", "--format", "json"])).unwrap();
    assert!(status["postgres"].is_null());
    assert!(status["local"].as_str().unwrap().ends_with("lake"));
    let warehouse = status["warehouse"].as_str().unwrap();
    assert!(
        warehouse.starts_with("file:///") && warehouse.ends_with("lake/data/"),
        "{warehouse}"
    );

    sql(dir, "create table t (k bigint); insert into t values (1), (2)", "csv");
    sql(
        dir,
        "insert into t select value + 2 from generate_series(1, 200000)",
        "csv",
    );
    assert_eq!(parquet(&dir.join("lake/data")), 1);

    ok(dir, &["branch", "create", "dev"]);
    sql(dir, "insert into dev.default.t values (0)", "csv");
    ok(dir, &["merge", "dev", "--check"]);
    ok(dir, &["merge", "dev"]);
    assert_eq!(sql(dir, "select count(*) as n from t", "csv"), "n\n200003\n");
    assert!(ok(dir, &["log", "--limit", "1"]).contains("merge"));

    ok(dir, &["retain", "main", "--min-snapshots", "1"]);
    ok(dir, &["maintain"]);
    assert_eq!(sql(dir, "select count(*) as n from t", "csv"), "n\n200003\n");

    let mut timings = Vec::new();
    for _ in 0..5 {
        let began = Instant::now();
        sql(dir, "select 1", "csv");
        timings.push(began.elapsed());
    }
    println!("select 1, server up: {timings:?}");
}

#[test]
fn connect_from_subdirectory() {
    let lake = Lake::new();
    let below = lake.0.join("a/b");
    fs::create_dir_all(&below).unwrap();
    sql(&below, "create table t (k int); insert into t values (7)", "csv");
    assert_eq!(sql(&lake.0, "select k from t", "csv"), "k\n7\n");
}

#[test]
fn local_lake_lifecycle() {
    let lake = Lake::new();
    let dir = &lake.0;
    sql(dir, "create table t (k int); insert into t values (7)", "csv");

    assert!(ok(dir, &["stop", "--format", "csv"]).contains("lake"));
    ok(dir, &["stop"]);

    // A pipe read to its end, as `$(metalake ...)` does, while the server starts behind it and stays.
    let began = Instant::now();
    assert_eq!(sql(dir, "select k from t", "csv"), "k\n7\n");
    println!("select, server down: {:?}", began.elapsed());

    ok(dir, &["stop"]);
    fs::remove_dir_all(dir.join("lake")).unwrap();
}

#[test]
fn concurrent_commands_share_server() {
    let lake = Lake::new();
    let dir = &lake.0;
    sql(dir, "create table t (k int)", "csv");
    ok(dir, &["stop"]);

    let insert = |k: i32| {
        common::metalake(dir)
            .args(["sql", &format!("insert into t values ({k})"), "--format", "csv"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    };
    let racing: Vec<_> = (0..8).map(insert).collect();
    let mut applied = 0;
    for child in racing {
        let output = child.wait_with_output().unwrap();
        match output.status.code() {
            Some(0) => applied += 1,
            Some(3) => {}
            other => panic!("{other:?}: {}", text(&output.stderr)),
        }
    }
    assert!(applied > 0);
    assert_eq!(
        sql(dir, "select count(*) as n from t", "csv"),
        format!("n\n{applied}\n")
    );
}

/// `path` as a `file:` URL.
fn file_url(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    format!("file:///{}", path.trim_start_matches('/'))
}

#[test]
fn init_validates_warehouse() {
    let dir = directory();
    let elsewhere = directory();
    let url = file_url(&elsewhere);
    ok(&dir, &["init", "./lake", &url]);
    let lake = Lake(dir);
    assert!(ok(&lake.0, &["status"]).contains(&format!("warehouse  {url}")));
    assert_eq!(fs::read_dir(&elsewhere).unwrap().count(), 0, "the check leaves nothing");

    let again = run(&lake.0, &["init", "./lake"]);
    assert_eq!(again.status.code(), Some(1));
    assert!(ok(&lake.0, &["status"]).contains(&url), "the first still stands");

    // A store that cannot be written is said at once, with no cluster and no file left.
    let bad = directory();
    let blocked = bad.join("blocked");
    fs::write(&blocked, "a file where a directory goes").unwrap();
    let refused = run(&bad, &["init", "./lake", &file_url(&blocked)]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(
        text(&refused.stderr).contains("cannot be written"),
        "{}",
        text(&refused.stderr)
    );
    assert!(!bad.join("metalake.toml").exists() && !bad.join("lake").exists());

    // A cluster already there leaves no file pointing at it.
    let other = directory();
    fs::create_dir_all(other.join("lake/postgres")).unwrap();
    fs::write(other.join("lake/postgres/x"), "").unwrap();
    assert_eq!(run(&other, &["init", "./lake"]).status.code(), Some(1));
    assert!(!other.join("metalake.toml").exists());
}

#[test]
fn stop_refuses_external_postgres() {
    let dir = directory();
    fs::write(dir.join("metalake.toml"), "postgres = \"host=nowhere\"\n").unwrap();
    let refused = run(&dir, &["stop"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(text(&refused.stderr).contains("no Postgres of its own"));

    let empty = directory();
    let without = run(&empty, &["init", "postgresql://u@h/db"]);
    assert_eq!(without.status.code(), Some(2), "{}", text(&without.stderr));
}
