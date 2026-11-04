//! What the tests of the command line share: the real binary, run as a person or an agent runs it.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};

static NEXT: AtomicU32 = AtomicU32::new(0);

pub fn unique() -> String {
    format!("cli_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

pub fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-cli bash sql/test.sh")
}

/// A new directory, of its own, for a test.
pub fn directory() -> PathBuf {
    let dir = std::env::temp_dir().join(unique());
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// The binary, run in `dir`, with none of the variables that would choose a lake for it.
pub fn metalake(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_metalake"));
    command
        .current_dir(dir)
        .env_remove("METALAKE_POSTGRES")
        .env_remove("METALAKE_SCHEMA")
        .env_remove("PGPASSWORD");
    command
}

pub fn run(dir: &Path, args: &[&str]) -> Output {
    metalake(dir).args(args).output().unwrap()
}

pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Runs a command that must succeed, for its stdout.
pub fn ok(dir: &Path, args: &[&str]) -> String {
    let output = run(dir, args);
    assert!(output.status.success(), "{args:?}: {}", text(&output.stderr));
    assert!(output.stderr.is_empty(), "{args:?}: {}", text(&output.stderr));
    text(&output.stdout)
}

/// A new lake in a new directory, and its schema.
pub fn lake() -> (PathBuf, String) {
    let dir = directory();
    let schema = unique();
    ok(&dir, &["init", &url(), "data", "--schema", &schema]);
    (dir, schema)
}

/// `metalake sql`, which must succeed, for its stdout.
pub fn sql(dir: &Path, statements: &str, format: &str) -> String {
    ok(dir, &["sql", statements, "--format", format])
}

/// A lake with a table of two rows, one with a null, which small enough stay in the catalog.
pub fn table() -> PathBuf {
    let (dir, _) = lake();
    sql(
        &dir,
        "create schema main.db;
         create table main.db.t (k bigint not null, s varchar, f double);
         insert into main.db.t values (1, 'a', 0.5), (2, null, 1.5);",
        "csv",
    );
    dir
}
