//! C10a (ADR 0003 §7): the command line, as a person or an agent runs it: the real binary, with its stdout, its stderr
//! and its exit code. Run by `CRATE=metalake-cli bash sql/test.sh`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::Value;

static NEXT: AtomicU32 = AtomicU32::new(0);

fn unique() -> String {
    format!("cli_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-cli bash sql/test.sh")
}

/// A new directory, of its own, for a test.
fn directory() -> PathBuf {
    let dir = std::env::temp_dir().join(unique());
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// The binary, run in `dir`, with none of the variables that would choose a lake for it.
fn metalake(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_metalake"));
    command
        .current_dir(dir)
        .env_remove("METALAKE_POSTGRES")
        .env_remove("METALAKE_SCHEMA")
        .env_remove("PGPASSWORD");
    command
}

fn run(dir: &Path, args: &[&str]) -> Output {
    metalake(dir).args(args).output().unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Runs a command that must succeed, for its stdout.
fn ok(dir: &Path, args: &[&str]) -> String {
    let output = run(dir, args);
    assert!(output.status.success(), "{args:?}: {}", text(&output.stderr));
    assert!(output.stderr.is_empty(), "{args:?}: {}", text(&output.stderr));
    text(&output.stdout)
}

/// A new lake in a new directory, and its schema.
fn lake() -> (PathBuf, String) {
    let dir = directory();
    let schema = unique();
    ok(&dir, &["init", &url(), "data", "--schema", &schema]);
    (dir, schema)
}

#[test]
fn a_lake_is_found_from_below_and_says_what_it_is() {
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
fn the_environment_names_a_lake_with_no_file() {
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
fn a_password_is_refused_and_left_nowhere() {
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
fn a_lake_is_not_created_twice() {
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
fn a_failure_is_on_stderr_with_its_code() {
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
