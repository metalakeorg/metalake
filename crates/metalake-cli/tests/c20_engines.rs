//! `metalake engine` (the engines a lake's faces send statements to): named, listed and forgotten in the lake's file,
//! reaching nothing, so that an engine is named before or after it, or the lake's Iceberg server, runs. Run by
//! `CRATE=metalake-cli bash sql/test.sh --test c20_engines`.

mod common;

use std::fs;

use common::{directory, ok, run, text};
use serde_json::Value;

/// A lake's file whose catalog is nowhere: what reaches no catalog still works on it.
fn unreachable_lake() -> std::path::PathBuf {
    let dir = directory();
    fs::write(
        dir.join("metalake.toml"),
        "# the team's lake\npostgres = \"postgresql://nobody@127.0.0.1:1/none\"\n",
    )
    .unwrap();
    dir
}

#[test]
fn an_engine_is_named_listed_and_forgotten_with_nothing_running() {
    let dir = unreachable_lake();
    let added = ok(
        &dir,
        &[
            "engine",
            "add",
            "trino_prod",
            "--url",
            "https://trino.example:8443",
            "--user",
            "ana",
            "--token-env",
            "TRINO_TOKEN",
            "--format",
            "json",
        ],
    );
    let row: Value = serde_json::from_str(added.lines().next().unwrap()).unwrap();
    assert_eq!(row["name"], "trino_prod");
    assert_eq!(row["type"], "trino");
    assert_eq!(row["auth"], "token from $TRINO_TOKEN");
    assert_eq!(row["catalog"], "lake");

    let file = fs::read_to_string(dir.join("metalake.toml")).unwrap();
    assert!(file.starts_with("# the team's lake\n"), "{file}");
    assert!(file.contains("[engines.trino_prod]"), "{file}");
    assert!(file.contains("token_env = \"TRINO_TOKEN\""), "{file}");

    // Below the lake's directory, as every command finds the lake.
    let below = dir.join("notebooks");
    fs::create_dir_all(&below).unwrap();
    let listed = ok(&below, &["engine", "list", "--format", "json"]);
    assert_eq!(listed.lines().count(), 1, "{listed}");

    let taken = run(
        &dir,
        &[
            "engine",
            "add",
            "trino_prod",
            "--url",
            "https://other:8443",
            "--user",
            "ana",
        ],
    );
    assert!(!taken.status.success());
    assert!(text(&taken.stderr).contains("--replace"), "{}", text(&taken.stderr));

    ok(&dir, &["engine", "remove", "trino_prod"]);
    assert_eq!(ok(&dir, &["engine", "list", "--format", "json"]).trim(), "");
    assert!(!run(&dir, &["engine", "remove", "trino_prod"]).status.success());
}

#[test]
fn what_could_never_be_asked_is_refused_before_it_is_written() {
    let dir = unreachable_lake();
    for args in [
        vec![
            "engine",
            "add",
            "Trino-Prod",
            "--url",
            "https://t:8443",
            "--user",
            "ana",
        ],
        vec!["engine", "add", "t", "--url", "ftp://t", "--user", "ana"],
        vec!["engine", "add", "t", "--url", "https://t:8443"],
        vec![
            "engine",
            "add",
            "t",
            "--type",
            "oracle",
            "--url",
            "https://t:8443",
            "--user",
            "ana",
        ],
    ] {
        let refused = run(&dir, &args);
        assert!(!refused.status.success(), "{args:?}");
    }
    let file = fs::read_to_string(dir.join("metalake.toml")).unwrap();
    assert!(!file.contains("engines"), "{file}");
}
