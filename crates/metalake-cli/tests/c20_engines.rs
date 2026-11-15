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

/// A Trino the lake names, end to end as a person sets it up: the lake's server run with `iceberg serve`, its profile
/// for that server written by `iceberg connect --uri` and given to Trino, the engine named, and `engine check` holding
/// on both links once Trino runs; a catalog Trino does not have fails the second link alone. Needs Docker and Trino's
/// image, as the `engines` workflow has: `METALAKE_TEST_ENGINES=trino`.
#[test]
fn a_trino_named_and_checked_end_to_end() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let wanted = std::env::var("METALAKE_TEST_ENGINES").unwrap_or_default();
    if !wanted.split(',').any(|e| e == "trino") {
        return;
    }
    let dir = common::table();
    let mut server = common::metalake(&dir)
        .args(["iceberg", "serve", "--listen", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let address = line.trim().strip_prefix("listening on ").expect(&line).to_string();

    let profile = ok(
        &dir,
        &["iceberg", "connect", "trino", "--uri", &format!("http://{address}")],
    );
    let catalog = directory();
    fs::write(catalog.join("lake.properties"), profile).unwrap();
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            "metalake-trino",
            "--network",
            "host",
            "--user",
            "root",
            "-v",
        ])
        .arg(format!("{}:/etc/trino/catalog", catalog.display()))
        .args(["-v", "/tmp:/tmp", "trinodb/trino:484"])
        .output()
        .unwrap();
    assert!(started.status.success(), "{}", text(&started.stderr));

    let checked = std::panic::catch_unwind(|| {
        ok(
            &dir,
            &[
                "engine",
                "add",
                "trino_test",
                "--url",
                "http://127.0.0.1:8080",
                "--user",
                "metalake",
            ],
        );
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut checked = run(&dir, &["engine", "check", "trino_test", "--format", "json"]);
        while !checked.status.success() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_secs(2));
            checked = run(&dir, &["engine", "check", "trino_test", "--format", "json"]);
        }
        assert!(
            checked.status.success(),
            "{}{}",
            text(&checked.stdout),
            text(&checked.stderr)
        );
        let links: Vec<Value> = text(&checked.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(links.iter().all(|l| l["holds"] == true), "{links:?}");
        assert!(
            links[1]["said"].as_str().unwrap().contains("sees the lake"),
            "{links:?}"
        );

        ok(
            &dir,
            &[
                "engine",
                "add",
                "trino_elsewhere",
                "--url",
                "http://127.0.0.1:8080",
                "--user",
                "metalake",
                "--catalog",
                "elsewhere",
            ],
        );
        let elsewhere = run(&dir, &["engine", "check", "trino_elsewhere", "--format", "json"]);
        assert!(!elsewhere.status.success());
        let links: Vec<Value> = text(&elsewhere.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            (links[0]["holds"].as_bool(), links[1]["holds"].as_bool()),
            (Some(true), Some(false)),
            "{links:?}"
        );
    });
    let _ = Command::new("docker").args(["rm", "-f", "metalake-trino"]).output();
    let _ = server.kill();
    let _ = server.wait();
    let _ = fs::remove_dir_all(&catalog);
    if let Err(failed) = checked {
        std::panic::resume_unwind(failed);
    }
}
