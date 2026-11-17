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

    // No token here: the profile reads the lake, as `connect` says apart on stderr.
    let connected = run(
        &dir,
        &["iceberg", "connect", "trino", "--uri", &format!("http://{address}")],
    );
    assert!(connected.status.success(), "{}", text(&connected.stderr));
    assert!(
        text(&connected.stderr).contains("does not write it"),
        "{}",
        text(&connected.stderr)
    );
    let profile = text(&connected.stdout);
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

        // A statement sent to it by `USING`, as Query and `lake.sql` send one: Trino reads the lake's table.
        let counted = ok(
            &dir,
            &[
                "sql",
                "using trino_test select count(*) as n from db.t",
                "--format",
                "json",
            ],
        );
        let row: Value = serde_json::from_str(counted.lines().next().unwrap()).unwrap();
        assert_eq!(row["n"], 2, "{counted}");

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

/// A Spark the lake names, end to end: the lake's server run with `iceberg serve`, a Spark Connect 3.5 server with
/// Iceberg's runtime on its classpath and nothing of the lake's, and the engine named with where Spark reaches the
/// lake's server. `engine check` holds on both links; `USING` reads the lake's table by the name MetaLake gives it,
/// writes it, and on a branch writes the branch alone. Needs Docker and Spark's image, as the `engines` workflow has:
/// `METALAKE_TEST_ENGINES=spark_connect` (`spark` alone is PySpark's, in `c12_engines`).
#[test]
fn a_spark_named_and_checked_end_to_end() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let wanted = std::env::var("METALAKE_TEST_ENGINES").unwrap_or_default();
    if !wanted.split(',').any(|e| e == "spark_connect") {
        return;
    }
    // The token the lake's server is started with, which Spark bears to write.
    let token = "spark-end-to-end";
    let with_token = |dir: &std::path::Path, args: &[&str]| {
        common::metalake(dir)
            .env("METALAKE_ICEBERG_TOKEN", token)
            .args(args)
            .output()
            .unwrap()
    };
    let dir = common::table();
    let mut server = common::metalake(&dir)
        .env("METALAKE_ICEBERG_TOKEN", token)
        .args(["iceberg", "serve", "--listen", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let address = line.trim().strip_prefix("listening on ").expect(&line).to_string();

    let ivy = std::path::PathBuf::from(std::env::var("HOME").unwrap()).join(".ivy2");
    fs::create_dir_all(&ivy).unwrap();
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            "metalake-spark-connect",
            "--network",
            "host",
            "--user",
            "root",
            "-e",
            "SPARK_NO_DAEMONIZE=1",
            "-v",
            "/tmp:/tmp",
            "-v",
        ])
        .arg(format!("{}:/root/.ivy2", ivy.display()))
        .args([
            "--entrypoint",
            "/opt/spark/sbin/start-connect-server.sh",
            "spark:3.5.9-scala2.12-java17-ubuntu",
            "--packages",
            "org.apache.spark:spark-connect_2.12:3.5.9,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
            "--conf",
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf",
            "spark.jars.ivy=/root/.ivy2",
        ])
        .output()
        .unwrap();
    assert!(started.status.success(), "{}", text(&started.stderr));

    let checked = std::panic::catch_unwind(|| {
        ok(
            &dir,
            &[
                "engine",
                "add",
                "spark_test",
                "--type",
                "spark",
                "--url",
                "sc://127.0.0.1:15002",
                "--lake-url",
                &format!("http://{address}"),
            ],
        );
        let deadline = Instant::now() + Duration::from_secs(420);
        let check = || with_token(&dir, &["engine", "check", "spark_test", "--format", "json"]);
        let mut checked = check();
        while !checked.status.success() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_secs(3));
            checked = check();
        }
        let logs = || {
            text(
                &Command::new("docker")
                    .args(["logs", "metalake-spark-connect"])
                    .output()
                    .unwrap()
                    .stderr,
            )
        };
        assert!(
            checked.status.success(),
            "{}{}\n{}",
            text(&checked.stdout),
            text(&checked.stderr),
            logs()
        );
        let links: Vec<Value> = text(&checked.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(links.iter().all(|l| l["holds"] == true), "{links:?}");
        assert!(
            links[1]["said"].as_str().unwrap().contains("reaches the lake"),
            "{links:?}"
        );

        let last = |args: &[&str]| -> Value {
            let output = with_token(&dir, args);
            assert!(
                output.status.success(),
                "{args:?}: {}\n{}",
                text(&output.stderr),
                logs()
            );
            let stdout = text(&output.stdout);
            serde_json::from_str(stdout.lines().last().expect(&stdout)).unwrap()
        };
        let count = "using spark_test select count(*) as n from default.db.t";

        // Read by the name MetaLake gives it, the catalog first.
        assert_eq!(last(&["sql", count, "--format", "json"])["n"], 2);

        // Written by Spark, read by MetaLake.
        // Spark's INSERT answers no rows.
        let inserted = with_token(
            &dir,
            &["sql", "using spark_test insert into default.db.t values (3, 'c', 2.5)"],
        );
        assert!(
            inserted.status.success(),
            "{}
{}",
            text(&inserted.stderr),
            logs()
        );
        assert_eq!(
            last(&["sql", "select count(*) as n from db.t", "--format", "json"])["n"],
            3
        );

        // On a branch, Spark writes the branch alone.
        let on_dev = format!(
            "create branch dev; use branch dev; \
             using spark_test insert into default.db.t values (4, 'd', 3.5); {count}"
        );
        assert_eq!(last(&["sql", &on_dev, "--format", "json"])["n"], 4);
        assert_eq!(last(&["sql", count, "--format", "json"])["n"], 3, "main is as it was");
    });
    let _ = Command::new("docker")
        .args(["rm", "-f", "metalake-spark-connect"])
        .output();
    let _ = server.kill();
    let _ = server.wait();
    if let Err(failed) = checked {
        std::panic::resume_unwind(failed);
    }
}
