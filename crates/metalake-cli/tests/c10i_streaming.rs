//! C10i: a table's changes and a stream into it, from the command line. `changes` prints a range of commits; `follow`
//! prints them as they come, in JSON lines to a program, keeps its position in a file and goes on after it; `ingest`
//! writes lines from stdin, and with a key a run given the same input again writes no row twice.

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Child, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::{metalake, ok, run, sql, table, text};
use serde_json::Value;

/// The commit sequences of `main`, the latest last.
fn commits(dir: &Path) -> Vec<i64> {
    let log = ok(dir, &["log", "--format", "json", "--limit", "100"]);
    let mut seqs: Vec<i64> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|v| {
            v["commit_seq"]
                .as_str()
                .map_or_else(|| v["commit_seq"].as_i64().unwrap(), |s| s.parse().unwrap())
        })
        .collect();
    seqs.sort_unstable();
    seqs
}

/// A commit reaches a follower once it is a second old (CHG-1).
fn settle() {
    thread::sleep(Duration::from_millis(1200));
}

/// Each JSON line's `(change, key)`, sorted.
fn changes(json: &str) -> Vec<(String, i64)> {
    let mut rows: Vec<(String, i64)> = json
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|v| {
            (
                v["_change_type"].as_str().unwrap().to_string(),
                v["k"].as_i64().unwrap(),
            )
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn changes_prints_range() {
    let dir = table();
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");
    sql(&dir, "delete from main.db.t where k = 1", "csv");
    settle();
    let seqs = commits(&dir);
    let (deleted, added) = (seqs[seqs.len() - 1], seqs[seqs.len() - 2]);
    let before = seqs[seqs.len() - 3];

    let range = ok(
        &dir,
        &[
            "changes",
            "db.t",
            "--after",
            &before.to_string(),
            "--until",
            &added.to_string(),
            "--format",
            "json",
        ],
    );
    assert_eq!(changes(&range), [("insert".to_string(), 3)]);
    let rest = ok(
        &dir,
        &["changes", "db.t", "--after", &added.to_string(), "--format", "json"],
    );
    assert_eq!(changes(&rest), [("delete".to_string(), 1)]);

    let backwards = run(
        &dir,
        &[
            "changes",
            "db.t",
            "--after",
            &deleted.to_string(),
            "--until",
            &before.to_string(),
        ],
    );
    assert!(!backwards.status.success(), "{}", text(&backwards.stdout));
}

/// Starts `metalake follow`, reading its stdout as it comes.
fn follow(dir: &Path, args: &[&str]) -> Child {
    metalake(dir)
        .arg("follow")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Lets a follower run for a while, then stops it, for what it printed.
fn stopped(mut child: Child, after: Duration) -> (String, String) {
    let end = Instant::now() + after;
    while Instant::now() < end && child.try_wait().unwrap().is_none() {
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    (text(&output.stdout), text(&output.stderr))
}

#[test]
fn follow_keeps_position_file() {
    let dir = table();
    let file = dir.join("t.position");
    let file_arg = file.to_string_lossy().to_string();

    // What the table holds, in JSON lines to a pipe, and the position written after it.
    let (printed, _) = stopped(
        follow(&dir, &["db.t", "--position-file", &file_arg, "--interval", "100ms"]),
        Duration::from_secs(3),
    );
    assert_eq!(
        changes(&printed),
        [("insert".to_string(), 1), ("insert".to_string(), 2)]
    );
    let kept: i64 = std::fs::read_to_string(&file).unwrap().trim().parse().unwrap();

    // Started again with the file, it prints only what came after.
    sql(&dir, "insert into main.db.t values (3, 'c', 2.5)", "csv");
    let (printed, _) = stopped(
        follow(&dir, &["db.t", "--position-file", &file_arg, "--interval", "100ms"]),
        Duration::from_secs(4),
    );
    assert_eq!(changes(&printed), [("insert".to_string(), 3)]);
    let now: i64 = std::fs::read_to_string(&file).unwrap().trim().parse().unwrap();
    assert!(now > kept);
}

#[test]
fn follow_refusals() {
    let dir = table();
    for args in [
        vec!["follow", "db.t", "--after", "1", "--latest"],
        vec!["follow", "db.t", "--since", "yesterday"],
        vec!["follow", "db.t", "--interval", "fast"],
    ] {
        let refused = run(&dir, &args);
        assert!(!refused.status.success(), "{args:?}");
    }
}

/// `metalake ingest` with `input` on its stdin, for its output.
fn ingest(dir: &Path, args: &[&str], input: &str) -> std::process::Output {
    let mut child = metalake(dir)
        .arg("ingest")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
    child.wait_with_output().unwrap()
}

fn keys(dir: &Path) -> Vec<i64> {
    let csv = sql(dir, "select k from main.db.t order by k", "csv");
    csv.lines().skip(1).map(|l| l.parse().unwrap()).collect()
}

#[test]
fn ingest_json_with_key_once() {
    let dir = table();
    let input = "{\"k\": 10, \"s\": \"x\"}\n{\"k\": 11}\n\n{\"k\": 12, \"f\": 1.0}\n";
    let args = ["db.t", "--app-id", "load", "--batch-lines", "2"];
    let first = ingest(&dir, &args, input);
    assert!(first.status.success(), "{}", text(&first.stderr));
    assert_eq!(keys(&dir), [1, 2, 10, 11, 12]);

    // The same input again, in other batches: the key says where the last run left it, and no row comes twice.
    let again = ingest(
        &dir,
        &["db.t", "--app-id", "load", "--batch-lines", "3"],
        &format!("{input}{{\"k\": 13}}\n"),
    );
    assert!(again.status.success(), "{}", text(&again.stderr));
    assert_eq!(keys(&dir), [1, 2, 10, 11, 12, 13]);
}

#[test]
fn ingest_csv_by_version_column() {
    let dir = table();
    let input = "s,k,f\nx,20,1\ny,21,2\n";
    let args = ["db.t", "--input", "csv", "--app-id", "csv", "--version-column", "k"];
    assert!(ingest(&dir, &args, input).status.success());
    assert!(ingest(&dir, &args, "s,k,f\ny,21,2\nz,22,3\n").status.success());
    assert_eq!(keys(&dir), [1, 2, 20, 21, 22]);

    let unknown = ingest(&dir, &["db.t", "--input", "csv"], "nope\n1\n");
    assert!(!unknown.status.success(), "a header names the table's columns");
}

#[test]
fn ingest_upsert_by_key() {
    let dir = table();
    let output = ingest(
        &dir,
        &["db.t", "--upsert-on", "k"],
        "{\"k\": 1, \"s\": \"one\"}\n{\"k\": 3}\n",
    );
    assert!(output.status.success(), "{}", text(&output.stderr));
    let rows = sql(&dir, "select k, s from main.db.t order by k", "csv");
    assert_eq!(rows.lines().skip(1).collect::<Vec<_>>(), ["1,one", "2,", "3,"]);
}

/// `metalake ingest` fed line by line, with a pause after each line that has one, for its output.
fn ingest_slowly(dir: &Path, args: &[&str], lines: &[(&str, Duration)]) -> std::process::Output {
    let mut child = metalake(dir)
        .arg("ingest")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for (line, pause) in lines {
        if stdin.write_all(format!("{line}\n").as_bytes()).is_err() {
            break;
        }
        stdin.flush().unwrap();
        thread::sleep(*pause);
    }
    drop(stdin);
    child.wait_with_output().unwrap()
}

#[test]
fn ingest_quiet_input_commits_what_came() {
    let dir = table();
    let mut child = metalake(&dir)
        .args(["ingest", "db.t", "--commit-every", "200ms"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"{\"k\": 30}\n").unwrap();
    stdin.flush().unwrap();
    // One line, far from a full batch, and nothing after it: it is committed while the input stays open.
    thread::sleep(Duration::from_secs(2));
    assert_eq!(keys(&dir), [1, 2, 30]);
    drop(stdin);
    assert!(child.wait_with_output().unwrap().status.success());
}

#[test]
fn ingest_version_spanning_batches_whole() {
    let dir = table();
    let pause = Duration::from_millis(600);
    let args = [
        "db.t",
        "--app-id",
        "spans",
        "--version-column",
        "k",
        "--batch-lines",
        "1",
        "--commit-every",
        "200ms",
    ];
    let output = ingest_slowly(
        &dir,
        &args,
        &[
            ("{\"k\": 7, \"s\": \"a\"}", pause),
            ("{\"k\": 7, \"s\": \"b\"}", pause),
            ("{\"k\": 8, \"s\": \"c\"}", Duration::ZERO),
        ],
    );
    assert!(output.status.success(), "{}", text(&output.stderr));
    let rows = sql(&dir, "select k, s from main.db.t where k > 2 order by k, s", "csv");
    assert_eq!(
        rows.lines().skip(1).collect::<Vec<_>>(),
        ["7,a", "7,b", "8,c"],
        "a timer splits no version"
    );
}

#[test]
fn ingest_version_going_back_refused() {
    let dir = table();
    let pause = Duration::from_millis(600);
    let args = [
        "db.t",
        "--app-id",
        "back",
        "--version-column",
        "k",
        "--batch-lines",
        "1",
        "--commit-every",
        "200ms",
    ];
    let output = ingest_slowly(&dir, &args, &[("{\"k\": 10}", pause), ("{\"k\": 9}", pause)]);
    assert!(!output.status.success());
    assert!(
        text(&output.stderr).contains("do not go back"),
        "{}",
        text(&output.stderr)
    );
    assert!(
        !keys(&dir).contains(&9),
        "a version that goes back is refused, not written"
    );
}

#[test]
fn follow_csv_position_after_printed() {
    let dir = table();
    let file = dir.join("csv.position");
    let file_arg = file.to_string_lossy().to_string();
    let (printed, _) = stopped(
        follow(
            &dir,
            &[
                "db.t",
                "--format",
                "csv",
                "--position-file",
                &file_arg,
                "--interval",
                "100ms",
            ],
        ),
        Duration::from_secs(3),
    );
    // Killed with no chance to flush: what the file says was printed is in what was printed.
    let kept: i64 = std::fs::read_to_string(&file).unwrap().trim().parse().unwrap();
    let header: Vec<&str> = printed.lines().next().expect("a header").split(',').collect();
    let seq = header.iter().position(|c| *c == "_commit_seq").unwrap();
    let printed_seqs: Vec<i64> = printed
        .lines()
        .skip(1)
        .map(|line| line.split(',').nth(seq).unwrap().parse().unwrap())
        .collect();
    assert!(printed_seqs.contains(&kept), "{kept} not among {printed_seqs:?}");
}
