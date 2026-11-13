//! C19: `metalake ui` on a lake (ADR 0007). It opens the lake as every verb does and serves at the lake's own port, the
//! same each run; its API says what the lake is, its refs and a ref's tables, to the run's token alone, and its events
//! say the lake changed: at once for a commit, which notifies, and within the interval for a ref deleted, which does
//! not. A lake that cannot be opened is said as every verb says it, before anything listens.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use common::{directory, lake, metalake, ok, sql, text};
use metalake_client::server::UI_PORTS;

const TOKEN: &str = "c19c19c19c19c19c19c19c19c19c19c19c19c19c";

/// `metalake ui --no-browser` on the lake in `dir`, and the port its URL names, once it listens.
fn started(dir: &Path) -> (Child, u16) {
    let mut ui = metalake(dir)
        .args(["ui", "--no-browser"])
        .env("METALAKE_UI_TOKEN", TOKEN)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut url = String::new();
    BufReader::new(ui.stdout.take().unwrap()).read_line(&mut url).unwrap();
    let url = url.trim();
    assert!(url.ends_with(&format!("/#token={TOKEN}")), "{url}");
    let port = url["http://127.0.0.1:".len()..]
        .split('/')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    (ui, port)
}

/// One `GET`, with or without the token: its status and its body.
fn get(port: u16, path: &str, token: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let bearer = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{bearer}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let status = answer.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = answer
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

#[test]
fn status_of_lake_at_own_port() {
    let (dir, schema) = lake();
    let (mut ui, port) = started(&dir);
    assert!(UI_PORTS.contains(&port), "the lake's own port: {port}");

    assert_eq!(get(port, "/api/v1/status", None).0, 401, "without the token");
    let (code, body) = get(port, "/api/v1/status", Some(TOKEN));
    assert_eq!(code, 200, "{body}");
    let status: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status["schema"], schema.as_str(), "{status}");
    assert!(status["warehouse"].as_str().is_some(), "{status}");
    let refs = status["refs"].as_array().unwrap();
    assert!(
        refs.iter().any(|r| r["name"] == "main" && r["kind"] == "branch"),
        "{status}"
    );
    ui.kill().unwrap();
    ui.wait().unwrap();

    // The same lake, the same port: what the browser stored by origin is the lake's again.
    let (mut again, same) = started(&dir);
    assert_eq!(same, port);
    again.kill().unwrap();
    again.wait().unwrap();
}

#[test]
fn no_lake_said_before_listening() {
    let output = metalake(&directory())
        .args(["ui", "--no-browser"])
        .env("METALAKE_UI_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "no URL: {}", text(&output.stdout));
    assert!(
        text(&output.stderr).starts_with("metalake: "),
        "{}",
        text(&output.stderr)
    );
}

#[test]
fn refs_and_tables_of_branch() {
    let (dir, _) = lake();
    sql(
        &dir,
        "create schema main.sales; create table main.sales.orders (k bigint);",
        "csv",
    );
    ok(&dir, &["branch", "create", "dev"]);
    sql(&dir, "create table dev.default.only_dev (k bigint);", "csv");
    let (mut ui, port) = started(&dir);

    let (code, body) = get(port, "/api/v1/refs", Some(TOKEN));
    assert_eq!(code, 200, "{body}");
    let refs: serde_json::Value = serde_json::from_str(&body).unwrap();
    let dev = refs
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "dev")
        .expect(&body);
    assert_eq!(dev["parent"], "main", "{body}");

    let names = |on: &str| {
        let (code, body) = get(port, &format!("/api/v1/refs/{on}/tables"), Some(TOKEN));
        assert_eq!(code, 200, "{body}");
        let tables: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        let mut names: Vec<String> = tables
            .iter()
            .map(|t| {
                format!(
                    "{}.{}",
                    t["namespace"][0].as_str().unwrap(),
                    t["name"].as_str().unwrap()
                )
            })
            .collect();
        names.sort();
        names
    };
    assert_eq!(names("main"), ["sales.orders"]);
    assert_eq!(names("dev"), ["default.only_dev", "sales.orders"]);
    assert_eq!(get(port, "/api/v1/refs/nope/tables", Some(TOKEN)).0, 400);
    ui.kill().unwrap();
    ui.wait().unwrap();
}

/// The events of a run, read as they come over one connection.
struct Events(BufReader<TcpStream>);

impl Events {
    fn open(port: u16) -> Self {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let request =
            format!("GET /api/v1/events HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {TOKEN}\r\n\r\n");
        stream.write_all(request.as_bytes()).unwrap();
        stream.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
        Self(BufReader::new(stream))
    }

    /// The next event's data, as JSON, within `within`.
    fn next(&mut self, within: Duration) -> Option<serde_json::Value> {
        let start = Instant::now();
        let mut line = String::new();
        while start.elapsed() < within {
            line.clear();
            match self.0.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_) => {
                    if let Some(data) = line.trim_end().strip_prefix("data: ") {
                        return Some(serde_json::from_str(data).unwrap());
                    }
                }
                Err(_) => continue,
            }
        }
        None
    }
}

#[test]
fn events_say_lake_changed() {
    let (dir, _) = lake();
    let (mut ui, port) = started(&dir);
    let mut events = Events::open(port);
    let first = events.next(Duration::from_secs(10)).expect("the mark now");

    // A commit notifies: its mark comes before the interval of 5 s would have read it.
    let committed = Instant::now();
    sql(&dir, "create table main.default.t (k bigint);", "csv");
    let after = events.next(Duration::from_secs(10)).expect("the commit's mark");
    assert!(after["seq"].as_i64() > first["seq"].as_i64(), "{first} {after}");
    assert!(
        committed.elapsed() < Duration::from_secs(4),
        "notified, not polled: {:?}",
        committed.elapsed()
    );

    // A ref made, then deleted, which notifies nothing: the interval reads it.
    ok(&dir, &["branch", "create", "gone"]);
    let made = events.next(Duration::from_secs(10)).expect("the new ref's mark");
    ok(&dir, &["branch", "delete", "gone", "--yes"]);
    let deleted = events.next(Duration::from_secs(10)).expect("the deleted ref's mark");
    assert_eq!(
        deleted["deleted"].as_i64(),
        made["deleted"].as_i64().map(|d| d + 1),
        "{made} {deleted}"
    );
    ui.kill().unwrap();
    ui.wait().unwrap();
}
