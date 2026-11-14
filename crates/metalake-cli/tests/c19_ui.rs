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
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::ipc::reader::StreamReader;
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

    // A ref made, then deleted, which notifies nothing: the interval reads it, though another lake of the same
    // Postgres notifies the channel all the while, more often than the interval.
    ok(&dir, &["branch", "create", "gone"]);
    let made = events.next(Duration::from_secs(10)).expect("the new ref's mark");
    let (other, _) = lake();
    let busy = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let noisy = {
        let busy = busy.clone();
        std::thread::spawn(move || {
            sql(&other, "create table main.default.n (k bigint);", "csv");
            while busy.load(std::sync::atomic::Ordering::Relaxed) {
                sql(&other, "insert into main.default.n values (1);", "csv");
            }
        })
    };
    ok(&dir, &["branch", "delete", "gone", "--yes"]);
    let deleted = events.next(Duration::from_secs(15)).expect("the deleted ref's mark");
    busy.store(false, std::sync::atomic::Ordering::Relaxed);
    noisy.join().unwrap();
    assert_eq!(
        deleted["deleted"].as_i64(),
        made["deleted"].as_i64().map(|d| d + 1),
        "{made} {deleted}"
    );
    ui.kill().unwrap();
    ui.wait().unwrap();
}

/// One `POST /api/v1/sql` from the UI's own origin: its status, its headers and its body.
fn run(port: u16, body: &str) -> (u16, String, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let request = format!(
        "POST /api/v1/sql HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://127.0.0.1:{port}\r\n\
         Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).unwrap();
    let end = answer.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&answer[..end]).into_owned();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, head.to_lowercase(), answer[end + 4..].to_vec())
}

#[test]
fn sql_on_branch_writes_and_reads() {
    let (dir, _) = lake();
    ok(&dir, &["branch", "create", "dev"]);
    let (mut ui, port) = started(&dir);

    // Names that say no ref are the branch's: the table is made and filled on `dev`.
    let made =
        r#"{"sql":"create table default.t (k bigint); insert into default.t values (3), (1), (2)","branch":"dev"}"#;
    let (code, _, body) = run(port, made);
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));

    let (code, head, body) = run(
        port,
        r#"{"sql":"select k from default.t order by k","branch":"dev","limit":2}"#,
    );
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    assert!(
        head.contains("content-type: application/vnd.apache.arrow.stream"),
        "{head}"
    );
    assert!(
        head.contains("metalake-rows: 2") && head.contains("metalake-truncated: true"),
        "{head}"
    );
    let reader = StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let column = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend(column.iter().map(Option::unwrap));
    }
    assert_eq!(values, [1, 2]);

    // All of it, within the limit, is not truncated.
    let (_, head, _) = run(port, r#"{"sql":"select k from dev.default.t"}"#);
    assert!(
        head.contains("metalake-rows: 3") && head.contains("metalake-truncated: false"),
        "{head}"
    );

    // `main` has no such table: said as the command line says it.
    let (code, _, body) = run(port, r#"{"sql":"select k from default.t"}"#);
    assert!(code >= 400, "{code}");
    let fault: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(fault["message"].as_str().unwrap().contains("t"), "{fault}");
    ui.kill().unwrap();
    ui.wait().unwrap();
}

/// The Catalog views' reads (ADR 0007): a ref with its schemas and what they hold, its commits, a schema with its
/// tables, a table with its columns and size, and its snapshots; and a tag, the lake as it was when tagged.
#[test]
fn catalog_details_of_ref_schema_and_table() {
    let (dir, _) = lake();
    let (mut ui, port) = started(&dir);
    let made = r#"{"sql":"create schema shop; create table shop.customers (id bigint not null, name varchar); insert into shop.customers values (1, 'ana'), (2, 'bob')"}"#;
    let (code, _, body) = run(port, made);
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    ok(&dir, &["tag", "create", "then"]);
    let (code, _, _) = run(port, r#"{"sql":"insert into shop.customers values (3, 'eva')"}"#);
    assert_eq!(code, 200);
    let read = |path: &str| -> serde_json::Value {
        let (code, body) = get(port, path, Some(TOKEN));
        assert_eq!(code, 200, "{path}: {body}");
        serde_json::from_str(&body).unwrap()
    };

    let main = read("/api/v1/refs/main");
    assert_eq!(
        (main["name"].as_str(), main["kind"].as_str()),
        (Some("main"), Some("branch"))
    );
    assert_eq!(main["size"]["tables"], 1);
    assert_eq!(main["size"]["rows"], 3);
    let shop = main["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["namespace"] == serde_json::json!(["shop"]))
        .unwrap();
    assert_eq!(shop["size"]["rows"], 3);

    let commits = read("/api/v1/refs/main/commits?limit=2");
    assert_eq!(commits.as_array().unwrap().len(), 2);
    assert_eq!(commits[0]["operation"], "append");

    let schema = read("/api/v1/refs/main/schemas/shop");
    assert_eq!(schema["tables"][0]["name"], "customers");
    assert_eq!(schema["tables"][0]["rows"], 3);
    assert!(schema["tables"][0]["changed_at"].is_string(), "{schema}");

    let table = read("/api/v1/refs/main/tables/shop/customers");
    let columns: Vec<_> = table["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].clone())
        .collect();
    assert_eq!(columns, ["id", "name"]);
    assert_eq!(table["columns"][0]["required"], true);
    assert_eq!(table["rows"], 3);
    assert!(table["bytes"].as_i64().unwrap() > 0, "{table}");
    assert!(table["location"].as_str().unwrap().contains("customers"), "{table}");

    let snapshots = read("/api/v1/refs/main/tables/shop/customers/snapshots");
    assert!(snapshots.as_array().unwrap().len() >= 2, "{snapshots}");

    // The tag: the lake as it was when tagged, two rows.
    let then = read("/api/v1/refs/then");
    assert_eq!(
        (then["kind"].as_str(), then["size"]["rows"].as_i64()),
        (Some("tag"), Some(2))
    );
    assert_eq!(read("/api/v1/refs/then/tables/shop/customers")["rows"], 2);

    // What does not exist is said, not answered empty.
    let (code, _) = get(port, "/api/v1/refs/main/schemas/nowhere", Some(TOKEN));
    assert!(code >= 400, "{code}");
    ui.kill().unwrap();
    ui.wait().unwrap();
}

/// The port a run of the UI's JupyterLab serves at, from the runtime file it writes in the run's own directory.
fn jupyter_port(ui: u32) -> Option<u16> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    let runs = std::path::PathBuf::from(home).join(".metalake").join("ui");
    for run in std::fs::read_dir(runs).ok()?.flatten() {
        if !run.file_name().to_string_lossy().starts_with(&format!("jupyter-{ui}-")) {
            continue;
        }
        for file in std::fs::read_dir(run.path().join("runtime")).ok()?.flatten() {
            let name = file.file_name().to_string_lossy().into_owned();
            if name.starts_with("jpserver-") && name.ends_with(".json") {
                let written: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(file.path()).ok()?).ok()?;
                return written["port"].as_u64().and_then(|p| u16::try_from(p).ok());
            }
        }
    }
    None
}

/// The Notebook's JupyterLab (ADR 0008, N2): started on the page's ask, its notebooks beside the lake's file, and ended
/// when `metalake ui` is killed, which asks it nothing. Where `METALAKE_TEST_JUPYTER_PYTHON` names a Python with
/// JupyterLab, as CI does.
#[test]
fn notebook_ends_with_ui() {
    let Some(python) = std::env::var_os("METALAKE_TEST_JUPYTER_PYTHON") else {
        return;
    };
    let (dir, _) = lake();
    let mut ui = metalake(&dir)
        .args(["ui", "--no-browser"])
        .env("METALAKE_UI_TOKEN", TOKEN)
        .env("METALAKE_PYTHON", &python)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut url = String::new();
    BufReader::new(ui.stdout.take().unwrap()).read_line(&mut url).unwrap();
    let port: u16 = url.trim()["http://127.0.0.1:".len()..]
        .split('/')
        .next()
        .unwrap()
        .parse()
        .unwrap();

    let (code, body) = post(port, "/api/v1/notebook/start");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains(r#""running":true"#), "{body}");
    assert!(dir.join("notebooks").is_dir(), "notebooks beside the lake's file");
    let jupyter = jupyter_port(ui.id()).expect("its runtime file");
    assert!(TcpStream::connect(("127.0.0.1", jupyter)).is_ok());

    ui.kill().unwrap();
    ui.wait().unwrap();
    let start = Instant::now();
    while TcpStream::connect(("127.0.0.1", jupyter)).is_ok() {
        assert!(start.elapsed() < Duration::from_secs(20), "JupyterLab outlived the UI");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A `POST` with no body from the UI's own origin: its status and its body.
fn post(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://127.0.0.1:{port}\r\n\
         Authorization: Bearer {TOKEN}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let status = answer.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = answer
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}
