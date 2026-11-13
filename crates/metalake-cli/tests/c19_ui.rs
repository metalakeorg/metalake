//! C19: `metalake ui` on a lake (ADR 0007, milestone 6). It opens the lake as every verb does and serves at the lake's
//! own port, the same each run; its API says what the lake is, to the run's token alone; and a lake that cannot be
//! opened is said as every verb says it, before anything listens.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Stdio};

use common::{directory, lake, metalake, text};
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
