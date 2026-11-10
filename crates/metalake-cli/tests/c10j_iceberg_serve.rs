//! C10j: `metalake iceberg serve`, the lake's Iceberg REST server run by the command line. On a free port it says where
//! it listens, in its first line and in its runtime file; it answers Iceberg's routes; a request bearing its token stops
//! it, and one without does not; and it removes its runtime file as it stops.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{lake, metalake};
use metalake_client::{Runtime, SHUTDOWN_TOKEN, VERSION};

/// The status code of one request.
fn status(port: u16, request: &str) -> u16 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    answer.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn shutdown(port: u16, token: &str) -> u16 {
    status(
        port,
        &format!(
            "POST /metalake/shutdown HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n"
        ),
    )
}

#[test]
fn served_found_and_stopped() {
    let (dir, _) = lake();
    let file = dir.join("server.json");
    let mut server = metalake(&dir)
        .args(["iceberg", "serve", "--listen", "127.0.0.1:0", "--materialize", "off"])
        .arg("--runtime-file")
        .arg(&file)
        .env(SHUTDOWN_TOKEN, "the-token")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    // Its first line says where it listens, and its file says the same, once it does.
    let mut first = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut first)
        .unwrap();
    let runtime = Runtime::read(&file).expect("a runtime file once it listens");
    assert_eq!(first.trim(), format!("listening on 127.0.0.1:{}", runtime.port));
    assert_eq!(runtime.url, format!("http://127.0.0.1:{}", runtime.port));
    assert_eq!((runtime.pid, runtime.version.as_str()), (server.id(), VERSION));
    assert_eq!(runtime.token, "the-token");

    let config = "GET /v1/config HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    assert_eq!(status(runtime.port, config), 200);
    assert_eq!(shutdown(runtime.port, "not-the-token"), 401);
    assert_eq!(status(runtime.port, config), 200, "still serving");

    assert_eq!(shutdown(runtime.port, &runtime.token), 202);
    let since = Instant::now();
    let exit = loop {
        if let Some(exit) = server.try_wait().unwrap() {
            break exit;
        }
        assert!(since.elapsed() < Duration::from_secs(20), "it stops");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(exit.success(), "{exit:?}");
    assert!(!file.exists(), "its runtime file goes with it");
}

#[test]
fn a_runtime_file_needs_a_token() {
    let (dir, _) = lake();
    let output = metalake(&dir)
        .args(["iceberg", "serve", "--listen", "127.0.0.1:0", "--materialize", "off"])
        .arg("--runtime-file")
        .arg(dir.join("server.json"))
        .env_remove(SHUTDOWN_TOKEN)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(said.contains(SHUTDOWN_TOKEN), "{said}");
}
