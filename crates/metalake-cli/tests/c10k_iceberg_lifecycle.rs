//! C10k: a lake's Iceberg server as the client ensures it, through the real binary: started detached, reused, one for
//! callers at once, replaced when it died or runs another version, outliving the process that started it, and stopped.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::lake;
use metalake_client::{Lake, Runtime, server};

/// The variable by which this test binary, run again, ensures a lake's server and exits.
const ENSURE_IN: &str = "METALAKE_TEST_ENSURE_IN";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// The lake in `dir`, with this crate's binary as how the client runs MetaLake.
fn open(dir: &Path) -> Lake {
    server::set_launcher(vec![env!("CARGO_BIN_EXE_metalake").into()]);
    runtime().block_on(Lake::open(Some(dir))).unwrap()
}

fn ensure(dir: &Path) -> Runtime {
    let lake = open(dir);
    runtime().block_on(lake.iceberg().ensure()).unwrap()
}

fn stop(dir: &Path) -> bool {
    let lake = open(dir);
    runtime().block_on(lake.iceberg().stop()).unwrap()
}

fn answers(runtime: &Runtime) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", runtime.port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let request = "GET /v1/config HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    answer.starts_with("HTTP/1.1 200")
}

fn kill(pid: u32) {
    let status = if cfg!(windows) {
        Command::new("taskkill").args(["/PID", &pid.to_string(), "/F"]).output()
    } else {
        Command::new("kill").args(["-9", &pid.to_string()]).output()
    };
    assert!(status.unwrap().status.success(), "the server {pid} could be killed");
}

fn until(what: &str, done: impl Fn() -> bool) {
    let since = Instant::now();
    while !done() {
        assert!(since.elapsed() < Duration::from_secs(30), "{what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The runtime file of the lake in `dir`, as the client keeps it.
fn file_of(dir: &Path) -> PathBuf {
    let runtime = ensure(dir);
    let files: Vec<PathBuf> = std::fs::read_dir(server::directory().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .filter(|path| Runtime::read(path).is_some_and(|r| r.pid == runtime.pid))
        .collect();
    assert_eq!(files.len(), 1, "{files:?}");
    files[0].clone()
}

#[test]
fn ensure_reuses_server() {
    let (dir, _) = lake();
    let file = file_of(&dir);
    let first = ensure(&dir);
    assert!(answers(&first));
    assert_eq!(ensure(&dir), first, "the one that runs");
    assert!(stop(&dir));
    // Stopped once it finished what it was doing, which it says by removing its file last.
    assert!(
        !file.exists(),
        "stop returns once the server has removed its runtime file"
    );
    assert!(!answers(&first));
    assert!(!stop(&dir), "none runs now");
}

#[test]
fn stop_during_start() {
    let (dir, _) = lake();
    // A server was started once, so its lock is there; then one starts while another stops.
    ensure(&dir);
    assert!(stop(&dir));
    let (started, stopped) = std::thread::scope(|scope| {
        let starting = scope.spawn(|| ensure(&dir));
        std::thread::sleep(Duration::from_millis(300));
        let stopped = stop(&dir);
        (starting.join().unwrap(), stopped)
    });
    assert!(stopped, "stop waits for the start under way, and stops what it started");
    assert!(!answers(&started));
}

#[test]
fn stop_external_lake_keeps_server() {
    let (dir, _) = lake();
    let running = ensure(&dir);
    // The lake's Postgres is not its own: `metalake stop` is refused, and stops nothing.
    let output = common::metalake(&dir).arg("stop").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("no Postgres of its own"));
    assert!(answers(&running), "its Iceberg server still runs");
    assert!(stop(&dir));
}

#[test]
fn ensure_concurrent_callers() {
    let (dir, _) = lake();
    let started: Vec<Runtime> = std::thread::scope(|scope| {
        let callers: Vec<_> = (0..4).map(|_| scope.spawn(|| ensure(&dir))).collect();
        callers.into_iter().map(|c| c.join().unwrap()).collect()
    });
    assert!(started.iter().all(|r| r == &started[0]), "{started:?}");
    assert!(stop(&dir));
}

#[test]
fn ensure_replaces_dead_server() {
    let (dir, _) = lake();
    let first = ensure(&dir);
    kill(first.pid);
    until("the killed server stops answering", || !answers(&first));
    let second = ensure(&dir);
    assert_ne!(second.pid, first.pid);
    assert!(answers(&second));
    assert!(stop(&dir));
}

#[test]
fn ensure_replaces_other_version() {
    let (dir, _) = lake();
    let file = file_of(&dir);
    let mut old = Runtime::read(&file).unwrap();
    old.version = "0.0.0-old".into();
    old.write(&file).unwrap();
    let new = ensure(&dir);
    assert_ne!(new.pid, old.pid, "a server of another version is replaced");
    assert!(!answers(&old));
    assert!(answers(&new));
    assert!(stop(&dir));
}

#[test]
fn server_outlives_starter() {
    let (dir, _) = lake();
    // This binary again, which ensures the lake's server and exits.
    let started = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ensure_and_exit", "--ignored", "--nocapture"])
        .env(ENSURE_IN, &dir)
        .output()
        .unwrap();
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stdout));
    let running = ensure(&dir);
    assert!(answers(&running), "it runs after the process that started it ended");
    let said = String::from_utf8_lossy(&started.stdout);
    assert!(
        said.contains(&format!("pid {}", running.pid)),
        "the same server: {said}"
    );
    assert!(stop(&dir));
}

/// Run by `server_outlives_starter` alone, in a process of its own.
#[test]
#[ignore]
fn ensure_and_exit() {
    let Some(dir) = std::env::var_os(ENSURE_IN) else {
        return;
    };
    let runtime = ensure(Path::new(&dir));
    println!("pid {}", runtime.pid);
}
