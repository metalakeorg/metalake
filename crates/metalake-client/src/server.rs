//! A lake's Iceberg REST server, as a client finds it: the runtime file a running server writes once it listens, readable
//! by its user alone, which says where it is, which process it is, which version of MetaLake runs it, and the token that
//! stops it. One per lake on a machine, as Jupyter keeps one file per server.

use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use metalake::environment;
use serde::{Deserialize, Serialize};

use crate::hash::fnv;
use crate::{Error, Result};

/// How long a new server has to say where it listens.
const STARTING: Duration = Duration::from_secs(20);
/// How long a server has to finish what it is doing once asked to stop.
const STOPPING: Duration = Duration::from_secs(30);
/// How long a probe waits for an answer.
const PROBE: Duration = Duration::from_secs(2);
/// The lines of its log a server that failed to start says.
const TAIL: usize = 20;

/// How this process runs MetaLake's command line, as `metalake …`, when a face says: the command line itself, its own
/// executable; Python, `python -m metalake`, whose command line lives in the interpreter's extension.
static LAUNCHER: OnceLock<Vec<OsString>> = OnceLock::new();

/// The variable that names MetaLake's command line for a program that links this client and is not it.
pub const METALAKE_BIN: &str = "METALAKE_BIN";

/// Says how to run MetaLake's command line from this process, once; later calls change nothing.
pub fn set_launcher(command: Vec<OsString>) {
    let _ = LAUNCHER.set(command);
}

/// How to run MetaLake's command line: as a face said; otherwise, for any program that links this client, what
/// `METALAKE_BIN` names or `metalake` on the `PATH`, and never the program itself, which may be anything.
fn launcher() -> Result<Vec<OsString>> {
    if let Some(command) = LAUNCHER.get() {
        return Ok(command.clone());
    }
    let found = resolve(std::env::var_os(METALAKE_BIN), std::env::var_os("PATH"));
    found.map(|path| vec![path.into_os_string()]).ok_or_else(|| {
        Error::failed(format!(
            "the lake's Iceberg server runs as `metalake iceberg serve`, and no metalake was found: install it on the \
             PATH, or set {METALAKE_BIN} to it"
        ))
    })
}

/// MetaLake's command line: the file `bin` names, else `metalake` in a directory of `path`.
/// Absolute, as the server is started in another directory than this process's.
fn resolve(bin: Option<OsString>, path: Option<OsString>) -> Option<PathBuf> {
    if let Some(bin) = bin.filter(|bin| !bin.is_empty()) {
        return std::path::absolute(PathBuf::from(bin)).ok();
    }
    let name = format!("metalake{}", std::env::consts::EXE_SUFFIX);
    std::env::split_paths(&path?)
        .map(|directory| directory.join(&name))
        .find(|candidate| runnable(candidate))
        .and_then(|found| std::path::absolute(found).ok())
}

/// Whether a file of the `PATH` can be run, as the system's own lookup asks: on Unix, one without a permission to execute
/// is passed over for the next.
fn runnable(candidate: &Path) -> bool {
    let Ok(metadata) = fs::metadata(candidate) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    metadata.is_file()
}

/// A lake, as its server's files name it: by what names the lake, its own directory or its catalog without a password
/// (`identity`, which reaching it does not change), and its schema.
pub(crate) fn key(identity: &str, schema: &str) -> String {
    format!("{:016x}", fnv(&format!("{identity}\0{schema}")))
}

/// The ports a lake's server tries first, one per lake by what names it.
pub const PORTS: std::ops::Range<u16> = 18100..19000;

/// The port a lake's server tries first: the same each time it starts, so that a profile written out, Trino's catalog
/// file or Spark's defaults, still reaches it after a restart. If another holds it, the server takes a free one.
pub fn preferred_port(identity: &str, schema: &str) -> u16 {
    let count = u64::from(PORTS.end - PORTS.start);
    PORTS.start + (fnv(&format!("{identity}\0{schema}")) % count) as u16
}

/// The files of a lake's server in this user's directory: where it says it runs, the lock callers take turns by, and its
/// log.
struct Files {
    runtime: PathBuf,
    lock: PathBuf,
    log: PathBuf,
    token: PathBuf,
}

impl Files {
    fn of(key: &str) -> Result<Self> {
        let dir = directory()?;
        Ok(Self {
            runtime: dir.join(format!("{key}.json")),
            lock: dir.join(format!("{key}.lock")),
            log: dir.join(format!("{key}.log")),
            token: dir.join(format!("{key}.token")),
        })
    }

    /// The lock, held until it is dropped; the system lets go of it if this process dies.
    /// Its lock, taken: the first file a server leaves, so the directory is made here.
    fn locked(&self) -> Result<fs::File> {
        if let Some(dir) = self.lock.parent() {
            fs::create_dir_all(dir)?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock)?;
        file.lock()?;
        Ok(file)
    }

    fn tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(TAIL)..].join("\n")
    }
}

/// The status code of one request to a server on the loopback, or none if it does not answer in time.
fn request(port: u16, method: &str, path: &str, bearer: Option<&str>) -> Option<u16> {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, PROBE).ok()?;
    stream.set_read_timeout(Some(PROBE)).ok()?;
    stream.set_write_timeout(Some(PROBE)).ok()?;
    let authorization = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let message = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{authorization}Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(message.as_bytes()).ok()?;
    // The status line, read until it ends: a server may send its answer in pieces.
    let mut head = Vec::with_capacity(64);
    let mut piece = [0u8; 64];
    while !head.windows(2).any(|w| {
        w == b"
"
    }) && head.len() < 256
    {
        let read = stream.read(&mut piece).ok()?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&piece[..read]);
    }
    let line = String::from_utf8_lossy(&head);
    line.split_whitespace().nth(1)?.parse().ok()
}

/// Whether a server answers Iceberg's first route there.
fn answers(port: u16) -> bool {
    request(port, "GET", "/v1/config", None) == Some(200)
}

/// Asks a server to stop, and waits until it no longer answers.
/// Asks a server to stop, and waits until it has: until it removes its runtime file at `path`, the last it does once it
/// has finished what it was doing, its writes to the catalog among them. Not until it stops answering, which it does
/// first: what stops the lake's Postgres after it would cut that work short. A server that died without removing its
/// file has stopped once it no longer answers.
fn shut(runtime: &Runtime, path: &Path) -> Result<()> {
    request(runtime.port, "POST", "/metalake/shutdown", Some(&runtime.token));
    let since = Instant::now();
    let still = || Runtime::read(path).is_some_and(|on_file| on_file.start == runtime.start);
    while still() {
        if since.elapsed() > STOPPING {
            if answers(runtime.port) {
                return Err(Error::failed(format!(
                    "the lake's server at {} did not stop within {STOPPING:?}",
                    runtime.url
                )));
            }
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// The lake's server: the one that runs, if it answers and runs this version; otherwise a new one, started detached so
/// that it outlives this process, as the lake's own Postgres does. Callers take turns, so they start one between them.
/// `postgres` is the catalog's connection with its password, which the server takes from its environment alone;
/// `identity` names the lake ([`key`]).
pub(crate) fn ensure(identity: &str, postgres: &str, schema: &str, auth: &str) -> Result<Runtime> {
    let files = Files::of(&key(identity, schema))?;
    let _turn = files.locked()?;
    if let Some(running) = Runtime::read(&files.runtime) {
        if answers(running.port) {
            // One of another version, or that signs in otherwise than the lake now says, is replaced.
            if running.version == VERSION && running.auth == auth {
                return Ok(running);
            }
            shut(&running, &files.runtime)?;
        }
        let _ = fs::remove_file(&files.runtime);
    }
    let token = lake_token(&files.token)?;
    start(&files, postgres, schema, auth, preferred_port(identity, schema), &token)
}

/// The lake's token: made the first time a server is started for it, kept where this user alone reads it, and the same
/// at each start after, as the lake's port is, so that what an engine was given still serves after a restart. It stops
/// the server, and makes a request that bears it a writer's (REST-11).
fn lake_token(path: &Path) -> Result<String> {
    if let Ok(held) = fs::read_to_string(path)
        && !held.trim().is_empty()
    {
        return Ok(held.trim().to_string());
    }
    let token = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let partial = path.with_extension("partial");
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&partial)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(&partial, path)?;
    Ok(token)
}

/// The variable a server is told the start it is of by, which it writes in its runtime file.
pub const START: &str = "METALAKE_ICEBERG_START";

/// Starts a server, and waits until its runtime file names it and it answers.
fn start(files: &Files, postgres: &str, schema: &str, auth: &str, port: u16, token: &str) -> Result<Runtime> {
    let started = uuid::Uuid::new_v4().simple().to_string();
    let command = launcher()?;
    let (program, prefix) = command
        .split_first()
        .ok_or_else(|| Error::failed("no command to run MetaLake with"))?;
    let log = fs::File::create(&files.log)?;
    let mut serve = Command::new(program);
    serve
        .args(prefix)
        .args([
            "iceberg",
            "serve",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--or-any-port",
            "--runtime-file",
        ])
        .arg(&files.runtime)
        .env(environment::POSTGRES, postgres)
        .env(environment::SCHEMA, schema)
        .env(environment::POSTGRES_AUTH, auth)
        .env(SHUTDOWN_TOKEN, token)
        .env(START, &started)
        .current_dir(directory()?)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    // Its own group, so that a terminal's Ctrl-C, or its window closing, does not stop it with this process.
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut serve, 0);
    #[cfg(windows)]
    {
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        std::os::windows::process::CommandExt::creation_flags(&mut serve, DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    let spawned = {
        let _held = StdHandlesKept::now();
        serve.spawn()
    };
    let mut child = spawned.map_err(|e| Error::failed(format!("the lake's server could not start: {e}")))?;
    let since = Instant::now();
    loop {
        // Its start, new each time, names it: not its pid, which is another process's where the command run is a
        // launcher, as a Windows venv's python.exe is, which starts Python itself as its child.
        if let Some(runtime) = Runtime::read(&files.runtime)
            && runtime.start == started
            && answers(runtime.port)
        {
            // Reaped when it ends, by a thread of this process if it is still here then.
            thread::spawn(move || {
                let _ = child.wait();
            });
            return Ok(runtime);
        }
        if let Some(exit) = child.try_wait()? {
            return Err(Error::failed(format!(
                "the lake's server stopped as it started ({exit}):\n{}",
                files.tail()
            )));
        }
        if since.elapsed() > STARTING {
            let _ = child.kill();
            return Err(Error::failed(format!(
                "the lake's server did not start within {STARTING:?}:\n{}",
                files.tail()
            )));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// While it lives, this process's standard handles are not inherited by a process it starts. On Windows a child
/// inherits every inheritable handle of its parent, whatever its own standard handles are: a server started detached
/// would hold its starter's output open, and whoever reads that output to its end, a pipe of `metalake … | grep` or a
/// test runner, would wait as long as the server lives. Elsewhere a child inherits only what it is given.
struct StdHandlesKept {
    #[cfg(windows)]
    handles: Vec<windows_sys::Win32::Foundation::HANDLE>,
}

impl StdHandlesKept {
    fn now() -> Self {
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation};
            use windows_sys::Win32::System::Console::{
                GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
            };
            let mut handles = Vec::new();
            for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
                // SAFETY: the standard handles of this process, which it does not close; only their flag changes.
                let handle = unsafe { GetStdHandle(which) };
                if !handle.is_null()
                    && handle != INVALID_HANDLE_VALUE
                    && unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } != 0
                {
                    handles.push(handle);
                }
            }
            Self { handles }
        }
        #[cfg(not(windows))]
        Self {}
    }
}

impl Drop for StdHandlesKept {
    fn drop(&mut self) {
        #[cfg(windows)]
        for &handle in &self.handles {
            use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};
            // SAFETY: as in `now`, the flag put back on a handle this process still holds.
            unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) };
        }
    }
}

/// Stops the lake's server, if one runs, and says whether one did.
pub fn stop(identity: &str, schema: &str) -> Result<bool> {
    let files = Files::of(&key(identity, schema))?;
    // None was ever started, which takes the lock first: nothing to stop, and nothing left in this user's home by
    // asking. Otherwise its turn, after one that may be starting a server now.
    if !files.lock.exists() {
        return Ok(false);
    }
    let _turn = files.locked()?;
    let Some(running) = Runtime::read(&files.runtime) else {
        return Ok(false);
    };
    let ran = answers(running.port);
    if ran {
        shut(&running, &files.runtime)?;
    }
    remove_if_mine(&files.runtime, running.pid);
    Ok(ran)
}

/// The version of MetaLake a server runs, which a client compares with its own: a server of another version may serve
/// another rule.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The variable a server takes its shutdown token from, so that it never shows among a process's arguments.
pub const SHUTDOWN_TOKEN: &str = "METALAKE_ICEBERG_TOKEN";

/// What a running server says of itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runtime {
    /// What a client connects to, as `http://127.0.0.1:PORT`.
    pub url: String,
    pub port: u16,
    pub pid: u32,
    pub version: String,
    /// The lake's token: what a request to `POST /metalake/shutdown` bears to stop it, and what makes a request a
    /// writer's (REST-11).
    pub token: String,
    /// What names this start of it, new each time.
    #[serde(default)]
    pub start: String,
    /// How it signs in to the catalog, by name: `password` when a server's file of before said nothing.
    #[serde(default = "password")]
    pub auth: String,
}

fn password() -> String {
    "password".into()
}

impl Runtime {
    /// Writes it whole or not at all, readable by this user alone.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let partial = path.with_extension("partial");
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&partial)?;
        file.write_all(&serde_json::to_vec_pretty(self).map_err(|e| Error::failed(e.to_string()))?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&partial, path)?;
        Ok(())
    }

    /// What the file says, if there is one that reads.
    pub fn read(path: &Path) -> Option<Self> {
        let bytes = fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

/// Removes a runtime file, if it is still this server's: a server replaced meanwhile keeps its own.
pub fn remove_if_mine(path: &Path, pid: u32) {
    if Runtime::read(path).is_some_and(|runtime| runtime.pid == pid) {
        let _ = fs::remove_file(path);
    }
}

/// The directory of this user's servers' files.
pub fn directory() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| Error::failed("no home directory for the lake's server: set HOME"))?;
    Ok(PathBuf::from(home).join(".metalake").join("iceberg"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_file_round_trip() {
        let dir = std::env::temp_dir().join(format!("metalake-server-{}", uuid::Uuid::new_v4().simple()));
        let path = dir.join("lake.json");
        assert_eq!(Runtime::read(&path), None);
        let runtime = Runtime {
            url: "http://127.0.0.1:1234".into(),
            port: 1234,
            pid: 42,
            version: VERSION.into(),
            token: "t".into(),
            start: "s".into(),
            auth: "password".into(),
        };
        runtime.write(&path).unwrap();
        assert_eq!(Runtime::read(&path), Some(runtime.clone()));
        assert!(!path.with_extension("partial").exists());

        remove_if_mine(&path, 7);
        assert!(path.exists(), "another server's file stays");
        remove_if_mine(&path, 42);
        assert!(!path.exists());

        fs::write(&path, "not json").unwrap();
        assert_eq!(Runtime::read(&path), None, "a file that does not read is no server");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn lake_token_kept() {
        let dir = std::env::temp_dir().join(format!("metalake-token-{}", uuid::Uuid::new_v4().simple()));
        let path = dir.join("lake.token");
        let first = lake_token(&path).unwrap();
        assert_eq!(first.len(), 64, "{first}");
        assert_eq!(lake_token(&path).unwrap(), first, "the same at each start");
        assert_ne!(
            lake_token(&dir.join("other.token")).unwrap(),
            first,
            "each lake its own"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "readable by this user alone");
        }
        assert!(!path.with_extension("partial").exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn launcher_resolution() {
        let directory = std::env::temp_dir().join(format!("metalake-bin-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&directory).unwrap();
        let name = format!("metalake{}", std::env::consts::EXE_SUFFIX);
        let path = std::env::join_paths([directory.join("empty"), directory.clone()]).unwrap();

        assert_eq!(resolve(None, Some(path.clone())), None, "no metalake there");
        fs::write(directory.join(&name), b"").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file = directory.join(&name);
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(
                resolve(None, Some(path.clone())),
                None,
                "one that cannot be run is passed over"
            );
            fs::set_permissions(&file, fs::Permissions::from_mode(0o755)).unwrap();
        }
        assert_eq!(
            resolve(None, Some(path.clone())),
            Some(directory.join(&name)),
            "the first on the PATH"
        );
        let named = std::path::absolute("/opt/metalake").unwrap();
        assert_eq!(
            resolve(Some(named.clone().into_os_string()), Some(path)),
            Some(named),
            "METALAKE_BIN first"
        );
        let relative = resolve(Some("bin/metalake".into()), None).unwrap();
        assert!(
            relative.is_absolute(),
            "a relative one is made absolute: {}",
            relative.display()
        );
        assert_eq!(relative, std::env::current_dir().unwrap().join("bin/metalake"));
        assert_eq!(resolve(Some("".into()), None), None);
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn preferred_port_by_lake() {
        let one = preferred_port("local:/home/a/lake", "metalake");
        assert!(PORTS.contains(&one));
        assert_eq!(
            one,
            preferred_port("local:/home/a/lake", "metalake"),
            "the same each time"
        );
        let ports: std::collections::HashSet<_> = (0..50)
            .map(|i| preferred_port(&format!("local:/lake{i}"), "metalake"))
            .collect();
        assert!(ports.len() > 40, "lakes spread over the range: {}", ports.len());
    }

    #[test]
    fn key_by_identity_and_schema() {
        let one = key("catalog:host=db user=u dbname=x", "lake");
        assert_eq!(one, key("catalog:host=db user=u dbname=x", "lake"));
        assert_ne!(
            one,
            key("catalog:host=db user=u dbname=x", "other"),
            "a schema is a lake"
        );
        assert_ne!(one, key("catalog:host=db2 user=u dbname=x", "lake"));
        assert_eq!(one.len(), 16);
    }

    #[test]
    fn probe_closed_port() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        assert!(!answers(port), "nothing listens there now");
    }

    #[test]
    fn probe_status() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let served = thread::spawn(move || {
            for status in ["200 OK", "404 Not Found"] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 256];
                let _ = stream.read(&mut request).unwrap();
                write!(stream, "HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").unwrap();
            }
        });
        assert!(answers(port), "a status line sent in pieces");
        assert!(!answers(port), "another status is not Iceberg's config");
        served.join().unwrap();
    }
}
