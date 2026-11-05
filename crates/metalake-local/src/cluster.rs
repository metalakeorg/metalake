//! The cluster inside a lake, and the calls that start and stop it. Nothing but the tools of Postgres touches its data
//! directory.
//!
//! On Linux and macOS it listens on a socket in the lake, closed to other users, with no TCP. Windows has no such
//! sockets here, so it listens on the loopback, behind a password kept in the lake.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::{Binaries, Error, MAJOR, Result};

const WINDOWS: bool = cfg!(windows);
/// The role every connection uses: a lake on one machine has one (ADR 0003 §2.7).
const USER: &str = "metalake";
const DATABASE: &str = "postgres";
/// A socket's port, which only names its file.
const SOCKET_PORT: u16 = 5432;
/// The longest path of a socket, which macOS sets lowest, less the name of its file.
const SOCKET_DIR_MAX: usize = 103 - ".s.PGSQL.5432".len() - 1;
/// How many ports to try, as another process can take one between choosing it and binding it.
const PORT_ATTEMPTS: usize = 5;
/// How long `pg_ctl` waits, in seconds.
const WAIT: &str = "60";
/// How long a server may take to be ready, recovery included, and how often to look.
const READY_WAIT: Duration = Duration::from_secs(60);
const READY_POLL: Duration = Duration::from_millis(20);
/// Written once: small, since this serves one lake on a machine doing other things.
const SETTINGS: &str = "
shared_buffers = 16MB
max_connections = 20
track_commit_timestamp = on
timezone = 'UTC'
log_timezone = 'UTC'
include_if_exists = 'where.conf'
";

/// A lake's cluster.
#[derive(Debug, Clone)]
pub struct Cluster {
    binaries: Binaries,
    lake: PathBuf,
}

impl Cluster {
    /// A new cluster in `lake`, which is made if need be.
    pub fn init(lake: &Path, binaries: Binaries) -> Result<Self> {
        fs::create_dir_all(lake)?;
        let cluster = Self {
            binaries,
            lake: std::path::absolute(lake)?,
        };
        let mut initdb = cluster.binaries.tool("initdb");
        initdb
            .arg("-D")
            .arg(cluster.data())
            .args(["-U", USER, "-E", "UTF8", "--locale=C", "--data-checksums"]);
        if WINDOWS {
            fs::write(cluster.password_file(), Uuid::new_v4().simple().to_string())?;
            initdb
                .arg("--auth=scram-sha-256")
                .arg("--pwfile")
                .arg(cluster.password_file());
        } else {
            initdb.arg("--auth=trust");
        }
        run("creating the lake's Postgres", &mut initdb)?;
        let mut settings = OpenOptions::new()
            .append(true)
            .open(cluster.data().join("postgresql.conf"))?;
        settings.write_all(SETTINGS.as_bytes())?;
        Ok(cluster)
    }

    /// The cluster already in `lake`, refused if these binaries cannot run it.
    pub fn open(lake: &Path, binaries: Binaries) -> Result<Self> {
        let cluster = Self {
            binaries,
            lake: std::path::absolute(lake)?,
        };
        let version = fs::read_to_string(cluster.data().join("PG_VERSION"))?;
        match version.trim() {
            found if found == MAJOR.to_string() => Ok(cluster),
            found => Err(Error::Version {
                found: found.to_string(),
            }),
        }
    }

    /// Whether the cluster's server is up.
    pub fn is_running(&self) -> Result<bool> {
        let status = self
            .binaries
            .tool("pg_ctl")
            .arg("status")
            .arg("-D")
            .arg(self.data())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        Ok(status.success())
    }

    /// The server, up, and how to reach it. It stays up after the caller is gone: at rest it costs little, and starting
    /// it costs every command half a second where it is slowest. Callers take turns, so one of them starts it, and
    /// none reads where it listens while that is being written.
    pub fn ensure(&self) -> Result<String> {
        let _gate = self.gate()?;
        let mut stale = None;
        if !self.is_running()? {
            stale = fs::read_to_string(self.pid_file()).ok();
            self.start()?;
        }
        self.wait_until_ready(stale.as_deref())?;
        self.connection()
    }

    /// Waits for the server to take connections, which its PID file says on its eighth line. `pg_ctl -w` is not enough:
    /// after a crash it can return on the file the dead server left, `stale`, which says ready and names its port, while
    /// the new one is still recovering. A server another process started may be at that point too.
    fn wait_until_ready(&self, stale: Option<&str>) -> Result<()> {
        let began = Instant::now();
        while began.elapsed() < READY_WAIT {
            let pid = fs::read_to_string(self.pid_file()).unwrap_or_default();
            let ready = pid.lines().nth(7).is_some_and(|status| status.trim() == "ready");
            if ready && stale != Some(pid.as_str()) {
                return Ok(());
            }
            thread::sleep(READY_POLL);
        }
        Err(self.with_log(Error::Failed {
            what: "starting the lake's Postgres".into(),
            detail: format!("not ready after {READY_WAIT:?}"),
        }))
    }

    /// Stops the server if it is up, ending the sessions it has.
    pub fn stop(&self) -> Result<()> {
        let _gate = self.gate()?;
        if !self.is_running()? {
            return Ok(());
        }
        run(
            "stopping the lake's Postgres",
            self.binaries
                .tool("pg_ctl")
                .arg("stop")
                .args(["-w", "-m", "fast", "-t", WAIT])
                .arg("-D")
                .arg(self.data()),
        )
    }

    /// Whoever holds this is the only one starting or stopping the server. The system lets go of it if they die.
    fn gate(&self) -> Result<File> {
        let gate = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.lake.join(".gate"))?;
        gate.lock()?;
        Ok(gate)
    }

    /// Starts the server and waits until it takes connections.
    fn start(&self) -> Result<()> {
        if !WINDOWS {
            fs::create_dir_all(self.socket_dir()?)?;
            return self.start_with(&self.socket_settings()?);
        }
        let mut last = None;
        for _ in 0..PORT_ATTEMPTS {
            let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
            let settings = format!("listen_addresses = '127.0.0.1'\nport = {port}\n");
            match self.start_with(&settings) {
                Ok(()) => return Ok(()),
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one attempt"))
    }

    fn start_with(&self, settings: &str) -> Result<()> {
        fs::write(self.data().join("where.conf"), settings)?;
        keep_standard_handles_from_children();
        // Nothing of ours may reach the server, which outlives us: not even a file for what `pg_ctl` says. It logs.
        let status = self
            .binaries
            .tool("pg_ctl")
            .arg("start")
            .arg("-w")
            .args(["-t", WAIT])
            .arg("-D")
            .arg(self.data())
            .arg("-l")
            .arg(self.log())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if status.success() {
            return Ok(());
        }
        let failure = Error::Failed {
            what: "starting the lake's Postgres".into(),
            detail: status.to_string(),
        };
        Err(self.with_log(failure))
    }

    /// How to reach the server that is up: a libpq connection string.
    fn connection(&self) -> Result<String> {
        if !WINDOWS {
            let host = quoted(&self.socket_dir()?.to_string_lossy());
            return Ok(format!("host={host} port={SOCKET_PORT} user={USER} dbname={DATABASE}"));
        }
        let password = fs::read_to_string(self.password_file())?;
        let port = self.port()?;
        Ok(format!(
            "host=127.0.0.1 port={port} user={USER} password={password} dbname={DATABASE}"
        ))
    }

    /// The port the server took, which its PID file says on its fourth line.
    fn port(&self) -> Result<String> {
        let pid = fs::read_to_string(self.pid_file())?;
        let port = pid.lines().nth(3).map(str::trim).filter(|p| !p.is_empty());
        port.map(str::to_string).ok_or_else(|| Error::Failed {
            what: "reading the lake's Postgres".into(),
            detail: "its PID file has no port".into(),
        })
    }

    fn data(&self) -> PathBuf {
        self.lake.join("postgres")
    }

    fn pid_file(&self) -> PathBuf {
        self.data().join("postmaster.pid")
    }

    fn log(&self) -> PathBuf {
        self.lake.join("postgres.log")
    }

    fn password_file(&self) -> PathBuf {
        self.lake.join(".password")
    }

    fn socket_settings(&self) -> Result<String> {
        let directory = self.socket_dir()?.to_string_lossy().replace('\'', "''");
        Ok(format!(
            "listen_addresses = ''\nport = {SOCKET_PORT}\nunix_socket_directories = '{directory}'\nunix_socket_permissions = 0700\n"
        ))
    }

    /// Where the socket goes: in the lake, unless its path is too long for one, when it goes to a directory of the
    /// machine named after the lake.
    fn socket_dir(&self) -> Result<PathBuf> {
        let inside = self.lake.join("run");
        if inside.as_os_str().len() <= SOCKET_DIR_MAX {
            return Ok(inside);
        }
        let mut hasher = DefaultHasher::new();
        self.lake.hash(&mut hasher);
        Ok(env::temp_dir().join(format!("metalake-{:016x}", hasher.finish())))
    }

    /// `failure`, with the end of the server's log, which says why.
    fn with_log(&self, failure: Error) -> Error {
        let log = fs::read_to_string(self.log()).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        let tail = lines[lines.len().saturating_sub(5)..].join("\n");
        match failure {
            Error::Failed { what, detail } if !tail.is_empty() => Error::Failed {
                what,
                detail: format!("{detail}\n{tail}"),
            },
            other => other,
        }
    }
}

/// On Windows a child inherits every handle that is marked inheritable, whatever it is given as its own stdio, and hands
/// them on. A server that stays up would then hold our stdout, and whoever reads it, as `$(metalake ...)` does, would
/// wait for it to end for as long as the server lives. So they stop being inheritable.
#[cfg(windows)]
fn keep_standard_handles_from_children() {
    use std::ffi::c_void;

    const STD_HANDLES: [i32; 3] = [-10, -11, -12];
    const HANDLE_FLAG_INHERIT: u32 = 1;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(handle: i32) -> *mut c_void;
        fn SetHandleInformation(handle: *mut c_void, mask: u32, flags: u32) -> i32;
    }
    for handle in STD_HANDLES {
        // Safety: both only read their arguments; a standard handle that is not there is an error here, and ignored.
        unsafe {
            SetHandleInformation(GetStdHandle(handle), HANDLE_FLAG_INHERIT, 0);
        }
    }
}

#[cfg(not(windows))]
fn keep_standard_handles_from_children() {}

/// A value of a libpq connection string.
fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Runs a tool of Postgres that starts nothing to its end, its output kept to explain a failure.
fn run(what: &str, command: &mut Command) -> Result<()> {
    let output = env::temp_dir().join(format!("metalake-{}.out", Uuid::new_v4().simple()));
    let file = File::create(&output)?;
    let status = command
        .stdin(Stdio::null())
        .stdout(file.try_clone()?)
        .stderr(file)
        .status();
    let said = fs::read_to_string(&output).unwrap_or_default();
    let _ = fs::remove_file(&output);
    match status? {
        status if status.success() => Ok(()),
        status => Err(Error::Failed {
            what: what.to_string(),
            detail: format!("{status}: {}", said.trim()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_quoted_for_libpq() {
        assert_eq!(quoted(r"C:\a b"), r"'C:\\a b'");
        assert_eq!(quoted("it's"), r"'it\'s'");
    }
}
