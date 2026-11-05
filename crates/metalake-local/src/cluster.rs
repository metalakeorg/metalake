//! The cluster inside a lake, and the calls that start and stop it. Nothing but the tools of Postgres touches its data
//! directory, and everything that depends on where the lake is now is written at each start, so a lake can be moved.
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

    /// Starts the server and waits until it takes connections.
    pub fn start(&self) -> Result<()> {
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
        // The log goes where `pg_ctl` sends it: a server that inherited a pipe would hold it open past this call.
        let done = run(
            "starting the lake's Postgres",
            self.binaries
                .tool("pg_ctl")
                .arg("start")
                .arg("-w")
                .args(["-t", WAIT])
                .arg("-D")
                .arg(self.data())
                .arg("-l")
                .arg(self.log()),
        );
        done.map_err(|e| self.with_log(e))
    }

    /// Stops the server, ending the sessions it has. It is for when nobody is meant to be using it.
    pub fn stop(&self) -> Result<()> {
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

    /// How to reach the server that is up: a libpq connection string.
    pub fn connection(&self) -> Result<String> {
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
        let pid = fs::read_to_string(self.data().join("postmaster.pid"))?;
        let port = pid.lines().nth(3).map(str::trim).filter(|p| !p.is_empty());
        port.map(str::to_string).ok_or_else(|| Error::Failed {
            what: "reading the lake's Postgres".into(),
            detail: "its PID file has no port".into(),
        })
    }

    fn data(&self) -> PathBuf {
        self.lake.join("postgres")
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

/// A value of a libpq connection string.
fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Runs a tool of Postgres to its end. Nothing it starts may inherit a pipe of ours, so its output goes to a file.
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
