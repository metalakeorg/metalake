//! The cluster inside a lake, and the calls that start and stop it. Nothing but the tools of Postgres touches its data
//! directory.
//!
//! On Linux and macOS it listens on a socket in the lake, closed to other users, with no TCP. Windows has no such
//! sockets here, so it listens on the loopback, behind a password kept in the lake.

use std::env;
use std::ffi::OsStr;
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
jit = off
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
        let code = self.pg_ctl_start()?;
        if code == 0 {
            return Ok(());
        }
        let failure = Error::Failed {
            what: "starting the lake's Postgres".into(),
            detail: format!("exit code {code}"),
        };
        Err(self.with_log(failure))
    }

    /// Runs `pg_ctl start`, and answers its exit code. Nothing of ours may reach the server, which outlives us: not
    /// even a file for what `pg_ctl` says. It logs.
    fn pg_ctl_start(&self) -> Result<i32> {
        let (data, log) = (self.data(), self.log());
        let args = [
            "start".as_ref(),
            "-w".as_ref(),
            "-t".as_ref(),
            WAIT.as_ref(),
            "-D".as_ref(),
            data.as_os_str(),
            "-l".as_ref(),
            log.as_os_str(),
        ];
        run_inheriting_nothing(&self.binaries.directory().join("pg_ctl"), &args)
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

/// Runs a program that inherits no handle. A launcher above us, such as Python's, can leave copies of a pipe of stdout
/// that we cannot name, and a server holding one keeps whoever reads that pipe waiting. `Command` always inherits.
#[cfg(windows)]
fn run_inheriting_nothing(program: &Path, args: &[&OsStr]) -> Result<i32> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null, null_mut};

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const INFINITE: u32 = 0xFFFF_FFFF;

    #[repr(C)]
    struct StartupInfo {
        size: u32,
        reserved: *mut u16,
        desktop: *mut u16,
        title: *mut u16,
        numbers: [u32; 7],
        flags: u32,
        show: u16,
        reserved2: u16,
        reserved3: *mut u8,
        handles: [*mut c_void; 3],
    }
    #[repr(C)]
    struct ProcessInformation {
        process: *mut c_void,
        thread: *mut c_void,
        ids: [u32; 2],
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateProcessW(
            application: *const u16,
            line: *mut u16,
            process_attributes: *const c_void,
            thread_attributes: *const c_void,
            inherit: i32,
            flags: u32,
            environment: *const c_void,
            directory: *const u16,
            startup: *const StartupInfo,
            process: *mut ProcessInformation,
        ) -> i32;
        fn WaitForSingleObject(handle: *mut c_void, milliseconds: u32) -> u32;
        fn GetExitCodeProcess(handle: *mut c_void, code: *mut u32) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    let mut line: Vec<u16> = Vec::new();
    for arg in std::iter::once(program.as_os_str()).chain(args.iter().copied()) {
        line.push(b'"'.into());
        line.extend(arg.encode_wide());
        line.extend([u16::from(b'"'), u16::from(b' ')]);
    }
    line.push(0);
    let startup = StartupInfo {
        size: size_of::<StartupInfo>() as u32,
        reserved: null_mut(),
        desktop: null_mut(),
        title: null_mut(),
        numbers: [0; 7],
        flags: 0,
        show: 0,
        reserved2: 0,
        reserved3: null_mut(),
        handles: [null_mut(); 3],
    };
    // Safety: every pointer is to a value of this frame, or null where the call allows it, and each handle is closed
    // once.
    unsafe {
        let mut process: ProcessInformation = std::mem::zeroed();
        let created = CreateProcessW(
            null(),
            line.as_mut_ptr(),
            null(),
            null(),
            0,
            CREATE_NO_WINDOW,
            null(),
            null(),
            &startup,
            &mut process,
        );
        if created == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        WaitForSingleObject(process.process, INFINITE);
        let mut code = 1;
        GetExitCodeProcess(process.process, &mut code);
        CloseHandle(process.thread);
        CloseHandle(process.process);
        Ok(code as i32)
    }
}

#[cfg(not(windows))]
fn run_inheriting_nothing(program: &Path, args: &[&OsStr]) -> Result<i32> {
    let status = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    Ok(status.code().unwrap_or(-1))
}

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
