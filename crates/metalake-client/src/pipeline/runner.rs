//! How a pipeline runs: `arroyo run`, one process that plans the query and runs it, keeping its checkpoints in the
//! pipeline's state, and restoring from them when it starts again with the same query. It runs as a process of this
//! machine when Arroyo is installed (`METALAKE_ARROYO`, or `arroyo` on the path), and otherwise in a container of
//! Arroyo's image, which is how it runs on Windows and macOS. Either way it stops with a final checkpoint.
//!
//! What it needs is a client's own environment: where the catalog is, its schema, and the storage's credentials, which
//! reach it as variables and never as arguments.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use crate::pipeline::record::{Handle, RunnerKind};
use crate::{Error, Result};

/// The binary of Arroyo to run a pipeline with.
pub const ARROYO: &str = "METALAKE_ARROYO";
/// The image of Arroyo to run a pipeline in, when there is no binary.
pub const ARROYO_IMAGE: &str = "METALAKE_ARROYO_IMAGE";
const DEFAULT_IMAGE: &str = "ghcr.io/describeloai/arroyo-metalake:feat-metalake-resolve";

/// How long a pipeline has to take its final checkpoint when it stops.
const STOPPING: Duration = Duration::from_secs(90);
/// The lines of its log a failed pipeline says.
const TAIL: usize = 40;

/// What starts a pipeline here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Runner {
    Process { binary: PathBuf },
    Docker { image: String },
}

/// What a pipeline is started with.
pub struct Launch<'a> {
    pub name: &'a str,
    pub query: &'a str,
    pub parallelism: u32,
    /// Its state's URL.
    pub state: &'a str,
    /// Its variables: the catalog's, and the storage's.
    pub env: Vec<(String, String)>,
    /// A directory of this machine for its query and its log.
    pub home: &'a Path,
    /// The warehouse's directory, when it is one of this machine, which a container must see where it is.
    pub warehouse: Option<PathBuf>,
    /// Whether the catalog is the lake's own Postgres, which only this machine reaches.
    pub own_postgres: bool,
}

impl Runner {
    /// Arroyo's binary if there is one, and otherwise Docker.
    pub fn find() -> Result<Self> {
        if let Some(binary) = env::var_os(ARROYO) {
            return Ok(Self::Process { binary: binary.into() });
        }
        if cfg!(unix) && which("arroyo") {
            return Ok(Self::Process {
                binary: "arroyo".into(),
            });
        }
        if which("docker") {
            let image = env::var(ARROYO_IMAGE).unwrap_or_else(|_| DEFAULT_IMAGE.to_string());
            return Ok(Self::Docker { image });
        }
        Err(Error::failed(format!(
            "a pipeline runs in Arroyo: install it (or set {ARROYO}), or install Docker to run its image"
        )))
    }

    pub fn start(&self, launch: &Launch<'_>) -> Result<Handle> {
        fs::create_dir_all(launch.home)?;
        fs::write(launch.home.join(QUERY), launch.query)?;
        let host = host();
        match self {
            Self::Process { binary } => {
                if cfg!(windows) {
                    return Err(Error::failed(format!(
                        "Arroyo runs as a process on Linux and macOS; on Windows, unset {ARROYO} to run its image in Docker"
                    )));
                }
                let log = fs::File::create(launch.home.join(LOG))?;
                let mut command = Command::new(binary);
                command
                    .args(arguments(launch, &launch.home.join(QUERY).to_string_lossy()))
                    .envs(launch.env.iter().map(|(k, v)| (k, v)))
                    .stdin(Stdio::null())
                    .stdout(log.try_clone()?)
                    .stderr(log);
                // Its own group, so that a terminal's Ctrl-C does not stop it with the command that started it.
                #[cfg(unix)]
                std::os::unix::process::CommandExt::process_group(&mut command, 0);
                let child = command
                    .spawn()
                    .map_err(|e| Error::failed(format!("Arroyo could not start ({}): {e}", binary.display())))?;
                Ok(Handle {
                    kind: RunnerKind::Process,
                    id: child.id().to_string(),
                    host,
                })
            }
            Self::Docker { image } => {
                let postgres = launch
                    .env
                    .iter()
                    .find(|(name, _)| name == metalake::environment::POSTGRES)
                    .map(|(_, value)| value.as_str())
                    .unwrap_or_default();
                // A lake's own Postgres listens on a socket in the lake (Linux, macOS) or on the loopback (Windows): a
                // container reaches the socket, mounted where it is, only where Docker shares a socket, on Linux.
                let socket = match (launch.own_postgres, socket_of(postgres)) {
                    (false, _) => None,
                    (true, Some(socket)) if cfg!(target_os = "linux") => Some(socket),
                    (true, _) => {
                        return Err(Error::usage(format!(
                            "this lake's own Postgres is reached only from this machine, and Arroyo's container cannot \
                             reach it here: install Arroyo and set {ARROYO} to run its pipelines as processes, or keep \
                             the lake on a Postgres reached over the network"
                        )));
                    }
                };
                let container = container(launch.name, launch.state);
                let _ = docker(&["rm", "-f", &container]);
                let mut command = Command::new("docker");
                command.args(["run", "-d", "--name", &container]);
                command.args(["--add-host", "host.docker.internal:host-gateway"]);
                // As the user that starts it, so that what it writes in a local warehouse is that user's to drop.
                if let Some(user) = user() {
                    command.args(["--user", &user]);
                }
                command.arg("-v").arg(format!("{}:/pipeline:ro", launch.home.display()));
                if let Some(socket) = &socket {
                    command.arg("-v").arg(format!("{socket}:{socket}"));
                }
                if let Some(warehouse) = &launch.warehouse {
                    command.arg("-v").arg(format!("{0}:{0}", warehouse.display()));
                }
                // By name only: the values stay in this process's environment, out of any listing of processes.
                for (name, value) in &launch.env {
                    command.arg("-e").arg(name);
                    command.env(name, in_container(name, value));
                }
                command.arg(image);
                command.args(arguments(launch, "/pipeline/query.sql"));
                let output = command
                    .output()
                    .map_err(|e| Error::failed(format!("Docker could not start Arroyo: {e}")))?;
                check(&output, "Docker could not start Arroyo")?;
                Ok(Handle {
                    kind: RunnerKind::Docker,
                    id: container,
                    host,
                })
            }
        }
    }
}

impl Handle {
    /// Whether it is on this machine, where it can be asked about and stopped.
    pub fn here(&self) -> bool {
        self.host == host()
    }

    pub fn alive(&self) -> bool {
        match self.kind {
            // A process that ended is a zombie until its parent reaps it, which a long-lived parent, as a Python
            // program, may never do: it still answers a signal, and is not running.
            RunnerKind::Process => {
                cfg!(unix)
                    && Command::new("ps")
                        .args(["-o", "stat=", "-p", &self.id])
                        .stderr(Stdio::null())
                        .output()
                        .is_ok_and(|o| {
                            let stat = String::from_utf8_lossy(&o.stdout);
                            let stat = stat.trim();
                            o.status.success() && !stat.is_empty() && !stat.starts_with('Z')
                        })
            }
            RunnerKind::Docker => docker(&["inspect", "-f", "{{.State.Running}}", &self.id])
                .is_ok_and(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "true"),
        }
    }

    /// Stops it with a final checkpoint, and waits for it; past the time it has for one, it is killed.
    pub fn stop(&self) -> Result<()> {
        match self.kind {
            RunnerKind::Process => {
                let _ = Command::new("kill").args(["-TERM", &self.id]).status();
                let since = Instant::now();
                while self.alive() && since.elapsed() < STOPPING {
                    std::thread::sleep(Duration::from_millis(200));
                }
                if self.alive() {
                    let _ = Command::new("kill").args(["-KILL", &self.id]).status();
                }
                Ok(())
            }
            RunnerKind::Docker => {
                let output = docker(&["stop", "-t", &STOPPING.as_secs().to_string(), &self.id])?;
                // A container already gone is stopped.
                if !output.status.success() && self.alive() {
                    check(&output, "Docker could not stop the pipeline")?;
                }
                Ok(())
            }
        }
    }

    /// What is left of it on this machine.
    pub fn remove(&self) {
        if self.kind == RunnerKind::Docker {
            let _ = docker(&["rm", "-f", &self.id]);
        }
    }

    /// The last lines of its log.
    pub fn log(&self, home: &Path) -> String {
        let text = match self.kind {
            RunnerKind::Process => fs::read_to_string(home.join(LOG)).unwrap_or_default(),
            RunnerKind::Docker => docker(&["logs", "--tail", &TAIL.to_string(), &self.id])
                .map(|o| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_default(),
        };
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(TAIL)..].join("\n")
    }
}

const QUERY: &str = "query.sql";
const LOG: &str = "pipeline.log";

/// `arroyo run`'s arguments for a pipeline.
fn arguments(launch: &Launch<'_>, query: &str) -> Vec<String> {
    vec![
        "run".into(),
        "--name".into(),
        launch.name.into(),
        "--parallelism".into(),
        launch.parallelism.to_string(),
        "--state-dir".into(),
        launch.state.into(),
        query.into(),
    ]
}

/// A pipeline's container: by its name and its state, so that two lakes' pipelines of one name are two containers.
pub fn container(name: &str, state: &str) -> String {
    format!("metalake-pipeline-{name}-{:08x}", fnv(state) as u32)
}

/// A pipeline's directory on this machine, for its query and its log.
pub fn home(state: &str) -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| Error::failed("no home directory for a pipeline's files: set HOME"))?;
    Ok(PathBuf::from(home)
        .join(".metalake")
        .join("pipelines")
        .join(format!("{:016x}", fnv(state))))
}

/// A variable as a container must see it: the machine's own Postgres is `host.docker.internal` from inside.
fn in_container(name: &str, value: &str) -> String {
    if name != metalake::environment::POSTGRES {
        return value.to_string();
    }
    let mut value = value.to_string();
    for local in ["localhost", "127.0.0.1"] {
        value = value
            .replace(&format!("@{local}"), "@host.docker.internal")
            .replace(&format!("host={local}"), "host=host.docker.internal")
            .replace(&format!("//{local}"), "//host.docker.internal");
    }
    value
}

/// The directory of a Postgres socket a connection string names, `host=/path` or `host='/path'`, if it names one.
fn socket_of(postgres: &str) -> Option<String> {
    let (_, rest) = postgres.split_once("host=")?;
    let host = match rest.strip_prefix('\'') {
        Some(quoted) => {
            let mut host = String::new();
            let mut chars = quoted.chars();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => host.extend(chars.next()),
                    '\'' => break,
                    c => host.push(c),
                }
            }
            host
        }
        None => rest.split_whitespace().next().unwrap_or_default().to_string(),
    };
    host.starts_with('/').then_some(host)
}

/// A name of this machine.
pub fn host() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .ok()
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// `uid:gid` of this process, on a system that has them.
fn user() -> Option<String> {
    if !cfg!(unix) {
        return None;
    }
    let id = |flag: &str| {
        Command::new("id")
            .arg(flag)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };
    Some(format!("{}:{}", id("-u")?, id("-g")?))
}

fn which(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn docker(args: &[&str]) -> Result<Output> {
    Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| Error::failed(format!("Docker: {e}")))
}

fn check(output: &Output, what: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    Err(Error::failed(format!(
        "{what}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

/// FNV-1a: a hash that is the same on every machine and in every build.
fn fnv(text: &str) -> u64 {
    text.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ byte as u64).wrapping_mul(0x100000001b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_from_a_container() {
        let postgres = metalake::environment::POSTGRES;
        assert_eq!(
            in_container(postgres, "host=localhost port=5432 user=postgres"),
            "host=host.docker.internal port=5432 user=postgres"
        );
        assert_eq!(
            in_container(postgres, "postgresql://u:p@127.0.0.1:5432/db"),
            "postgresql://u:p@host.docker.internal:5432/db"
        );
        assert_eq!(in_container(postgres, "host=db.internal"), "host=db.internal");
        assert_eq!(
            in_container("AWS_REGION", "localhost"),
            "localhost",
            "only the catalog's"
        );
    }

    #[test]
    fn sockets() {
        assert_eq!(
            socket_of("host='/lake/postgres/run' port=5432 user=metalake").as_deref(),
            Some("/lake/postgres/run")
        );
        assert_eq!(socket_of(r"host='/a b/it\'s' port=1").as_deref(), Some("/a b/it's"));
        assert_eq!(socket_of("host=/run/pg port=1").as_deref(), Some("/run/pg"));
        assert_eq!(socket_of("host=127.0.0.1 port=1"), None);
        assert_eq!(socket_of("postgresql://u@db/x"), None);
    }

    #[test]
    fn names_by_lake() {
        assert_ne!(
            container("t", "s3://a/_pipelines/t/state"),
            container("t", "s3://b/_pipelines/t/state")
        );
        assert_eq!(container("t", "x"), container("t", "x"));
        assert!(container("totals", "x").starts_with("metalake-pipeline-totals-"));
        assert_eq!(fnv(""), 0xcbf29ce484222325, "the same everywhere");
    }

    #[test]
    fn run_arguments() {
        let home = PathBuf::from("/h");
        let launch = Launch {
            name: "totals",
            query: "q",
            parallelism: 2,
            state: "s3://l/_pipelines/totals/state",
            env: vec![],
            home: &home,
            warehouse: None,
            own_postgres: false,
        };
        assert_eq!(
            arguments(&launch, "/pipeline/query.sql"),
            [
                "run",
                "--name",
                "totals",
                "--parallelism",
                "2",
                "--state-dir",
                "s3://l/_pipelines/totals/state",
                "/pipeline/query.sql"
            ]
        );
    }
}
