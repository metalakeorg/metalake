//! The Notebook (ADR 0008): JupyterLab, run by the Python metalake was installed in, started by `metalake ui` when its
//! page first asks, and ended with it. The `metalake` command pip installs is a Python script, which names its Python
//! in [`PYTHON`]; a `metalake` built alone has none, and so no Notebook. JupyterLab is looked for each time it is asked,
//! so that installing it while the UI runs is seen without a restart.
//!
//! Started isolated (§4): on the loopback, at a port the system chooses, under [`PREFIX`], with a token of the run in
//! its environment, and directories of the run's own for its runtime, config, settings and workspaces, so that neither
//! the user's `~/.jupyter` nor a past run applies. Its life is the UI's: it watches the process of `metalake ui`, and
//! ends once it has, however it ended; its kernels end with it. Not by its standard input: on Windows a thread blocked
//! reading it blocks every other use of it, and JupyterLab, which asks of it as it starts, never starts (tried).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::token::Token;

/// The variable that names the Python the UI's Notebook runs in.
pub const PYTHON: &str = "METALAKE_PYTHON";

/// What the Notebook needs, as pip installs it: the extra of metalake that brings JupyterLab and the kernel.
pub const EXTRA: &str = r#""metalake[notebook]""#;

/// Where JupyterLab is served, on the UI's origin; the page itself is `/notebook`.
pub const PREFIX: &str = "/jupyter/";

/// The JupyterLabs this process started, which each run's directory is named by.
static RUNS: AtomicU32 = AtomicU32::new(0);

/// How long a look for JupyterLab may take: a Python's start, and importing what the Notebook runs.
const LOOK: Duration = Duration::from_secs(20);

/// How long JupyterLab may take to start.
const START: Duration = Duration::from_secs(90);

/// How long JupyterLab has to end once asked.
const END: Duration = Duration::from_secs(10);

/// What the Python runs: JupyterLab, with a watch on the process `METALAKE_UI_PID` names, which ends it once that
/// process has ended. On Windows, a handle to it, waited on, as ipykernel watches its parent; elsewhere, a look each
/// second.
const LAUNCH: &str = r#"
import os, sys, threading, time
def watch(pid):
    if os.name == "nt":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x00100000, False, pid)
        if handle:
            kernel32.WaitForSingleObject(handle, 0xFFFFFFFF)
    else:
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            except PermissionError:
                pass
            time.sleep(1)
    os._exit(0)
threading.Thread(target=watch, args=(int(os.environ["METALAKE_UI_PID"]),), daemon=True).start()
from jupyterlab.labapp import main
sys.argv = ["jupyter-lab", *sys.argv[1:]]
sys.exit(main())
"#;

/// The Python a Notebook would run in, where its notebooks are, and its JupyterLab once started.
#[derive(Debug, Default)]
pub struct Notebook {
    python: Option<PathBuf>,
    /// Where notebooks are kept: Jupyter's root.
    root: Option<PathBuf>,
    running: Mutex<Option<Jupyter>>,
    /// The process JupyterLab's life is tied to: this one, unless a test gives another to end.
    watched: Option<u32>,
    #[cfg(test)]
    found: Option<bool>,
}

/// A JupyterLab this UI started.
#[derive(Debug)]
struct Jupyter {
    child: Child,
    port: u16,
    token: Token,
    /// The run's own directories, removed when it ends.
    dir: PathBuf,
}

impl Notebook {
    /// The Python [`PYTHON`] names, if any.
    pub fn from_environment() -> Self {
        let python = std::env::var_os(PYTHON).filter(|p| !p.is_empty()).map(PathBuf::from);
        Self::with_python(python)
    }

    pub fn with_python(python: Option<PathBuf>) -> Self {
        Self {
            python,
            ..Self::default()
        }
    }

    /// Keeps notebooks in `root`.
    pub fn keeping(mut self, root: PathBuf) -> Self {
        self.root = Some(root);
        self
    }

    /// For a test: a Python whose JupyterLab is `found` or not, without running it.
    #[cfg(test)]
    pub(crate) fn answering(python: bool, found: bool) -> Self {
        Self {
            python: python.then(|| PathBuf::from("python")),
            found: Some(found),
            ..Self::default()
        }
    }

    pub fn python(&self) -> Option<&PathBuf> {
        self.python.as_ref()
    }

    /// Where notebooks are kept: the root given, else where `metalake ui` was started.
    pub fn root(&self) -> PathBuf {
        self.root
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

    /// The line that installs what the Notebook needs into its own Python, not into whichever `pip` a shell finds
    /// first; with no Python known, the plain line.
    pub fn install(&self) -> String {
        match &self.python {
            Some(python) => format!(r#""{}" -m pip install {EXTRA}"#, python.display()),
            None => format!("pip install {EXTRA}"),
        }
    }

    /// Whether the Python imports what the Notebook runs, JupyterLab and the kernel: imported, not only found, so that
    /// an installation missing one of their dependencies is not taken for one that works. Not if there is no Python, or
    /// it cannot be run.
    pub async fn jupyterlab(&self) -> bool {
        #[cfg(test)]
        if let Some(found) = self.found {
            return found;
        }
        let Some(python) = &self.python else {
            return false;
        };
        let look = Command::new(python)
            .args(["-c", "import jupyterlab, jupyter_server, ipykernel"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status();
        matches!(tokio::time::timeout(LOOK, look).await, Ok(Ok(status)) if status.success())
    }

    /// Whether its JupyterLab runs now.
    pub async fn running(&self) -> bool {
        let mut running = self.running.lock().await;
        if running.as_mut().is_some_and(Jupyter::ended)
            && let Some(gone) = running.take()
        {
            gone.forget();
        }
        running.is_some()
    }

    /// Starts its JupyterLab, unless it runs already, and answers once it serves. One at a time: a second ask waits for
    /// the first's start.
    pub async fn start(&self) -> Result<(), String> {
        let python = self
            .python
            .clone()
            .ok_or("this metalake runs without Python: the Notebook needs metalake installed with pip")?;
        let mut running = self.running.lock().await;
        if running.as_mut().is_some_and(Jupyter::ended)
            && let Some(gone) = running.take()
        {
            gone.forget();
        }
        if running.is_none() {
            let watched = self.watched.unwrap_or_else(std::process::id);
            *running = Some(Jupyter::start(&python, &self.root(), watched).await?);
        }
        Ok(())
    }

    /// Ends its JupyterLab, if it runs: asked to shut down, its input closed, and, if it has not ended, killed.
    pub async fn stop(&self) {
        let jupyter = self.running.lock().await.take();
        if let Some(jupyter) = jupyter {
            jupyter.stop().await;
        }
    }

    /// The port and token of its JupyterLab, while it runs.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn reach(&self) -> Option<(u16, String)> {
        let running = self.running.lock().await;
        running.as_ref().map(|j| (j.port, j.token.as_str().to_owned()))
    }

    /// For a test: JupyterLab's life tied to the process `pid`, which the test ends as `metalake ui` would die.
    #[cfg(test)]
    pub(crate) fn watching(mut self, pid: u32) -> Self {
        self.watched = Some(pid);
        self
    }

    /// For a test: its JupyterLab let go, nothing asked, left to its watch; the port it served at, to see it end.
    #[cfg(test)]
    pub(crate) async fn abandon(&self) -> Option<u16> {
        let jupyter = self.running.lock().await.take()?;
        let port = jupyter.port;
        let _ = std::fs::remove_dir_all(&jupyter.dir);
        // Its drop would kill it: the watch must end it.
        std::mem::forget(jupyter.child);
        Some(port)
    }
}

impl Jupyter {
    async fn start(python: &Path, root: &Path, watched: u32) -> Result<Self, String> {
        let token = Token::generate().map_err(|e| e.to_string())?;
        let dir = crate::browser::directory().map_err(|e| e.to_string())?.join(format!(
            "jupyter-{}-{}",
            std::process::id(),
            RUNS.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in ["runtime", "config", "settings", "workspaces"] {
            std::fs::create_dir_all(dir.join(sub)).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        std::fs::create_dir_all(root).map_err(|e| format!("{}: {e}", root.display()))?;
        let log = std::fs::File::create(dir.join("jupyter.log")).map_err(|e| e.to_string())?;
        let child = Command::new(python)
            .arg("-c")
            .arg(LAUNCH)
            .arg("--no-browser")
            .arg("--ServerApp.ip=127.0.0.1")
            .arg("--ServerApp.port=0")
            .arg(format!("--ServerApp.base_url={PREFIX}"))
            .arg(format!("--ServerApp.root_dir={}", root.display()))
            .arg("--ServerApp.terminals_enabled=False")
            .arg("--LabApp.news_url=None")
            .arg("--LabApp.check_for_updates_class=jupyterlab.NeverCheckForUpdate")
            .env("METALAKE_UI_PID", watched.to_string())
            .env("JUPYTER_TOKEN", token.as_str())
            .env("JUPYTER_RUNTIME_DIR", dir.join("runtime"))
            .env("JUPYTER_CONFIG_DIR", dir.join("config"))
            .env("JUPYTERLAB_SETTINGS_DIR", dir.join("settings"))
            .env("JUPYTERLAB_WORKSPACES_DIR", dir.join("workspaces"))
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("{}: {e}", python.display()))?;
        let mut jupyter = Self {
            child,
            port: 0,
            token,
            dir,
        };
        match jupyter.ready().await {
            Ok(port) => {
                jupyter.port = port;
                Ok(jupyter)
            }
            Err(e) => {
                let said = jupyter.said();
                jupyter.stop().await;
                Err(format!("JupyterLab did not start: {e}{said}"))
            }
        }
    }

    /// Whether its process has ended.
    fn ended(&mut self) -> bool {
        !matches!(self.child.try_wait(), Ok(None))
    }

    /// The port it serves at, once its status answers: the port from the runtime file it writes, in a directory of
    /// the run's own, so that no past run's file is read.
    async fn ready(&mut self) -> Result<u16, String> {
        let start = Instant::now();
        while start.elapsed() < START {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("it ended ({status})"));
            }
            if let Some(port) = self.written_port()
                && asked(port, "GET", "api/status", self.token.as_str()).await == Some(200)
            {
                return Ok(port);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(format!("it did not answer in {}s", START.as_secs()))
    }

    fn written_port(&self) -> Option<u16> {
        let files = std::fs::read_dir(self.dir.join("runtime")).ok()?;
        files.flatten().find_map(|file| {
            let name = file.file_name();
            let name = name.to_string_lossy();
            if !(name.starts_with("jpserver-") && name.ends_with(".json")) {
                return None;
            }
            let text = std::fs::read_to_string(file.path()).ok()?;
            let written: serde_json::Value = serde_json::from_str(&text).ok()?;
            u16::try_from(written["port"].as_u64()?).ok()
        })
    }

    /// The end of its log, for a start that failed.
    fn said(&self) -> String {
        let log = std::fs::read_to_string(self.dir.join("jupyter.log")).unwrap_or_default();
        let lines: Vec<&str> = log.lines().rev().take(8).collect();
        if lines.is_empty() {
            String::new()
        } else {
            format!("\n{}", lines.into_iter().rev().collect::<Vec<_>>().join("\n"))
        }
    }

    async fn stop(mut self) {
        if self.port != 0 {
            let _ = asked(self.port, "POST", "api/shutdown", self.token.as_str()).await;
        }
        if tokio::time::timeout(END, self.child.wait()).await.is_err() {
            let _ = self.child.kill().await;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// A process found ended: its directories go.
    fn forget(self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One request to the JupyterLab at `port`, with its token: its status code, or none if it does not answer.
pub(crate) async fn asked(port: u16, method: &str, path: &str, token: &str) -> Option<u16> {
    let ask = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
        let request = format!(
            "{method} {PREFIX}{path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: token {token}\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.ok()?;
        let mut head = [0u8; 16];
        let read = stream.read(&mut head).await.ok()?;
        let line = std::str::from_utf8(&head[..read]).ok()?;
        line.split_whitespace().nth(1)?.parse().ok()
    };
    tokio::time::timeout(Duration::from_secs(5), ask).await.ok().flatten()
}
