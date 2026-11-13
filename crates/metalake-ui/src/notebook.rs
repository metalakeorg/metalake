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
//!
//! Its one kernel is MetaLake's (§2): IPython, in the same Python, which starts with `lake` bound to the lake the UI
//! serves, as `metalake.connect` reaches it, on `main` until `lake.on` says another ref. Its spec and its startup file
//! are the run's own, so that no kernel the user installed is offered, nor any IPython profile of theirs applies.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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

/// The name of MetaLake's kernel, the only one offered, and what Jupyter shows of it.
pub const KERNEL: &str = "metalake";
const KERNEL_SHOWN: &str = "MetaLake";

/// The ref selector's package, as JupyterLab finds it among the run's extensions.
pub const SELECTOR: &str = "@metalake/jupyterlab-ref";

/// The variable that tells the kernel where the lake is: the directory of its `metalake.toml`. Unset, `connect` finds
/// it as it would anywhere, from the environment the kernel inherits from `metalake ui`.
pub const LAKE: &str = "METALAKE_NOTEBOOK_LAKE";

/// What the kernel runs as it starts: `lake`, reached. A lake it cannot reach, or a Python without metalake, leaves
/// `lake` saying why at its first use, rather than a kernel that fails to start and says nothing in the notebook.
pub(crate) const STARTUP: &str = r#"# MetaLake's kernel: `lake` is the lake `metalake ui` serves, on `main` until `lake.on("ref")`.
def _metalake_lake():
    import os

    class Unreachable:
        def __init__(self, why):
            self._why = why

        def __getattr__(self, name):
            raise RuntimeError(f"no lake: {self._why}")

        def __repr__(self):
            return f"<no lake: {self._why}>"

    try:
        import metalake
    except ImportError as e:
        return Unreachable(f"metalake is not installed in this Python ({e})")
    try:
        return metalake.connect(os.environ.get("METALAKE_NOTEBOOK_LAKE") or None)
    except Exception as e:
        return Unreachable(f"the lake could not be reached ({e})")


lake = _metalake_lake()
del _metalake_lake
"#;

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
    /// The directory of the lake's `metalake.toml`, which its kernel connects to.
    lake: Option<PathBuf>,
    running: Mutex<Option<Jupyter>>,
    /// Set once the UI ends: a start in progress gives up, rather than hold the end for as long as JupyterLab takes.
    ending: AtomicBool,
    /// The process JupyterLab's life is tied to: this one, unless a test gives another to end.
    watched: Option<u32>,
    #[cfg(test)]
    found: Option<bool>,
    #[cfg(test)]
    reaching: Option<u16>,
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

    /// Its kernel's `lake` is the one whose `metalake.toml` is in `directory`.
    pub fn on_lake(mut self, directory: PathBuf) -> Self {
        self.lake = Some(directory);
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
            let lake = self.lake.as_deref();
            *running = Some(Jupyter::start(&python, &self.root(), lake, watched, &self.ending).await?);
        }
        Ok(())
    }

    /// Ends its JupyterLab, if it runs: asked to shut down, its input closed, and, if it has not ended, killed.
    pub async fn stop(&self) {
        self.ending.store(true, Ordering::Relaxed);
        let jupyter = self.running.lock().await.take();
        if let Some(jupyter) = jupyter {
            jupyter.stop().await;
        }
    }

    /// The port and token of its JupyterLab, while it runs.
    pub(crate) async fn reach(&self) -> Option<(u16, String)> {
        #[cfg(test)]
        if let Some(port) = self.reaching {
            return Some((port, "test".to_owned()));
        }
        let running = self.running.lock().await;
        running.as_ref().map(|j| (j.port, j.token.as_str().to_owned()))
    }

    /// Where the page opens JupyterLab while it runs: its own path, with its token once, which Jupyter turns into its
    /// login cookie.
    pub async fn open(&self) -> Option<String> {
        let (_, token) = self.reach().await?;
        Some(format!("{PREFIX}lab?token={token}"))
    }

    /// For a test: a JupyterLab taken to serve at `port`, a server of the test's.
    #[cfg(test)]
    pub(crate) fn reaching(mut self, port: u16) -> Self {
        self.reaching = Some(port);
        self
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
    async fn start(
        python: &Path,
        root: &Path,
        lake: Option<&Path>,
        watched: u32,
        ending: &AtomicBool,
    ) -> Result<Self, String> {
        let token = Token::generate().map_err(|e| e.to_string())?;
        let dir = crate::browser::directory().map_err(|e| e.to_string())?.join(format!(
            "jupyter-{}-{}",
            std::process::id(),
            RUNS.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        private(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        for sub in ["runtime", "config", "settings", "workspaces"] {
            std::fs::create_dir_all(dir.join(sub)).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        std::fs::create_dir_all(root).map_err(|e| format!("{}: {e}", root.display()))?;
        // Jupyter's news never fetched, and never asked about: answered in the run's own settings.
        let notification = dir.join("settings").join("@jupyterlab").join("apputils-extension");
        std::fs::create_dir_all(&notification).map_err(|e| e.to_string())?;
        std::fs::write(
            notification.join("notification.jupyterlab-settings"),
            r#"{"fetchNews": "false"}"#,
        )
        .map_err(|e| e.to_string())?;
        kernel(&dir, python, lake).map_err(|e| format!("{}: {e}", dir.display()))?;
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
            .arg("--LabApp.extension_manager=readonly")
            .arg("--LabApp.news_url=None")
            .arg("--LabApp.check_for_updates_class=jupyterlab.NeverCheckForUpdate")
            .arg(format!("--KernelSpecManager.allowed_kernelspecs={KERNEL}"))
            .arg(format!("--MultiKernelManager.default_kernel_name={KERNEL}"))
            .env("METALAKE_UI_PID", watched.to_string())
            .env("JUPYTER_TOKEN", token.as_str())
            .env("JUPYTER_RUNTIME_DIR", dir.join("runtime"))
            .env("JUPYTER_CONFIG_DIR", dir.join("config"))
            .env("JUPYTERLAB_SETTINGS_DIR", dir.join("settings"))
            .env("JUPYTERLAB_WORKSPACES_DIR", dir.join("workspaces"))
            .env("JUPYTER_PATH", dir.join("data"))
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
        match jupyter.ready(ending).await {
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
    async fn ready(&mut self, ending: &AtomicBool) -> Result<u16, String> {
        let start = Instant::now();
        while start.elapsed() < START {
            if ending.load(Ordering::Relaxed) {
                return Err("the UI is ending".to_owned());
            }
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

/// MetaLake's kernel, in the run's directory: its spec, under `data`, which `JUPYTER_PATH` names, and its IPython's
/// own directory, whose startup file binds `lake`; and the ref selector, among the run's extensions, if it was built.
pub(crate) fn kernel(dir: &Path, python: &Path, lake: Option<&Path>) -> std::io::Result<()> {
    let extension = dir.join("data").join("labextensions").join(SELECTOR);
    for path in <crate::assets::Selector as rust_embed::RustEmbed>::iter() {
        let file = <crate::assets::Selector as rust_embed::RustEmbed>::get(&path).expect("an embedded file");
        let to = extension.join(path.as_ref());
        std::fs::create_dir_all(to.parent().expect("a file's directory"))?;
        std::fs::write(to, &file.data)?;
    }
    let ipython = dir.join("ipython");
    let startup = ipython.join("profile_default").join("startup");
    std::fs::create_dir_all(&startup)?;
    std::fs::write(startup.join("00-metalake.py"), STARTUP)?;
    let mut env = serde_json::Map::new();
    env.insert("IPYTHONDIR".into(), ipython.display().to_string().into());
    if let Some(lake) = lake {
        env.insert(LAKE.into(), lake.display().to_string().into());
    }
    let spec = serde_json::json!({
        "argv": [python.display().to_string(), "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": KERNEL_SHOWN,
        "language": "python",
        "env": env,
        "metadata": {"debugger": true},
    });
    let specs = dir.join("data").join("kernels").join(KERNEL);
    std::fs::create_dir_all(&specs)?;
    std::fs::write(specs.join("kernel.json"), serde_json::to_vec_pretty(&spec)?)
}

/// The run's directory, readable by its user alone: Jupyter writes its token in the files it keeps there (its runtime
/// file, its page that opens the browser), which any other user of the machine could otherwise read. On Windows a
/// directory under the user's home is theirs alone already.
fn private(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)?;
    #[cfg(unix)]
    std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    Ok(())
}

/// One request to the JupyterLab at `port`, with its token: its status code, or none if it does not answer.
pub(crate) async fn asked(port: u16, method: &str, path: &str, token: &str) -> Option<u16> {
    fetched(port, method, path, token, false)
        .await
        .map(|(status, _)| status)
}

/// One request, and with `whole`, its whole answer, head and body, as text.
pub(crate) async fn fetched(port: u16, method: &str, path: &str, token: &str, whole: bool) -> Option<(u16, String)> {
    let ask = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
        let request = format!(
            "{method} {PREFIX}{path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: token {token}\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.ok()?;
        let mut answer = Vec::new();
        if whole {
            stream.read_to_end(&mut answer).await.ok()?;
        } else {
            let mut head = [0u8; 16];
            let read = stream.read(&mut head).await.ok()?;
            answer.extend_from_slice(&head[..read]);
        }
        let text = String::from_utf8_lossy(&answer).into_owned();
        let status = text.split_whitespace().nth(1)?.parse().ok()?;
        Some((status, text))
    };
    tokio::time::timeout(Duration::from_secs(5), ask).await.ok().flatten()
}
