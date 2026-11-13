//! Whether this `metalake` can give a Notebook (ADR 0008): JupyterLab, run by the Python metalake was installed in. The
//! `metalake` command pip installs is a Python script, which names its Python in [`PYTHON`]; a `metalake` built alone
//! has none, and so no Notebook. JupyterLab is looked for each time it is asked, so that installing it while the UI
//! runs is seen without a restart.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

/// The variable that names the Python the UI's Notebook runs in.
pub const PYTHON: &str = "METALAKE_PYTHON";

/// What the Notebook needs, as pip installs it: the extra of metalake that brings JupyterLab and the kernel.
pub const EXTRA: &str = r#""metalake[notebook]""#;

/// How long a look for JupyterLab may take: a Python's start, and importing what the Notebook runs.
const LOOK: Duration = Duration::from_secs(20);

/// The Python a Notebook would run in, and how to tell whether JupyterLab is there.
#[derive(Debug, Clone, Default)]
pub struct Notebook {
    python: Option<PathBuf>,
    #[cfg(test)]
    found: Option<bool>,
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
            #[cfg(test)]
            found: None,
        }
    }

    /// For a test: a Python whose JupyterLab is `found` or not, without running it.
    #[cfg(test)]
    pub(crate) fn answering(python: bool, found: bool) -> Self {
        Self {
            python: python.then(|| PathBuf::from("python")),
            found: Some(found),
        }
    }

    pub fn python(&self) -> Option<&PathBuf> {
        self.python.as_ref()
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
}
