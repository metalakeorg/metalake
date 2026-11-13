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

/// The line that installs what the Notebook needs.
pub const INSTALL: &str = r#"pip install "metalake[notebook]""#;

/// How long a look for JupyterLab may take: a Python's start, an import's search.
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

    /// Whether JupyterLab can be imported by the Python, as the Notebook would start it; not if there is none, or it
    /// cannot be run.
    pub async fn jupyterlab(&self) -> bool {
        #[cfg(test)]
        if let Some(found) = self.found {
            return found;
        }
        let Some(python) = &self.python else {
            return false;
        };
        let look = Command::new(python)
            .args([
                "-c",
                "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('jupyterlab') else 3)",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status();
        matches!(tokio::time::timeout(LOOK, look).await, Ok(Ok(status)) if status.success())
    }
}
