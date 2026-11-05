//! `connect`, `init` and the lake they answer.

use std::path::PathBuf;
use std::sync::Arc;

use metalake_client::{Lake, Session};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::result::QueryResult;
use crate::runtime::wait;
use crate::table::PyTable;

/// A lake, reached, with the session its statements run in.
#[pyclass(name = "Lake", module = "metalake", frozen)]
pub struct PyLake {
    lake: Arc<Lake>,
    session: Arc<Session>,
}

impl PyLake {
    fn new(py: Python<'_>, reached: metalake_client::Result<(Lake, Session)>) -> PyResult<Self> {
        let (lake, session) = reached.map_err(|e| to_python(py, e))?;
        Ok(Self {
            lake: Arc::new(lake),
            session: Arc::new(session),
        })
    }
}

#[pymethods]
impl PyLake {
    /// Runs statements in order, each committed before the next, and answers the rows of the last. A read waits to be
    /// read, at the point of this call; anything else has happened when this returns.
    fn sql(&self, py: Python<'_>, text: String) -> PyResult<QueryResult> {
        let session = self.session.clone();
        let prepared = wait(py, async move { session.run(&text).await })?;
        Ok(QueryResult::new(prepared.map_err(|e| to_python(py, e))?))
    }

    /// A table by the name a statement would use: `t`, `namespace.t` or `ref.namespace.t`.
    fn table(&self, py: Python<'_>, name: &str) -> PyResult<PyTable> {
        PyTable::open(py, self.lake.clone(), self.session.clone(), name)
    }

    /// Takes a new picture of what `information_schema` shows, for what others changed. A table or a ref named in a
    /// statement is found without it.
    fn refresh(&self, py: Python<'_>) -> PyResult<()> {
        let session = self.session.clone();
        wait(py, async move { session.refresh().await })?.map_err(|e| to_python(py, e))
    }

    fn __repr__(&self) -> String {
        match (self.lake.postgres(), self.lake.local()) {
            (Some(postgres), _) => format!("Lake(postgres={postgres:?})"),
            (None, Some(local)) => format!("Lake(local={:?})", local.display().to_string()),
            (None, None) => "Lake()".to_string(),
        }
    }
}

fn text(path: Option<PathBuf>) -> Option<String> {
    path.map(|p| p.to_string_lossy().into_owned())
}

/// Reaches a lake: a directory, a Postgres, or, naming none, what `metalake` would find from here.
#[pyfunction]
#[pyo3(signature = (lake=None, *, schema=None))]
pub fn connect(py: Python<'_>, lake: Option<PathBuf>, schema: Option<String>) -> PyResult<PyLake> {
    let target = text(lake);
    let reached = wait(py, async move {
        let lake = Lake::connect(target.as_deref(), schema.as_deref()).await?;
        let session = Session::new(&lake).await?;
        Ok((lake, session))
    })?;
    PyLake::new(py, reached)
}

/// Makes a lake: in a directory, with a Postgres of its own and `metalake.toml` beside it, or on a Postgres that exists.
#[pyfunction]
#[pyo3(signature = (catalog, warehouse=None, *, schema=None))]
pub fn init(py: Python<'_>, catalog: PathBuf, warehouse: Option<PathBuf>, schema: Option<String>) -> PyResult<PyLake> {
    let catalog = catalog.to_string_lossy().into_owned();
    let warehouse = text(warehouse);
    let made = wait(py, async move {
        let lake = Lake::init(&catalog, warehouse.as_deref(), schema.as_deref()).await?;
        let session = Session::new(&lake).await?;
        Ok((lake, session))
    })?;
    PyLake::new(py, made)
}
