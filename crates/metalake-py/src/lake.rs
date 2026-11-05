//! `connect`, `init` and the lake they answer.

use std::path::PathBuf;
use std::sync::Arc;

use metalake_client::{Error, Lake, RefKind, Session};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::info::{History, PyRef, PyStatus};
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
    /// A new ref, which `information_schema` shows at once.
    fn create_ref(&self, py: Python<'_>, kind: RefKind, name: String, start: Option<String>) -> PyResult<PyRef> {
        let (lake, session) = (self.lake.clone(), self.session.clone());
        let created = wait(py, async move {
            let created = lake.create_ref(kind, &name, start.as_deref()).await?;
            session.refresh().await?;
            Ok::<_, Error>(created)
        })?;
        created.map(PyRef::from).map_err(|e| to_python(py, e))
    }

    fn delete_ref(&self, py: Python<'_>, kind: RefKind, name: String) -> PyResult<()> {
        let (lake, session) = (self.lake.clone(), self.session.clone());
        let deleted = wait(py, async move {
            lake.delete_ref(kind, &name).await?;
            session.refresh().await
        })?;
        deleted.map_err(|e| to_python(py, e))
    }

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

    /// What the lake is. Printed, it reads as `metalake status` does.
    fn status(&self, py: Python<'_>) -> PyResult<PyStatus> {
        let lake = self.lake.clone();
        let status = wait(py, async move { lake.status().await })?;
        status.map(PyStatus::from).map_err(|e| to_python(py, e))
    }

    /// Every branch and tag.
    fn refs(&self, py: Python<'_>) -> PyResult<Vec<PyRef>> {
        let lake = self.lake.clone();
        let refs = wait(py, async move { lake.refs(None).await })?.map_err(|e| to_python(py, e))?;
        Ok(refs.into_iter().map(PyRef::from).collect())
    }

    /// A new branch, from `main` or from `start`: a ref, or `ref@sequence`. It copies nothing.
    #[pyo3(signature = (name, start=None))]
    fn branch(&self, py: Python<'_>, name: String, start: Option<String>) -> PyResult<PyRef> {
        self.create_ref(py, RefKind::Branch, name, start)
    }

    /// A new tag, a state that does not change, from `main` or from `start`: a ref, or `ref@sequence`.
    #[pyo3(signature = (name, start=None))]
    fn tag(&self, py: Python<'_>, name: String, start: Option<String>) -> PyResult<PyRef> {
        self.create_ref(py, RefKind::Tag, name, start)
    }

    /// Deletes a branch. Its name is never used again.
    fn delete_branch(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.delete_ref(py, RefKind::Branch, name)
    }

    /// Deletes a tag. Its name is never used again.
    fn delete_tag(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.delete_ref(py, RefKind::Tag, name)
    }

    /// The commits of a ref, the latest first, or, with `table`, `namespace.table`, the snapshots of that table.
    #[pyo3(signature = (on="main".to_string(), *, limit=20, table=None))]
    fn log(&self, py: Python<'_>, on: String, limit: i64, table: Option<String>) -> PyResult<History> {
        let lake = self.lake.clone();
        let history = wait(py, async move {
            Ok(match table {
                Some(table) => {
                    let snapshots = lake.snapshots(&on, &table, limit).await?;
                    History::Snapshots(snapshots.into_iter().map(Into::into).collect())
                }
                None => History::Commits(lake.commits(&on, limit).await?.into_iter().map(Into::into).collect()),
            })
        })?;
        history.map_err(|e: Error| to_python(py, e))
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
