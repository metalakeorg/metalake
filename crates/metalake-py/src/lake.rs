//! `connect`, `init` and the lake they answer.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use metalake_client::{Error, Lake, MaintainRequest, MergeRequest, RefKind, Retention, Session, Task, duration};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::errors::to_python;
use crate::info::{History, PyDone, PyMerged, PyRef, PyStatus};
use crate::owner::Owner;
use crate::result::QueryResult;
use crate::runtime::wait;
use crate::table::PyTable;

/// A length of time: `"30d"`, `"12h"`, `"15m"`, `"45s"`, or a `timedelta`.
#[derive(FromPyObject)]
pub enum Age {
    Text(String),
    Span(Duration),
}

impl Age {
    fn duration(self) -> metalake_client::Result<Duration> {
        match self {
            Age::Text(text) => duration(&text),
            Age::Span(span) => Ok(span),
        }
    }
}

/// A lake, reached, with the session its statements run in.
#[pyclass(name = "Lake", module = "metalake", frozen)]
pub struct PyLake {
    owner: Owner,
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
            owner: Owner::here(),
            lake: Arc::new(lake),
            session: Arc::new(session),
        })
    }
}

#[pymethods]
impl PyLake {
    /// Closes the connections of this lake, which with `with` happens by itself. Its tables and results go with it, and
    /// its server stays up: that is the lake's, and `stop` ends it. Closing twice does nothing.
    fn close(&self) {
        if self.owner.close() {
            self.lake.close();
        }
    }

    fn __enter__(slf: pyo3::PyRef<'_, Self>) -> PyResult<pyo3::PyRef<'_, Self>> {
        slf.owner.check(slf.py(), "Lake")?;
        Ok(slf)
    }

    #[pyo3(signature = (*_failure))]
    fn __exit__(&self, _failure: &Bound<'_, PyTuple>) {
        self.close();
    }

    /// Runs statements in order, each committed before the next, and answers the rows of the last. A read waits to be
    /// read, at the point of this call; anything else has happened when this returns.
    fn sql(&self, py: Python<'_>, text: String) -> PyResult<QueryResult> {
        self.owner.check(py, "Lake")?;
        let session = self.session.clone();
        let prepared = wait(py, async move { session.run(&text).await })?;
        Ok(QueryResult::new(
            prepared.map_err(|e| to_python(py, e))?,
            self.owner.clone(),
        ))
    }

    /// A table by the name a statement would use: `t`, `namespace.t` or `ref.namespace.t`.
    fn table(&self, py: Python<'_>, name: &str) -> PyResult<PyTable> {
        self.owner.check(py, "Lake")?;
        PyTable::open(py, self.owner.clone(), self.lake.clone(), self.session.clone(), name)
    }

    /// Takes a new picture of what `information_schema` shows, for what others changed. A table or a ref named in a
    /// statement is found without it.
    fn refresh(&self, py: Python<'_>) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        let session = self.session.clone();
        wait(py, async move { session.refresh().await })?.map_err(|e| to_python(py, e))
    }

    /// What the lake is. Printed, it reads as `metalake status` does.
    fn status(&self, py: Python<'_>) -> PyResult<PyStatus> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let status = wait(py, async move { lake.status().await })?;
        status.map(PyStatus::from).map_err(|e| to_python(py, e))
    }

    /// Every branch and tag.
    fn refs(&self, py: Python<'_>) -> PyResult<Vec<PyRef>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let refs = wait(py, async move { lake.refs(None).await })?.map_err(|e| to_python(py, e))?;
        Ok(refs.into_iter().map(PyRef::from).collect())
    }

    /// A new branch, from `main` or from `start`: a ref, or `ref@sequence`. It copies nothing.
    #[pyo3(signature = (name, start=None))]
    fn branch(&self, py: Python<'_>, name: String, start: Option<String>) -> PyResult<PyRef> {
        self.owner.check(py, "Lake")?;
        self.create_ref(py, RefKind::Branch, name, start)
    }

    /// A new tag, a state that does not change, from `main` or from `start`: a ref, or `ref@sequence`.
    #[pyo3(signature = (name, start=None))]
    fn tag(&self, py: Python<'_>, name: String, start: Option<String>) -> PyResult<PyRef> {
        self.owner.check(py, "Lake")?;
        self.create_ref(py, RefKind::Tag, name, start)
    }

    /// Deletes a branch. Its name is never used again.
    fn delete_branch(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        self.delete_ref(py, RefKind::Branch, name)
    }

    /// Deletes a tag. Its name is never used again.
    fn delete_tag(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        self.delete_ref(py, RefKind::Tag, name)
    }

    /// Says what a ref keeps, which `maintain` then acts on: what was committed within `max_age`, at least its latest
    /// `min_snapshots`, and everything from the sequence `min_seq` on. It replaces all the ref said before, so what is
    /// left out stops being kept. With no policy, a ref keeps everything.
    #[pyo3(signature = (on, *, max_age=None, min_snapshots=None, min_seq=None))]
    fn retain(
        &self,
        py: Python<'_>,
        on: String,
        max_age: Option<Age>,
        min_snapshots: Option<i32>,
        min_seq: Option<i64>,
    ) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        let max_age = max_age.map(Age::duration).transpose().map_err(|e| to_python(py, e))?;
        let retention = Retention {
            min_seq,
            max_age,
            min_snapshots,
        };
        let lake = self.lake.clone();
        wait(py, async move { lake.retain(&on, &retention).await })?.map_err(|e| to_python(py, e))
    }

    /// Says how long an expired object waits before it is deleted, for the whole lake. Reads should end within it.
    fn grace(&self, py: Python<'_>, age: Age) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        let age = age.duration().map_err(|e| to_python(py, e))?;
        let lake = self.lake.clone();
        wait(py, async move { lake.set_grace(age).await })?.map_err(|e| to_python(py, e))
    }

    /// Runs the core's tasks once, for whatever schedules it, and answers what was done. Every task is safe to repeat and
    /// to run beside commits. In order, they are `times`, `flush`, `compact`, `expire`, `sweep` and `orphans`; `task` runs
    /// one, and `on`, a branch, and `table`, `namespace.table`, narrow a `flush` or a `compact`. `strays`, asked for by
    /// table, is not among them: it lists the table's location, for what an Iceberg client left, for `orphans` to take. If
    /// a task fails, the exception's `done` says what had been done, and running it again goes on from there.
    #[pyo3(signature = (task=None, *, on=None, table=None))]
    fn maintain(
        &self,
        py: Python<'_>,
        task: Option<String>,
        on: Option<String>,
        table: Option<String>,
    ) -> PyResult<Vec<PyDone>> {
        self.owner.check(py, "Lake")?;
        let task = task.as_deref().map(Task::parse).transpose();
        let request = MaintainRequest {
            task: task.map_err(|e| to_python(py, e))?,
            on,
            table,
        };
        let lake = self.lake.clone();
        let run = wait(py, async move { lake.maintain(&request).await })?.map_err(|e| to_python(py, e))?;
        let done: Vec<PyDone> = run.done.into_iter().map(PyDone::from).collect();
        match run.failed {
            None => Ok(done),
            Some(failed) => {
                let raised = to_python(py, failed);
                raised.value(py).setattr("done", done)?;
                Err(raised)
            }
        }
    }

    /// Merges a branch into its parent, every table or those of `tables`, as `namespace.table`. With `check`, says what
    /// that would do and changes nothing. What cannot merge is a `ConflictError`, and leaves both as they were. `split`
    /// allows a selection of tables that one of the branch's commits crossed, by splitting that commit.
    #[pyo3(signature = (branch, *, check=false, tables=None, split=false, message=None, author=None))]
    #[allow(clippy::too_many_arguments, reason = "each is a keyword of Python")]
    fn merge(
        &self,
        py: Python<'_>,
        branch: String,
        check: bool,
        tables: Option<Vec<String>>,
        split: bool,
        message: Option<String>,
        author: Option<String>,
    ) -> PyResult<PyMerged> {
        self.owner.check(py, "Lake")?;
        let request = MergeRequest {
            branch,
            tables: tables.unwrap_or_default(),
            split,
            message,
            author,
            check,
        };
        let (lake, session) = (self.lake.clone(), self.session.clone());
        let merged = wait(py, async move {
            let merged = lake.merge(&request).await?;
            // A table new to the parent shows in `information_schema` at once.
            session.refresh().await?;
            Ok::<_, Error>(merged)
        })?;
        merged.map(PyMerged::from).map_err(|e| to_python(py, e))
    }

    /// The commits of a ref, the latest first, or, with `table`, `namespace.table`, the snapshots of that table.
    #[pyo3(signature = (on="main".to_string(), *, limit=20, table=None))]
    fn log(&self, py: Python<'_>, on: String, limit: i64, table: Option<String>) -> PyResult<History> {
        self.owner.check(py, "Lake")?;
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

/// Brings a lake's catalog to this package's protocol, once per upgrade, by whoever administers the lake, and reaches it.
/// `connect` refuses a catalog that needs it, and nothing does it alone: it holds every ref while it runs.
#[pyfunction]
#[pyo3(signature = (lake=None, *, schema=None))]
pub fn migrate(py: Python<'_>, lake: Option<PathBuf>, schema: Option<String>) -> PyResult<PyLake> {
    let target = text(lake);
    let reached = wait(py, async move {
        let (lake, _) = Lake::migrate_named(target.as_deref(), schema.as_deref()).await?;
        let session = Session::new(&lake).await?;
        Ok((lake, session))
    })?;
    PyLake::new(py, reached)
}

/// Stops a lake's own Postgres, which otherwise stays up, and answers its directory. A `Lake` open on it stops working;
/// `connect` brings the server up again.
#[pyfunction]
#[pyo3(signature = (lake=None))]
pub fn stop(py: Python<'_>, lake: Option<PathBuf>) -> PyResult<PathBuf> {
    let target = text(lake);
    let stopped = wait(py, async move { Lake::stop_named(target.as_deref()) })?;
    stopped.map_err(|e| to_python(py, e))
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
