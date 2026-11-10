//! A table's changes as Python iterates them: each step the rows of the commits that came, with the position to go on
//! after, which a reader keeps with what it did with them. A step waits for a commit, and Ctrl-C ends the wait with the
//! reader where it was.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use futures_util::TryStreamExt;
use metalake_client::{Error, Follower, Prepared};
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::errors::to_python;
use crate::owner::Owner;
use crate::result::QueryResult;
use crate::runtime::wait;

/// A reader of a table's changes.
#[pyclass(name = "Follower", module = "metalake", frozen)]
pub struct PyFollower {
    owner: Owner,
    follower: Arc<Mutex<Follower>>,
    interval: Duration,
}

impl PyFollower {
    pub fn new(owner: Owner, follower: Follower, interval: Duration) -> Self {
        Self {
            owner,
            follower: Arc::new(Mutex::new(follower)),
            interval,
        }
    }
}

/// The rows of the commits a step took, and where a reader that dealt with them goes on after.
#[pyclass(name = "Step", module = "metalake", frozen, get_all)]
pub struct PyStep {
    /// The rows, with `_change_type`, `_commit_seq` and `_commit_timestamp`: a result, as `sql` answers.
    rows: Py<QueryResult>,
    /// The commit to follow again after, with `follow(after=…)`, once these rows are dealt with.
    position: Option<i64>,
}

#[pymethods]
impl PyStep {
    fn __repr__(&self) -> String {
        format!("Step(position={:?})", self.position)
    }
}

#[pymethods]
impl PyFollower {
    /// The commit a reader that dealt with every step so far goes on after; None before the first.
    #[getter]
    fn position(&self, py: Python<'_>) -> PyResult<Option<i64>> {
        self.owner.check(py, "Follower")?;
        let follower = self.follower.clone();
        wait(py, async move { follower.lock().await.position() })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next step, once a commit comes. Its rows are read when it is given, so it counts then: a reader that fails
    /// on them goes on after the position it kept, with a new `follow`.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyStep> {
        self.owner.check(py, "Follower")?;
        let (follower, interval) = (self.follower.clone(), self.interval);
        let taken = wait(py, async move {
            let mut follower = follower.lock().await;
            let rows = follower.wait(interval).await?;
            let schema = follower.schema();
            let batches: Vec<RecordBatch> = rows.try_collect().await?;
            let prepared = Prepared::from_batches(schema, batches)?;
            Ok::<_, Error>((prepared, follower.position()))
        })?;
        let (prepared, position) = taken.map_err(|e| to_python(py, e))?;
        let rows = Py::new(py, QueryResult::new(prepared, self.owner.clone()))?;
        Ok(PyStep { rows, position })
    }

    fn __repr__(&self) -> String {
        "Follower()".to_string()
    }
}
