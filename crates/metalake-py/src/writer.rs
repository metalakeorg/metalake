//! A producer's writer as Python uses it: rows written as they come, committed on the writer's cadence, each commit
//! idempotent with the producer's version when it has a key. With `with`, what it holds is committed at a clean end;
//! after an exception it is not, since the producer may have stopped within a version, and a commit of part of one
//! would take the whole version for done: the producer gives it again from `version` when it comes back.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use metalake_client::{Error, Writer, Wrote};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_arrow::input::AnyRecordBatch;
use tokio::sync::Mutex;

use crate::errors::to_python;
use crate::owner::Owner;
use crate::runtime::wait;
use crate::table::PyWritten;

/// A writer into a table, until it is closed.
#[pyclass(name = "Writer", module = "metalake", frozen)]
pub struct PyWriter {
    owner: Owner,
    writer: Arc<Mutex<Option<Writer>>>,
}

impl PyWriter {
    pub fn new(owner: Owner, writer: Writer) -> Self {
        Self {
            owner,
            writer: Arc::new(Mutex::new(Some(writer))),
        }
    }

    fn closed() -> Error {
        Error::usage("this writer is closed")
    }
}

#[pymethods]
impl PyWriter {
    /// The version its key reached in the table: a producer goes on with what comes after it. None without a key, or
    /// before its first commit.
    #[getter]
    fn version(&self, py: Python<'_>) -> PyResult<Option<i64>> {
        self.owner.check(py, "Writer")?;
        let writer = self.writer.clone();
        let version = wait(py, async move { writer.lock().await.as_ref().map(Writer::version) })?;
        version.ok_or_else(|| to_python(py, Self::closed()))
    }

    /// Takes rows, from anything that speaks Arrow, with the producer's version of them when the writer has a key:
    /// several writes may give one version, and versions never go back. What the writer committed meanwhile is the
    /// answer, if anything; rows of a version the key already passed are skipped.
    #[pyo3(signature = (data, version=None))]
    fn write(&self, py: Python<'_>, data: AnyRecordBatch, version: Option<i64>) -> PyResult<Option<PyWritten>> {
        self.owner.check(py, "Writer")?;
        let reader = data.into_reader()?;
        let schema = reader.schema();
        let mut batches = reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .map_err(|e| to_python(py, e.into()))?;
        // No rows still let the cadence commit what is held, as a quiet producer's heartbeat.
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema));
        }
        let writer = self.writer.clone();
        let wrote = wait(py, async move {
            let mut writer = writer.lock().await;
            let writer = writer.as_mut().ok_or_else(Self::closed)?;
            let mut committed = None;
            for batch in batches {
                if let Wrote::Committed(written) = writer.write(batch, version).await? {
                    committed = Some(written);
                }
            }
            Ok::<_, Error>(committed)
        })?;
        Ok(wrote.map_err(|e| to_python(py, e))?.map(PyWritten::from))
    }

    /// Commits what it holds, if anything. With a key, between versions.
    fn flush(&self, py: Python<'_>) -> PyResult<Option<PyWritten>> {
        self.owner.check(py, "Writer")?;
        let writer = self.writer.clone();
        let flushed = wait(py, async move {
            let mut writer = writer.lock().await;
            writer.as_mut().ok_or_else(Self::closed)?.flush().await
        })?;
        Ok(flushed.map_err(|e| to_python(py, e))?.map(PyWritten::from))
    }

    /// Commits what it holds, and ends. A closed writer takes nothing more; closing it again does nothing.
    fn close(&self, py: Python<'_>) -> PyResult<Option<PyWritten>> {
        self.owner.check(py, "Writer")?;
        let writer = self.writer.clone();
        let closed = wait(py, async move {
            match writer.lock().await.take() {
                Some(writer) => writer.close().await,
                None => Ok(None),
            }
        })?;
        Ok(closed.map_err(|e| to_python(py, e))?.map(PyWritten::from))
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// At a clean end, commits what it holds. After an exception, drops it: the producer gives it again from
    /// `version`, whole.
    #[pyo3(signature = (*failure))]
    fn __exit__(&self, py: Python<'_>, failure: &Bound<'_, PyTuple>) -> PyResult<()> {
        let failed = failure.iter().next().is_some_and(|kind| !kind.is_none());
        if failed {
            let writer = self.writer.clone();
            return wait(py, async move {
                writer.lock().await.take();
            });
        }
        self.close(py).map(|_| ())
    }

    fn __repr__(&self) -> String {
        "Writer()".to_string()
    }
}
