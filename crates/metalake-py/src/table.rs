//! A table of a lake, by its name. It holds no data and no point: each operation takes the table as it is then.

use std::sync::Arc;
use std::thread;

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use futures_util::{Stream, stream};
use metalake_client::{Details, Error, Lake, Session, TablePath, Written};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::input::AnyRecordBatch;
use tokio::sync::mpsc;

use crate::errors::to_python;
use crate::runtime::wait;

/// Batches read ahead of the writer.
const AHEAD: usize = 2;

/// What is given to a table, as a stream. A thread of its own reads it, since it may wait for Python, which must find the
/// GIL free and must not hold up the runtime; nothing here is called from under a library.
fn batches_of(
    reader: Box<dyn RecordBatchReader + Send>,
) -> impl Stream<Item = Result<RecordBatch, Error>> + Unpin + Send {
    let (sender, mut receiver) = mpsc::channel(AHEAD);
    thread::spawn(move || {
        for batch in reader {
            if sender.blocking_send(batch.map_err(Error::from)).is_err() {
                break;
            }
        }
    });
    stream::poll_fn(move |cx| receiver.poll_recv(cx))
}

fn details(message: Option<String>, author: Option<String>, idempotent: Option<(String, i64)>) -> Details {
    Details {
        author,
        message,
        idempotent,
    }
}

/// What a write left.
#[pyclass(name = "Written", module = "metalake", frozen, get_all)]
pub struct PyWritten {
    commit: Option<i64>,
    rows: u64,
    applied: bool,
}

impl From<Written> for PyWritten {
    fn from(written: Written) -> Self {
        Self {
            commit: written.commit,
            rows: written.rows,
            applied: written.applied,
        }
    }
}

#[pymethods]
impl PyWritten {
    fn __repr__(&self) -> String {
        format!(
            "Written(commit={:?}, rows={}, applied={})",
            self.commit, self.rows, self.applied
        )
    }
}

#[pyclass(name = "Table", module = "metalake", frozen)]
pub struct PyTable {
    lake: Arc<Lake>,
    session: Arc<Session>,
    path: TablePath,
}

impl PyTable {
    /// The table named, which must exist.
    pub fn open(py: Python<'_>, lake: Arc<Lake>, session: Arc<Session>, name: &str) -> PyResult<Self> {
        let path = TablePath::parse(name).map_err(|e| to_python(py, e))?;
        let (reached, wanted) = (lake.clone(), path.clone());
        wait(py, async move { reached.table(&wanted).await.map(|_| ()) })?.map_err(|e| to_python(py, e))?;
        Ok(Self { lake, session, path })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<SchemaRef> {
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let found = wait(py, async move { lake.table(&path).await })?;
        let (_, table) = found.map_err(|e| to_python(py, e))?;
        Ok(table.schema().clone())
    }
}

#[pymethods]
impl PyTable {
    /// Its whole name: `ref.namespace.table`.
    #[getter]
    fn name(&self) -> String {
        self.path.to_string()
    }

    /// The names of its columns, now.
    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let schema = self.schema(py)?;
        Ok(schema.fields().iter().map(|f| f.name().clone()).collect())
    }

    /// Adds rows, from anything that speaks Arrow, in one commit. Columns go by name. With `idempotent`, a writer's id and
    /// a version it never repeats for different data, repeating a call applies nothing.
    #[pyo3(signature = (data, *, message=None, author=None, idempotent=None))]
    fn append(
        &self,
        py: Python<'_>,
        data: AnyRecordBatch,
        message: Option<String>,
        author: Option<String>,
        idempotent: Option<(String, i64)>,
    ) -> PyResult<PyWritten> {
        let reader = data.into_reader()?;
        let given = reader.schema();
        let batches = batches_of(reader);
        let details = details(message, author, idempotent);
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let written = wait(py, async move { lake.append(&path, &given, batches, details).await })?;
        written.map(PyWritten::from).map_err(|e| to_python(py, e))
    }

    /// Replaces what the table holds with these rows, in one commit: no rows leave it empty. What it held stays readable
    /// at its earlier points. A write that lands meanwhile makes this a `ConflictError`, and nothing is lost.
    #[pyo3(signature = (data, *, message=None, author=None, idempotent=None))]
    fn overwrite(
        &self,
        py: Python<'_>,
        data: AnyRecordBatch,
        message: Option<String>,
        author: Option<String>,
        idempotent: Option<(String, i64)>,
    ) -> PyResult<PyWritten> {
        let reader = data.into_reader()?;
        let given = reader.schema();
        let batches = batches_of(reader);
        let details = details(message, author, idempotent);
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let written = wait(py, async move { lake.overwrite(&path, &given, batches, details).await })?;
        written.map(PyWritten::from).map_err(|e| to_python(py, e))
    }

    /// Deletes the rows that a filter matches, in one commit. The filter is a SQL expression, and is required: `"true"`
    /// says every row.
    #[pyo3(signature = (filter, *, message=None, author=None, idempotent=None))]
    fn delete(
        &self,
        py: Python<'_>,
        filter: String,
        message: Option<String>,
        author: Option<String>,
        idempotent: Option<(String, i64)>,
    ) -> PyResult<PyWritten> {
        let details = details(message, author, idempotent);
        let (session, path) = (self.session.clone(), self.path.clone());
        let written = wait(py, async move { session.delete(&path, &filter, details).await })?;
        written.map(PyWritten::from).map_err(|e| to_python(py, e))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.schema(py)?.as_ref())?)
    }

    fn __repr__(&self) -> String {
        format!("Table({:?})", self.path.to_string())
    }
}
