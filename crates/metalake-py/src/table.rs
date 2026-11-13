//! A table of a lake, by its name. It holds no data and no point: each operation takes the table as it is then.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use futures_util::{Stream, stream};
use metalake_client::{Details, Error, Lake, Mode, Session, StartAt, TablePath, WriterOptions, Written};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::input::AnyRecordBatch;
use tokio::sync::mpsc;

use crate::errors::to_python;
use crate::follow::PyFollower;
use crate::owner::Owner;
use crate::result::QueryResult;
use crate::runtime::wait;
use crate::writer::PyWriter;

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
    inserted: u64,
    updated: u64,
}

impl From<Written> for PyWritten {
    fn from(written: Written) -> Self {
        Self {
            commit: written.commit,
            rows: written.rows,
            applied: written.applied,
            inserted: written.inserted,
            updated: written.updated,
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
    owner: Owner,
    lake: Arc<Lake>,
    session: Arc<Session>,
    path: TablePath,
}

impl PyTable {
    /// The table named, which must exist.
    pub fn open(py: Python<'_>, owner: Owner, lake: Arc<Lake>, session: Arc<Session>, name: &str) -> PyResult<Self> {
        let path = TablePath::parse_on(name, &session.current_ref()).map_err(|e| to_python(py, e.into()))?;
        let (reached, wanted) = (lake.clone(), path.clone());
        wait(py, async move { reached.table(&wanted).await.map(|_| ()) })?.map_err(|e| to_python(py, e))?;
        Ok(Self {
            owner,
            lake,
            session,
            path,
        })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<SchemaRef> {
        self.owner.check(py, "Table")?;
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
        self.owner.check(py, "Table")?;
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
        self.owner.check(py, "Table")?;
        let reader = data.into_reader()?;
        let given = reader.schema();
        let batches = batches_of(reader);
        let details = details(message, author, idempotent);
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let written = wait(py, async move { lake.overwrite(&path, &given, batches, details).await })?;
        written.map(PyWritten::from).map_err(|e| to_python(py, e))
    }

    /// Replaces the rows whose key `on` is among these rows, and adds the others, in one commit. The key's values must be
    /// there and be unique. A table that changes meanwhile is a `ConflictError`, and trying again is safe.
    #[pyo3(signature = (data, on, *, message=None, author=None, idempotent=None))]
    fn upsert(
        &self,
        py: Python<'_>,
        data: AnyRecordBatch,
        on: Vec<String>,
        message: Option<String>,
        author: Option<String>,
        idempotent: Option<(String, i64)>,
    ) -> PyResult<PyWritten> {
        self.owner.check(py, "Table")?;
        let reader = data.into_reader()?;
        let given = reader.schema();
        let batches = batches_of(reader);
        let details = details(message, author, idempotent);
        let (lake, session, path) = (self.lake.clone(), self.session.clone(), self.path.clone());
        let written = wait(py, async move {
            lake.upsert(&session, &path, &given, batches, &on, details).await
        })?;
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
        self.owner.check(py, "Table")?;
        let details = details(message, author, idempotent);
        let (session, path) = (self.session.clone(), self.path.clone());
        let written = wait(py, async move { session.delete(&path, &filter, details).await })?;
        written.map(PyWritten::from).map_err(|e| to_python(py, e))
    }

    /// The rows the commits after `after`, and up to `until` or to the last one a second old, inserted and deleted, with
    /// `_change_type`, `_commit_seq` and `_commit_timestamp`, as `sql` answers them: `table_changes` of this table.
    /// Ranges chain: after `a` up to `b`, then after `b`, take each commit once.
    #[pyo3(signature = (after, until=None))]
    fn changes(&self, py: Python<'_>, after: i64, until: Option<i64>) -> PyResult<QueryResult> {
        self.owner.check(py, "Table")?;
        if let Some(until) = until.filter(|until| *until < after) {
            return Err(to_python(
                py,
                Error::usage(format!(
                    "changes of {} after {after} up to {until}: the end comes before the start",
                    self.path
                )),
            ));
        }
        let name = self.path.to_string().replace('\'', "''");
        let until = until.map(|until| format!(", {until}")).unwrap_or_default();
        let text = format!("select * from table_changes('{name}', {after}{until})");
        let session = self.session.clone();
        let prepared = wait(py, async move { session.run(&text).await })?;
        Ok(QueryResult::new(
            prepared.map_err(|e| to_python(py, e))?,
            self.owner.clone(),
        ))
    }

    /// A reader of the table's changes, a step at a time: from what it holds now (`"snapshot"`), from now
    /// (`"latest"`), after a commit (`after`, a step's `position`), or after a time (`since`). Each step waits for the
    /// next commit, asking every `interval` seconds, and Ctrl-C ends a wait with the reader where it was.
    #[pyo3(signature = (start="snapshot", *, after=None, since=None, interval=1.0))]
    fn follow(
        &self,
        py: Python<'_>,
        start: &str,
        after: Option<i64>,
        since: Option<SystemTime>,
        interval: f64,
    ) -> PyResult<PyFollower> {
        self.owner.check(py, "Table")?;
        let start = match (start, after, since) {
            (_, Some(after), None) => StartAt::After(after),
            (_, None, Some(since)) => StartAt::Time(since),
            (_, Some(_), Some(_)) => return Err(to_python(py, Error::usage("follow takes after or since, not both"))),
            ("snapshot", None, None) => StartAt::Snapshot,
            ("latest", None, None) => StartAt::Latest,
            (other, None, None) => {
                let message = format!("follow starts at \"snapshot\" or \"latest\", not {other:?}");
                return Err(to_python(py, Error::usage(message)));
            }
        };
        let interval = Duration::try_from_secs_f64(interval)
            .ok()
            .filter(|i| !i.is_zero())
            .ok_or_else(|| to_python(py, Error::usage("the interval is a number of seconds above 0")))?;
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let follower = wait(py, async move { lake.follow(&path, start).await })?;
        let follower = follower.map_err(|e| to_python(py, e))?;
        Ok(PyFollower::new(self.owner.clone(), follower, interval))
    }

    /// A writer of a stream into the table, which commits on its own cadence: each `commit_every` seconds, or once what
    /// it holds reaches `max_bytes`. With `app_id`, each commit is idempotent with the highest `version` given, and
    /// `version` says where a producer that comes back goes on from. With `upsert_on`, rows replace those with their key.
    /// Used with `with`, it commits what it holds at the end.
    #[pyo3(signature = (*, app_id=None, upsert_on=None, commit_every=1.0, max_bytes=67108864, author=None, message=None))]
    #[allow(clippy::too_many_arguments)]
    fn writer(
        &self,
        py: Python<'_>,
        app_id: Option<String>,
        upsert_on: Option<Vec<String>>,
        commit_every: f64,
        max_bytes: usize,
        author: Option<String>,
        message: Option<String>,
    ) -> PyResult<PyWriter> {
        self.owner.check(py, "Table")?;
        let commit_every = Duration::try_from_secs_f64(commit_every)
            .map_err(|_| to_python(py, Error::usage("commit_every is a number of seconds")))?;
        let options = WriterOptions {
            app_id,
            mode: upsert_on.map_or(Mode::Append, Mode::Upsert),
            commit_every,
            max_bytes,
            author,
            message,
        };
        let (lake, path) = (self.lake.clone(), self.path.clone());
        let writer = wait(py, async move { lake.writer(&path, options).await })?;
        let writer = writer.map_err(|e| to_python(py, e))?;
        Ok(PyWriter::new(self.owner.clone(), writer))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.schema(py)?.as_ref())?)
    }

    fn __repr__(&self) -> String {
        format!("Table({:?})", self.path.to_string())
    }
}
