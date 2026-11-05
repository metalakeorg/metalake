//! What a statement answers. A result is a read that has not happened: each read runs it, from the same files, as a stream
//! of Arrow batches that anything speaking Arrow can take with no copy.

use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::sync::{Arc, Mutex, TryLockError};

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use futures_util::StreamExt;
use metalake_client::{Error, Prepared};
use pyo3::exceptions::{PyImportError, PyStopIteration};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::{PyRecordBatch, PyRecordBatchReader};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::errors::to_python;
use crate::runtime::{self, PATIENCE};

/// Batches ahead of the reader: a statement waits past these, so a slow reader does not fill memory.
const AHEAD: usize = 2;

type Batch = Result<RecordBatch, Error>;

/// One read at work. Dropping it ends it.
struct Pump {
    batches: Receiver<Batch>,
    room: Arc<Semaphore>,
    task: AbortHandle,
}

impl Drop for Pump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Pump {
    fn start(prepared: Arc<Prepared>) -> Self {
        let (sender, batches) = channel();
        let room = Arc::new(Semaphore::new(AHEAD));
        let held = room.clone();
        let task = runtime::spawn(async move {
            let mut stream = match prepared.read() {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            };
            while let Some(batch) = stream.next().await {
                let Ok(permit) = held.acquire().await else {
                    break;
                };
                permit.forget();
                if sender.send(batch.map_err(Error::from)).is_err() {
                    break;
                }
            }
        });
        Self { batches, room, task }
    }

    /// The next batch, none at the end, or an error if this wait ran out.
    fn wait(&mut self) -> Result<Option<Batch>, RecvTimeoutError> {
        match self.batches.recv_timeout(PATIENCE) {
            Ok(batch) => {
                self.room.add_permits(1);
                Ok(Some(batch))
            }
            Err(RecvTimeoutError::Disconnected) => Ok(None),
            Err(timeout) => Err(timeout),
        }
    }
}

/// Why a read stopped. Arrow's interface carries only text, so a library says it in its own words; this is what was
/// really meant, for whoever called the library.
enum Stopped {
    Failed(Error),
    Interrupted(PyErr),
}

/// Where one read leaves why it stopped.
type Slot = Arc<Mutex<Option<Stopped>>>;

/// A read as Arrow's C interface takes it. It may run under a reader that holds the GIL, as Polars does.
struct Reader {
    pump: Pump,
    schema: SchemaRef,
    stopped: Slot,
}

impl Reader {
    fn stop(&self, stopped: Stopped) {
        *self.stopped.lock().expect("a reason is only set") = Some(stopped);
    }
}

impl Iterator for Reader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.pump.wait() {
                Ok(None) => return None,
                Ok(Some(Ok(batch))) => return Some(Ok(batch)),
                Ok(Some(Err(error))) => {
                    let said = ArrowError::ExternalError(error.message.clone().into());
                    self.stop(Stopped::Failed(error));
                    return Some(Err(said));
                }
                Err(_) => {
                    if let Some(raised) = interrupted() {
                        self.stop(Stopped::Interrupted(raised));
                        return Some(Err(ArrowError::ExternalError("interrupted".into())));
                    }
                }
            }
        }
    }
}

/// An interrupt that came, seen from under a library that may have released the GIL. `Python::attach` will not do here:
/// when `to_polars` is on the stack, PyO3 counts the GIL as held on this thread, and Polars, with a PyO3 of its own, has
/// released it, so that would touch Python without it. `PyGILState_Ensure` asks Python.
fn interrupted() -> Option<PyErr> {
    // Safety: the state is released once, and the error is taken while it is held.
    unsafe {
        let state = ffi::PyGILState_Ensure();
        let raised = match ffi::PyErr_CheckSignals() {
            0 => None,
            _ => PyErr::take(Python::assume_attached()),
        };
        ffi::PyGILState_Release(state);
        raised
    }
}

impl RecordBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The rows of a statement, at the point it was prepared. It can be read any number of times, and every read is the same.
#[pyclass(name = "Result", module = "metalake", frozen)]
pub struct QueryResult {
    prepared: Arc<Prepared>,
}

impl QueryResult {
    pub fn new(prepared: Prepared) -> Self {
        Self {
            prepared: Arc::new(prepared),
        }
    }

    /// One read, for anything that speaks Arrow.
    fn reader<'py>(&self, py: Python<'py>, stopped: &Slot) -> PyResult<Bound<'py, PyRecordBatchReader>> {
        let reader = Reader {
            pump: Pump::start(self.prepared.clone()),
            schema: self.prepared.schema(),
            stopped: stopped.clone(),
        };
        Bound::new(py, PyRecordBatchReader::new(Box::new(reader)))
    }

    /// What `convert` makes of one read. If the read stopped, that is what is raised, and not a library's words for it.
    fn converted<'py>(
        &self,
        py: Python<'py>,
        convert: impl FnOnce(Bound<'py, PyRecordBatchReader>) -> PyResult<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stopped = Slot::default();
        let reader = self.reader(py, &stopped)?;
        convert(reader).map_err(|theirs| match stopped.lock().expect("a reason is only set").take() {
            Some(Stopped::Failed(error)) => to_python(py, error),
            Some(Stopped::Interrupted(interrupted)) => interrupted,
            None => theirs,
        })
    }
}

/// A library a conversion needs, which only that conversion loads.
fn library<'py>(py: Python<'py>, method: &str, name: &str, extra: &str) -> PyResult<Bound<'py, PyModule>> {
    py.import(name)
        .map_err(|e| match e.is_instance_of::<PyImportError>(py) {
            true => PyImportError::new_err(format!("{method} needs {name}: pip install metalake[{extra}]")),
            false => e,
        })
}

#[pymethods]
impl QueryResult {
    /// The names of the columns.
    #[getter]
    fn columns(&self) -> Vec<String> {
        let schema = self.prepared.schema();
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.prepared.schema().as_ref())?)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader(py, &Slot::default())?;
        reader.call_method1("__arrow_c_stream__", (requested_schema,))
    }

    /// The rows as a `polars.DataFrame`.
    fn to_polars<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let polars = library(py, "to_polars", "polars", "polars")?;
        self.converted(py, |read| polars.getattr("DataFrame")?.call1((read,)))
    }

    /// The rows as a `pyarrow.Table`.
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pyarrow = library(py, "to_arrow", "pyarrow", "pyarrow")?;
        self.converted(py, |read| pyarrow.getattr("table")?.call1((read,)))
    }

    /// The rows as a `pandas.DataFrame`, which pandas makes through pyarrow.
    fn to_pandas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        library(py, "to_pandas", "pandas", "pandas")?;
        let pyarrow = library(py, "to_pandas", "pyarrow", "pandas")?;
        let table = self.converted(py, |read| pyarrow.getattr("table")?.call1((read,)))?;
        table.call_method0("to_pandas")
    }

    /// One read, batch by batch.
    fn __iter__(&self) -> Batches {
        Batches {
            pump: Mutex::new(Some(Pump::start(self.prepared.clone()))),
        }
    }
}

/// One read of a result, batch by batch, from one thread at a time. Another is refused and not left waiting for a lock
/// with the GIL in hand, which the first would need to go on.
#[pyclass(name = "Batches", module = "metalake", frozen)]
pub struct Batches {
    pump: Mutex<Option<Pump>>,
}

#[pymethods]
impl Batches {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next batch, waiting with the GIL released. An interrupt or a failure ends the read.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyBatch> {
        let mut pump = match self.pump.try_lock() {
            Ok(pump) => pump,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                return Err(to_python(py, Error::failed("another thread is reading this result")));
            }
        };
        let Some(reading) = pump.as_mut() else {
            return Err(PyStopIteration::new_err(()));
        };
        loop {
            match py.detach(|| reading.wait()) {
                Ok(Some(Ok(batch))) => return Ok(PyBatch { batch }),
                Ok(Some(Err(error))) => {
                    *pump = None;
                    return Err(to_python(py, error));
                }
                Ok(None) => {
                    *pump = None;
                    return Err(PyStopIteration::new_err(()));
                }
                Err(_) => {
                    if let Err(interrupted) = py.check_signals() {
                        *pump = None;
                        return Err(interrupted);
                    }
                }
            }
        }
    }
}

/// One batch of a result, itself Arrow.
#[pyclass(name = "Batch", module = "metalake", frozen)]
pub struct PyBatch {
    batch: RecordBatch,
}

#[pymethods]
impl PyBatch {
    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    /// The names of the columns.
    #[getter]
    fn columns(&self) -> Vec<String> {
        let schema = self.batch.schema();
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.batch.schema().as_ref())?)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let batch = PyRecordBatch::new(self.batch.clone());
        Bound::new(py, batch)?.call_method1("__arrow_c_array__", (requested_schema,))
    }

    fn __repr__(&self) -> String {
        format!("Batch(rows={}, columns={:?})", self.num_rows(), self.columns())
    }
}
