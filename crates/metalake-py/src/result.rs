//! What a statement answers. A result is a read that has not happened: each read runs it, from the same files, as a stream
//! of Arrow batches that anything speaking Arrow can take with no copy.

use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::sync::{Arc, Mutex, TryLockError};

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use futures_util::StreamExt;
use metalake_client::{Error, Prepared};
use pyo3::exceptions::PyStopIteration;
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

/// A failure of a stream, kept for when whoever read it through Arrow comes back.
type Failure = Arc<Mutex<Option<Error>>>;

/// A read as Arrow's C interface takes it. It may run under a reader that holds the GIL, as Polars does.
struct Reader {
    pump: Pump,
    schema: SchemaRef,
    failure: Failure,
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
                    *self.failure.lock().expect("a failure is only set") = Some(error);
                    return Some(Err(said));
                }
                Err(_) => {
                    if let Err(interrupted) = Python::attach(|py| py.check_signals()) {
                        return Some(Err(ArrowError::ExternalError(Box::new(interrupted))));
                    }
                }
            }
        }
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
    failure: Failure,
}

impl QueryResult {
    pub fn new(prepared: Prepared) -> Self {
        Self {
            prepared: Arc::new(prepared),
            failure: Failure::default(),
        }
    }
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
        let reader = Reader {
            pump: Pump::start(self.prepared.clone()),
            schema: self.prepared.schema(),
            failure: self.failure.clone(),
        };
        let reader = PyRecordBatchReader::new(Box::new(reader));
        Bound::new(py, reader)?.call_method1("__arrow_c_stream__", (requested_schema,))
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
