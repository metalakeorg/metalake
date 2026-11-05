//! What a statement answers, as a stream of Arrow batches that anything speaking Arrow can read, with no copy.
//!
//! The stream runs in a task of the runtime and its batches come through a plain channel. A wait on tokio's timers would
//! not end while the statement keeps every worker busy, since nothing drives them then, and an interrupt would go
//! unnoticed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;

use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use metalake_client::Error;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::{PyRecordBatch, PyRecordBatchReader};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::errors::to_python;
use crate::runtime;

/// Batches ahead of the reader: a statement waits past these, so a slow reader does not fill memory.
const AHEAD: usize = 2;
/// How often a wait looks for an interrupt.
const PATIENCE: Duration = Duration::from_millis(100);

type Batch = Result<RecordBatch, Error>;

/// The statement at work. Dropping it ends it.
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
    fn start(mut stream: SendableRecordBatchStream) -> Self {
        let (sender, batches) = channel();
        let room = Arc::new(Semaphore::new(AHEAD));
        let held = room.clone();
        let task = runtime::spawn(async move {
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

/// A failure of the stream, kept for when whoever read it through Arrow comes back.
type Failure = Arc<Mutex<Option<Error>>>;

/// The stream as Arrow's C interface reads it. It may run under a reader that holds the GIL, as Polars does.
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

enum State {
    Ready(SendableRecordBatchStream),
    Reading(Pump),
    Read,
}

/// The rows of a statement.
#[pyclass(name = "Result", module = "metalake", frozen)]
pub struct QueryResult {
    schema: SchemaRef,
    state: Mutex<State>,
    failure: Failure,
    /// Set while one thread reads, so that another is refused and not left waiting for a lock with the GIL in hand.
    reading: AtomicBool,
}

impl QueryResult {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            schema: stream.schema(),
            state: Mutex::new(State::Ready(stream)),
            failure: Failure::default(),
            reading: AtomicBool::new(false),
        }
    }

    fn already_read(py: Python<'_>) -> PyErr {
        to_python(py, Error::failed("this result has already been read"))
    }

    fn state(&self, py: Python<'_>) -> PyResult<std::sync::MutexGuard<'_, State>> {
        match self.state.try_lock() {
            Ok(state) => Ok(state),
            Err(TryLockError::Poisoned(poisoned)) => Ok(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => Err(to_python(py, Error::failed("another thread is reading this result"))),
        }
    }
}

#[pymethods]
impl QueryResult {
    /// The names of the columns.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.schema.as_ref())?)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut state = self.state(py)?;
        let State::Ready(_) = &*state else {
            return Err(Self::already_read(py));
        };
        let State::Ready(stream) = std::mem::replace(&mut *state, State::Read) else {
            unreachable!("looked at above");
        };
        let reader = Reader {
            pump: Pump::start(stream),
            schema: self.schema.clone(),
            failure: self.failure.clone(),
        };
        let reader = PyRecordBatchReader::new(Box::new(reader));
        Bound::new(py, reader)?.call_method1("__arrow_c_stream__", (requested_schema,))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next batch, waiting with the GIL released. An interrupt or a failure ends the statement.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyBatch> {
        if self.reading.swap(true, Ordering::AcqRel) {
            return Err(to_python(py, Error::failed("another thread is reading this result")));
        }
        let next = self.next_batch(py);
        self.reading.store(false, Ordering::Release);
        next
    }
}

impl QueryResult {
    fn next_batch(&self, py: Python<'_>) -> PyResult<PyBatch> {
        let mut state = self.state(py)?;
        if let State::Ready(_) = &*state {
            let State::Ready(stream) = std::mem::replace(&mut *state, State::Read) else {
                unreachable!("looked at above");
            };
            *state = State::Reading(Pump::start(stream));
        }
        let State::Reading(pump) = &mut *state else {
            return Err(Self::already_read(py));
        };
        loop {
            match py.detach(|| pump.wait()) {
                Ok(Some(Ok(batch))) => return Ok(PyBatch { batch }),
                Ok(Some(Err(error))) => {
                    *state = State::Read;
                    return Err(to_python(py, error));
                }
                Ok(None) => {
                    *state = State::Read;
                    return Err(PyStopIteration::new_err(()));
                }
                Err(_) => {
                    if let Err(interrupted) = py.check_signals() {
                        *state = State::Read;
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
