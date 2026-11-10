//! `lake.create_pipeline`, `lake.pipeline` and `lake.pipelines`: a continuous pipeline, a query that Arroyo keeps running
//! over each commit of what it reads. A `Pipeline` says where it was when last asked, and `refresh` asks again;
//! `suspend`, `resume` and `drop` are the client's calls, as every face makes them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use metalake_client::{Lake, Pipeline, PipelineState, state_name};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::owner::Owner;
use crate::runtime::wait;

/// What is asked of a pipeline.
enum Call {
    Refresh,
    Suspend,
    Resume,
}

/// A continuous pipeline, as it was when last asked.
#[pyclass(name = "Pipeline", module = "metalake", frozen)]
pub struct PyPipeline {
    owner: Owner,
    lake: Arc<Lake>,
    seen: Mutex<Pipeline>,
}

impl PyPipeline {
    pub fn new(owner: Owner, lake: Arc<Lake>, pipeline: Pipeline) -> Self {
        Self {
            owner,
            lake,
            seen: Mutex::new(pipeline),
        }
    }

    fn seen<T>(&self, read: impl FnOnce(&Pipeline) -> T) -> T {
        read(&self.seen.lock().expect("a pipeline is replaced whole"))
    }

    fn name(&self) -> String {
        self.seen(|p| p.name.clone())
    }

    /// Runs one of the client's calls on it, and keeps where it is after.
    fn call(&self, py: Python<'_>, call: Call) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let name = self.name();
        let now = wait(py, async move {
            match call {
                Call::Refresh => lake.pipeline(&name).await,
                Call::Suspend => lake.suspend_pipeline(&name).await,
                Call::Resume => lake.resume_pipeline(&name).await,
            }
        })?
        .map_err(|e| to_python(py, e))?;
        *self.seen.lock().expect("a pipeline is replaced whole") = now;
        Ok(())
    }
}

#[pymethods]
impl PyPipeline {
    #[getter(name)]
    fn get_name(&self) -> String {
        self.name()
    }

    #[getter]
    fn query(&self) -> String {
        self.seen(|p| p.query.clone())
    }

    #[getter]
    fn parallelism(&self) -> u32 {
        self.seen(|p| p.parallelism)
    }

    /// `running`, `suspended`, `failed`, or `running on <host>`, when another machine runs it.
    #[getter]
    fn state(&self) -> String {
        self.seen(|p| state_name(&p.state))
    }

    /// When it failed, its log's last lines.
    #[getter]
    fn log(&self) -> Option<String> {
        self.seen(|p| match &p.state {
            PipelineState::Failed { log } => Some(log.clone()),
            _ => None,
        })
    }

    #[getter]
    fn reads(&self) -> Vec<String> {
        self.seen(|p| p.reads.iter().map(ToString::to_string).collect())
    }

    #[getter]
    fn writes(&self) -> Vec<String> {
        self.seen(|p| p.writes.iter().map(ToString::to_string).collect())
    }

    /// Each table it writes, with the last commit it holds.
    #[getter]
    fn reached(&self) -> BTreeMap<String, Option<i64>> {
        self.seen(|p| p.reached.iter().map(|(path, last)| (path.to_string(), *last)).collect())
    }

    #[getter]
    fn created_at(&self) -> SystemTime {
        self.seen(|p| p.created_at)
    }

    /// Asks where it is now.
    fn refresh(&self, py: Python<'_>) -> PyResult<()> {
        self.call(py, Call::Refresh)
    }

    /// Stops it with a checkpoint, to resume from it.
    fn suspend(&self, py: Python<'_>) -> PyResult<()> {
        self.call(py, Call::Suspend)
    }

    /// Runs it again from its last checkpoint: what its sources committed meanwhile comes then.
    fn resume(&self, py: Python<'_>) -> PyResult<()> {
        self.call(py, Call::Resume)
    }

    /// Stops it and forgets it. What it wrote stays in its tables.
    fn drop(&self, py: Python<'_>) -> PyResult<()> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let name = self.name();
        wait(py, async move { lake.drop_pipeline(&name).await })?.map_err(|e| to_python(py, e))
    }

    fn __repr__(&self) -> String {
        self.seen(|p| format!("Pipeline({:?}, {})", p.name, state_name(&p.state)))
    }
}
