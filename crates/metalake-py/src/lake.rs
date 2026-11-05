//! `connect`, `init` and the lake they answer.

use std::path::PathBuf;

use metalake_client::{Lake, Session};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::runtime::block;

/// A lake, reached, with the session its statements run in.
#[pyclass(name = "Lake", module = "metalake", frozen)]
pub struct PyLake {
    lake: Lake,
    #[allow(dead_code, reason = "statements come with `sql`")]
    session: Session,
}

impl PyLake {
    fn new(py: Python<'_>, lake: Lake) -> PyResult<Self> {
        let session = block(py, Session::new(&lake)).map_err(|e| to_python(py, e))?;
        Ok(Self { lake, session })
    }
}

#[pymethods]
impl PyLake {
    fn __repr__(&self) -> String {
        match (self.lake.postgres(), self.lake.local()) {
            (Some(postgres), _) => format!("Lake(postgres={postgres:?})"),
            (None, Some(local)) => format!("Lake(local={:?})", local.display().to_string()),
            (None, None) => "Lake()".to_string(),
        }
    }
}

fn text(path: &Option<PathBuf>) -> Option<String> {
    path.as_ref().map(|p| p.to_string_lossy().into_owned())
}

/// Reaches a lake: a directory, a Postgres, or, naming none, what `metalake` would find from here.
#[pyfunction]
#[pyo3(signature = (lake=None, *, schema=None))]
pub fn connect(py: Python<'_>, lake: Option<PathBuf>, schema: Option<&str>) -> PyResult<PyLake> {
    let target = text(&lake);
    let reached = block(py, Lake::connect(target.as_deref(), schema)).map_err(|e| to_python(py, e))?;
    PyLake::new(py, reached)
}

/// Makes a lake: in a directory, with a Postgres of its own, or on a Postgres that exists.
#[pyfunction]
#[pyo3(signature = (catalog, warehouse=None, *, schema=None))]
pub fn init(py: Python<'_>, catalog: PathBuf, warehouse: Option<PathBuf>, schema: Option<&str>) -> PyResult<PyLake> {
    let catalog = catalog.to_string_lossy().into_owned();
    let warehouse = text(&warehouse);
    let made = block(py, Lake::create(&catalog, warehouse.as_deref(), schema, None, None));
    PyLake::new(py, made.map_err(|e| to_python(py, e))?)
}
