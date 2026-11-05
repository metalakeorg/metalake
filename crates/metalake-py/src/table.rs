//! A table of a lake, by its name. It holds no data and no point: each operation takes the table as it is then.

use std::sync::Arc;

use metalake_client::{Lake, TablePath};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::to_schema_pycapsule;

use crate::errors::to_python;
use crate::runtime::wait;

#[pyclass(name = "Table", module = "metalake", frozen)]
pub struct PyTable {
    lake: Arc<Lake>,
    path: TablePath,
}

impl PyTable {
    /// The table named, which must exist.
    pub fn open(py: Python<'_>, lake: Arc<Lake>, name: &str) -> PyResult<Self> {
        let path = TablePath::parse(name).map_err(|e| to_python(py, e))?;
        let (reached, wanted) = (lake.clone(), path.clone());
        wait(py, async move { reached.table(&wanted).await.map(|_| ()) })?.map_err(|e| to_python(py, e))?;
        Ok(Self { lake, path })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<datafusion::arrow::datatypes::SchemaRef> {
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

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        Ok(to_schema_pycapsule(py, self.schema(py)?.as_ref())?)
    }

    fn __repr__(&self) -> String {
        format!("Table({:?})", self.path.to_string())
    }
}
