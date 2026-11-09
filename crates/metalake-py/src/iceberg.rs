//! `lake.iceberg`: when Iceberg clients find each table's metadata written (REST-9). Every table is served to them and
//! written when one loads it; a table set `on_commit` on a ref is written by the Iceberg service after each commit.

use std::sync::Arc;

use metalake_client::{IcebergTable, Lake};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::owner::Owner;
use crate::runtime::wait;

/// The lake's Iceberg face, from `lake.iceberg`.
#[pyclass(name = "Iceberg", module = "metalake", frozen)]
pub struct PyIceberg {
    owner: Owner,
    lake: Arc<Lake>,
}

impl PyIceberg {
    pub fn new(owner: Owner, lake: Arc<Lake>) -> Self {
        Self { owner, lake }
    }
}

#[pymethods]
impl PyIceberg {
    /// Has the Iceberg service write the metadata of `tables`, each `namespace.table`, after each of their commits on
    /// `on`, in one commit, and answers it; `None` if every one was so already. A table `on` does not see refuses all.
    #[pyo3(signature = (tables, *, on = "main".to_string()))]
    fn on_commit(&self, py: Python<'_>, tables: Vec<String>, on: String) -> PyResult<Option<i64>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let set = wait(py, async move {
            let names: Vec<&str> = tables.iter().map(String::as_str).collect();
            lake.iceberg().on_commit(&on, &names).await
        })?;
        set.map_err(|e| to_python(py, e))
    }

    /// Has it written when a client loads them, as every table has unless set, and answers the commit, if any.
    #[pyo3(signature = (tables, *, on = "main".to_string()))]
    fn on_load(&self, py: Python<'_>, tables: Vec<String>, on: String) -> PyResult<Option<i64>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let set = wait(py, async move {
            let names: Vec<&str> = tables.iter().map(String::as_str).collect();
            lake.iceberg().on_load(&on, &names).await
        })?;
        set.map_err(|e| to_python(py, e))
    }

    /// The tables set on-commit on `on`, and how far their Iceberg metadata is behind.
    #[pyo3(signature = (*, on = "main".to_string()))]
    fn status(&self, py: Python<'_>, on: String) -> PyResult<Vec<PyIcebergTable>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let found = wait(py, async move { lake.iceberg().status(&on).await })?;
        let found = found.map_err(|e| to_python(py, e))?;
        Ok(found.into_iter().map(PyIcebergTable::from).collect())
    }

    fn __repr__(&self) -> &'static str {
        "Iceberg()"
    }
}

/// Where the Iceberg metadata of a table set on-commit stands: what its latest metadata file serves, and how many of
/// its commits it does not yet.
#[pyclass(name = "IcebergTable", module = "metalake", frozen, get_all)]
pub struct PyIcebergTable {
    table: String,
    head: Option<i64>,
    written: Option<i64>,
    behind: i64,
    since: Option<String>,
    metadata: Option<String>,
}

impl From<IcebergTable> for PyIcebergTable {
    fn from(t: IcebergTable) -> Self {
        Self {
            table: t.table,
            head: t.head,
            written: t.written,
            behind: t.behind,
            since: t.since,
            metadata: t.metadata,
        }
    }
}

#[pymethods]
impl PyIcebergTable {
    fn __repr__(&self) -> String {
        format!(
            "IcebergTable({:?}, head={:?}, written={:?}, behind={})",
            self.table, self.head, self.written, self.behind
        )
    }
}
