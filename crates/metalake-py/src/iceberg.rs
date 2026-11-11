//! `lake.iceberg`: when Iceberg clients find each table's metadata written (REST-9). Every table is served to them and
//! written when one loads it; a table set `on_commit` on a ref is written by the Iceberg service after each commit.

use std::collections::BTreeMap;
use std::sync::Arc;

use metalake_client::{Engine, IcebergTable, Lake, Profile};
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
    #[pyo3(signature = (tables, *, on = "main"))]
    fn on_commit(&self, py: Python<'_>, tables: Vec<String>, on: &str) -> PyResult<Option<i64>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let on = on.to_string();
        let set = wait(py, async move {
            let names: Vec<&str> = tables.iter().map(String::as_str).collect();
            lake.iceberg().on_commit(&on, &names).await
        })?;
        set.map_err(|e| to_python(py, e))
    }

    /// Has it written when a client loads them, as every table has unless set, and answers the commit, if any.
    #[pyo3(signature = (tables, *, on = "main"))]
    fn on_load(&self, py: Python<'_>, tables: Vec<String>, on: &str) -> PyResult<Option<i64>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let on = on.to_string();
        let set = wait(py, async move {
            let names: Vec<&str> = tables.iter().map(String::as_str).collect();
            lake.iceberg().on_load(&on, &names).await
        })?;
        set.map_err(|e| to_python(py, e))
    }

    /// The tables set on-commit on `on`, and how far their Iceberg metadata is behind.
    #[pyo3(signature = (*, on = "main"))]
    fn status(&self, py: Python<'_>, on: &str) -> PyResult<Vec<PyIcebergTable>> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let on = on.to_string();
        let found = wait(py, async move { lake.iceberg().status(&on).await })?;
        let found = found.map_err(|e| to_python(py, e))?;
        Ok(found.into_iter().map(PyIcebergTable::from).collect())
    }

    /// The URL engines connect to: the lake's Iceberg REST server on this machine, started if it is not, which runs on
    /// after this process until stopped.
    #[getter]
    fn uri(&self, py: Python<'_>) -> PyResult<String> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let uri = wait(py, async move { lake.iceberg().uri().await })?;
        uri.map_err(|e| to_python(py, e))
    }

    /// What `engine` (`pyiceberg`, `duckdb`, `spark`, `flink`, `trino`) needs to connect to the lake, its catalog named
    /// `name`: what fixes it as it starts and what sets it once running, in one dict, by the engine's own keys. For what
    /// MetaLake does not connect itself: a Spark Connect server, Trino, a file of one's own. Starts the lake's server.
    #[pyo3(signature = (engine, *, name = "lake"))]
    fn properties(&self, py: Python<'_>, engine: &str, name: &str) -> PyResult<BTreeMap<String, String>> {
        let profile = self.profile(py, engine, name)?;
        Ok(profile.startup.into_iter().chain(profile.properties).collect())
    }

    /// Everything `engine` needs to connect to the lake, its catalog named `name`, apart: what fixes it as it starts,
    /// what sets it once running, the statements that make the catalog, the libraries it loads. Starts the lake's
    /// server, and fetches the jars Flink is given as files.
    #[pyo3(signature = (engine, *, name = "lake"))]
    fn profile(&self, py: Python<'_>, engine: &str, name: &str) -> PyResult<PyProfile> {
        self.owner.check(py, "Lake")?;
        let engine: Engine = engine.parse().map_err(|e| to_python(py, e))?;
        let lake = self.lake.clone();
        let name = name.to_string();
        let profile = wait(py, async move { lake.iceberg().profile(engine, &name).await })?;
        Ok(PyProfile::from(profile.map_err(|e| to_python(py, e))?))
    }

    /// A PyIceberg catalog of the lake, named `name`. Polars' `scan_iceberg` and Daft take it as it is.
    #[pyo3(signature = (*, name = "lake"))]
    fn pyiceberg<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        let profile = self.profile(py, "pyiceberg", name)?;
        engines(py)?.getattr("pyiceberg")?.call1((profile,))
    }

    /// A DuckDB connection with the lake attached as `name`: the one given, or a new one.
    #[pyo3(signature = (connection = None, *, name = "lake"))]
    fn duckdb<'py>(
        &self,
        py: Python<'py>,
        connection: Option<Bound<'py, PyAny>>,
        name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let profile = self.profile(py, "duckdb", name)?;
        engines(py)?.getattr("duckdb")?.call1((profile, connection))
    }

    /// A SparkSession connected to the lake as `name`: the one given, else the active one, else a local one started
    /// with Iceberg. A session that runs already gets only what can be set once running, the catalog; one started
    /// without Iceberg is refused, with what it needs at start.
    #[pyo3(signature = (session = None, *, name = "lake"))]
    fn spark<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
        name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let profile = self.profile(py, "spark", name)?;
        engines(py)?.getattr("spark")?.call1((profile, session))
    }

    /// A Flink TableEnvironment with the lake's catalog as `name`: the one given, or a new one, in batch unless
    /// `streaming`. Iceberg's and Hadoop's jars are added to the ones it has, fetched once into `~/.metalake/jars`.
    #[pyo3(signature = (environment = None, *, name = "lake", streaming = false))]
    fn flink<'py>(
        &self,
        py: Python<'py>,
        environment: Option<Bound<'py, PyAny>>,
        name: &str,
        streaming: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let profile = self.profile(py, "flink", name)?;
        engines(py)?.getattr("flink")?.call1((profile, environment, streaming))
    }

    /// Stops the lake's server on this machine, if one runs, and says whether one did. `metalake.stop` stops it too.
    fn stop(&self, py: Python<'_>) -> PyResult<bool> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let stopped = wait(py, async move { lake.iceberg().stop().await })?;
        stopped.map_err(|e| to_python(py, e))
    }

    fn __repr__(&self) -> &'static str {
        "Iceberg()"
    }
}

/// Where each engine is connected, in Python: the engines are Python's own libraries, imported when one is asked for.
fn engines(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    py.import("metalake._engines")
}

/// What an engine needs to connect to the lake, from `lake.iceberg.profile`; `str()` of it is the engine's own form,
/// as `metalake iceberg connect` prints it.
#[pyclass(name = "Profile", module = "metalake", frozen)]
pub struct PyProfile {
    #[pyo3(get)]
    engine: String,
    #[pyo3(get)]
    name: String,
    /// The lake's Iceberg REST server.
    #[pyo3(get)]
    uri: String,
    /// What can be set on an engine already running.
    #[pyo3(get)]
    properties: BTreeMap<String, String>,
    /// What is fixed only as an engine starts.
    #[pyo3(get)]
    startup: BTreeMap<String, String>,
    /// For DuckDB and Flink, the statements that make the catalog, in order.
    #[pyo3(get)]
    statements: Vec<String>,
    /// The libraries it loads, by their Maven coordinates.
    #[pyo3(get)]
    artifacts: Vec<String>,
    rendered: String,
}

impl From<Profile> for PyProfile {
    fn from(profile: Profile) -> Self {
        Self {
            engine: profile.engine.to_string(),
            uri: profile.target.uri.clone(),
            statements: profile.statements(),
            artifacts: profile.artifacts.iter().map(|a| a.coordinate()).collect(),
            rendered: profile.to_string(),
            name: profile.name,
            properties: profile.properties,
            startup: profile.startup,
        }
    }
}

#[pymethods]
impl PyProfile {
    fn __str__(&self) -> &str {
        &self.rendered
    }

    fn __repr__(&self) -> String {
        format!("Profile({:?}, name={:?}, uri={:?})", self.engine, self.name, self.uri)
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
