//! `lake.add_engine`, `lake.engine`, `lake.engines` and `lake.remove_engine`: engines elsewhere the lake's faces send
//! statements to, named in the lake's `metalake.toml`. Naming one reaches nothing; a secret is never written, only the
//! variable of the environment that holds it.

use std::sync::Arc;

use metalake_client::{EngineConfig, Lake};
use pyo3::prelude::*;

use crate::errors::to_python;
use crate::owner::Owner;
use crate::result::QueryResult;
use crate::runtime::wait;

/// An engine the lake names: where it answers and who asks.
#[pyclass(name = "Engine", module = "metalake", frozen)]
pub struct PyEngine {
    owner: Owner,
    lake: Arc<Lake>,
    name: String,
    config: EngineConfig,
}

impl PyEngine {
    pub fn new(owner: Owner, lake: Arc<Lake>, name: String, config: EngineConfig) -> Self {
        Self {
            owner,
            lake,
            name,
            config,
        }
    }
}

#[pymethods]
impl PyEngine {
    /// Runs one statement on the engine, in its own SQL, on `main`, as `USING <name>` sends it: `demo.t` is the lake's
    /// `demo.t`, and Spark is given the lake's catalogs first. Its rows come back as a `Result`, as `lake.sql` answers.
    fn sql(&self, py: Python<'_>, text: String) -> PyResult<QueryResult> {
        self.owner.check(py, "Lake")?;
        let lake = self.lake.clone();
        let name = self.name.clone();
        let answered = wait(
            py,
            async move { metalake_client::sql_on_engine(&lake, &name, &text).await },
        )?;
        let prepared = answered.map_err(|e| to_python(py, e))?;
        Ok(QueryResult::new(prepared, self.owner.clone()))
    }

    /// Whether a statement can run on it, each link apart: `engine`, this side reaches it and is let in; `lake`, it
    /// reaches the lake through the lake's Iceberg server. Each is whether it holds, and what was found.
    fn check(&self, py: Python<'_>) -> PyResult<std::collections::BTreeMap<String, (bool, String)>> {
        self.owner.check(py, "Lake")?;
        let remote = metalake_client::Remote::new(&self.name, &self.config).map_err(|e| to_python(py, e))?;
        let checked = wait(py, async move { remote.check().await })?;
        let link = |r: &Result<String, String>| match r {
            Ok(said) => (true, said.clone()),
            Err(said) => (false, said.clone()),
        };
        Ok([
            ("engine".to_string(), link(&checked.engine)),
            ("lake".to_string(), link(&checked.lake)),
        ]
        .into())
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn r#type(&self) -> &str {
        self.config.kind.name()
    }

    #[getter]
    fn url(&self) -> &str {
        &self.config.url
    }

    #[getter]
    fn user(&self) -> Option<&str> {
        self.config.user.as_deref()
    }

    /// How it signs in, never the secret: `token from $TRINO_TOKEN`.
    #[getter]
    fn auth(&self) -> String {
        self.config.auth()
    }

    /// The lake's catalog in the engine.
    #[getter]
    fn catalog(&self) -> &str {
        self.config.catalog()
    }

    #[getter]
    fn lake_url(&self) -> Option<&str> {
        self.config.lake_url.as_deref()
    }

    #[getter]
    fn mode(&self) -> Option<&str> {
        self.config.mode.map(|m| m.name())
    }

    fn __repr__(&self) -> String {
        format!(
            "Engine({}, type={}, url={}, auth={})",
            self.name,
            self.config.kind.name(),
            self.config.url,
            self.config.auth()
        )
    }
}
