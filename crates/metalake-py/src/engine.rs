//! `lake.add_engine`, `lake.engine`, `lake.engines` and `lake.remove_engine`: engines elsewhere the lake's faces send
//! statements to, named in the lake's `metalake.toml`. Naming one reaches nothing; a secret is never written, only the
//! variable of the environment that holds it.

use std::sync::Arc;

use metalake_client::{EngineConfig, Lake};
use pyo3::prelude::*;

use crate::owner::Owner;

/// An engine the lake names: where it answers and who asks.
#[pyclass(name = "Engine", module = "metalake", frozen)]
pub struct PyEngine {
    #[allow(dead_code)]
    owner: Owner,
    #[allow(dead_code)]
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
