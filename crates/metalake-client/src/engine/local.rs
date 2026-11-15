//! Engines that run in this process, as a library: DuckDB. They are Python's, so they run where `metalake` was
//! installed with pip, whose extension says how when it loads ([`set_local_runner`]): the same call a notebook makes,
//! `lake.iceberg.duckdb()`, and nothing else. A `metalake` built alone has no Python, and says so.
//!
//! The lake reaches the engine as a notebook's does: its Iceberg server on this machine, started if it is not, and the
//! engine's profile, which attaches the lake as `lake`; then `USE lake.default`, so that `demo.t` is the lake's `demo.t`.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;

use super::Using;
use crate::iceberg::Engine;
use crate::{Error, Lake, Prepared, Result};

/// The engines that run in this process.
pub const LOCAL: [&str; 1] = ["duckdb"];

/// Why a local engine did not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalFailure {
    /// The engine is not installed in this Python: what to install says how.
    Missing(String),
    /// The engine refused the statement, in its words.
    Failed(String),
}

/// Runs one statement on a local engine: the engine's name, the statements that connect it to the lake (run once per
/// connection), and the statement. It answers the statement's rows.
pub type LocalRunner = Arc<
    dyn Fn(&str, &[String], &str) -> std::result::Result<(SchemaRef, Vec<RecordBatch>), LocalFailure> + Send + Sync,
>;

static RUNNER: OnceLock<LocalRunner> = OnceLock::new();

/// Says how this process runs a local engine, once; later calls change nothing.
pub fn set_local_runner(runner: LocalRunner) {
    let _ = RUNNER.set(runner);
}

/// One statement `USING` a local engine, on `lake`, the session reading `on`.
pub(crate) async fn run_local(lake: &Lake, using: &Using) -> Result<Prepared> {
    let runner = RUNNER.get().cloned().ok_or_else(|| {
        Error::usage(format!(
            "USING {} runs in the Python metalake is installed in: pip install \"metalake[{}]\", and run it from there",
            using.engine, using.engine
        ))
    })?;
    let profile = lake.iceberg().profile(Engine::DuckDb, crate::CATALOG).await?;
    let mut setup = profile.statements();
    // `USE lake` alone means its schema `main`, which a lake has none of: `default`, which every lake has, so that a
    // bare `t` is the lake's `default.t`, and `demo.t` its schema `demo`'s.
    setup.push(format!("USE {}.default", crate::CATALOG));
    let (engine, sql) = (using.engine.clone(), using.sql.clone());
    let answered = tokio::task::spawn_blocking(move || runner(&engine, &setup, &sql))
        .await
        .map_err(|e| Error::failed(format!("USING {}: {e}", using.engine)))?;
    let (schema, batches) = answered.map_err(|failure| match failure {
        LocalFailure::Missing(said) => Error::usage(said),
        LocalFailure::Failed(said) => Error::failed(format!("{}: {said}", using.engine)),
    })?;
    Prepared::from_batches(schema, batches)
}
