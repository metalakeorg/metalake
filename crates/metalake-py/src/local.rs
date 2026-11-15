//! How this process runs a local engine for `USING`: `metalake._engines.run_local`, the call a notebook's
//! `lake.iceberg.duckdb()` makes, its rows taken as Arrow through Arrow's C interface, with no copy.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use metalake_client::LocalFailure;
use pyo3::exceptions::PyImportError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;

/// One statement on a local engine. Called from a thread of the runtime, with the GIL taken for it.
pub fn run(engine: &str, setup: &[String], sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), LocalFailure> {
    Python::attach(|py| {
        let failed = |e: PyErr| {
            if e.is_instance_of::<PyImportError>(py) {
                LocalFailure::Missing(e.value(py).to_string())
            } else {
                LocalFailure::Failed(e.value(py).to_string())
            }
        };
        let module = py.import("metalake._engines").map_err(failed)?;
        let answer = module
            .getattr("run_local")
            .map_err(failed)?
            .call1((engine, setup.to_vec(), sql))
            .map_err(failed)?;
        let rows: AnyRecordBatch = answer.extract().map_err(|e: PyErr| failed(e))?;
        let reader = rows.into_reader().map_err(failed)?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .map_err(|e| LocalFailure::Failed(e.to_string()))?;
        Ok((schema, batches))
    })
}
