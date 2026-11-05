//! A failure as the exception of its kind, one for one with the exit codes of the command line. The classes are in
//! `metalake/errors.py`, since one of them has two bases.

use metalake_client::{Error, Kind};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

fn class(kind: Kind) -> &'static str {
    match kind {
        Kind::Failed => "MetaLakeError",
        Kind::Usage => "UsageError",
        Kind::Conflict => "ConflictError",
        Kind::Unknown => "UnknownOutcomeError",
    }
}

pub fn to_python(py: Python<'_>, error: Error) -> PyErr {
    let raised = (|| -> PyResult<PyErr> {
        let module = py.import("metalake.errors")?;
        let arguments = (&error.message, &error.rule, &error.code, error.retryable);
        let exception = module.getattr(class(error.kind))?.call1(arguments)?;
        Ok(PyErr::from_value(exception))
    })();
    raised.unwrap_or_else(|e| PyRuntimeError::new_err(format!("{error} ({e})")))
}
