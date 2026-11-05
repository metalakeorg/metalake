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

/// What a long-lived program needs to hear of a server that went away, which a command never meets: it brings one up.
fn said(error: &Error) -> String {
    match error.message.contains("error connecting to server") {
        true => format!("{}: if its server stopped, `connect` again", error.message),
        false => error.message.clone(),
    }
}

pub fn to_python(py: Python<'_>, error: Error) -> PyErr {
    let raised = (|| -> PyResult<PyErr> {
        let module = py.import("metalake.errors")?;
        let arguments = (said(&error), &error.rule, &error.code, error.retryable);
        let exception = module.getattr(class(error.kind))?.call1(arguments)?;
        Ok(PyErr::from_value(exception))
    })();
    raised.unwrap_or_else(|e| PyRuntimeError::new_err(format!("{error} ({e})")))
}
