//! Whether an object can still be used. A fork copies a `Lake` with the connections of its parent, and two processes on one
//! socket would corrupt what Postgres reads, so an object says so in another process and does not try. And what a `Lake`
//! gave out goes with it when it closes, which every copy of this sees.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use metalake_client::Error;
use pyo3::prelude::*;

use crate::errors::to_python;

#[derive(Debug, Clone)]
pub struct Owner {
    process: u32,
    closed: Arc<AtomicBool>,
}

impl Owner {
    /// This process, open.
    pub fn here() -> Self {
        Self {
            process: std::process::id(),
            closed: Arc::default(),
        }
    }

    /// Marks the lake closed, and answers whether it was open.
    pub fn close(&self) -> bool {
        !self.closed.swap(true, Ordering::AcqRel)
    }

    /// Refuses a call from a process other than the one that made `what`, or on a closed lake.
    pub fn check(&self, py: Python<'_>, what: &str) -> PyResult<()> {
        if self.process != std::process::id() {
            let message = format!("this {what} was opened in another process: connect again in this one");
            return Err(to_python(py, Error::usage(message)));
        }
        if self.closed.load(Ordering::Acquire) {
            let message = format!("this {what} belongs to a closed Lake: connect again");
            return Err(to_python(py, Error::usage(message)));
        }
        Ok(())
    }
}
