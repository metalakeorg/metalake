//! Which process an object belongs to. A fork copies a `Lake` with the connections of its parent, and two processes on one
//! socket would corrupt what Postgres reads, so an object says so in another process and does not try.

use metalake_client::Error;
use pyo3::prelude::*;

use crate::errors::to_python;

#[derive(Debug, Clone, Copy)]
pub struct Owner(u32);

impl Owner {
    /// This process.
    pub fn here() -> Self {
        Self(std::process::id())
    }

    /// Refuses a call from a process other than the one that made `what`.
    pub fn check(self, py: Python<'_>, what: &str) -> PyResult<()> {
        if self.0 == std::process::id() {
            return Ok(());
        }
        let message = format!("this {what} was opened in another process: connect again in this one");
        Err(to_python(py, Error::usage(message)))
    }
}
