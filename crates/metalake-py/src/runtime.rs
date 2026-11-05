//! The one runtime of a process. A connection to Postgres lives on a task of the runtime that opened it, so a program
//! that makes many calls needs them all on the same one.

use std::future::Future;
use std::sync::OnceLock;

use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::task::AbortHandle;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a runtime starts")
    })
}

/// Waits for `future` with the GIL released, so that the rest of a Python program goes on.
pub fn block<F>(py: Python<'_>, future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    py.detach(|| runtime().block_on(future))
}

/// Runs `future` on the runtime, and answers how to end it.
pub fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    runtime().spawn(future).abort_handle()
}
