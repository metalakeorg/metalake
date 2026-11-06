//! The one runtime of a process, and the one way Python waits for it. A connection to Postgres lives on a task of the
//! runtime that opened it, so a program that makes many calls needs them all on the same one. A fork copies a runtime
//! without its threads, so a process makes its own, and leaves the one it inherited: dropping it would wait for those
//! threads, which are not there.
//!
//! The wait is a plain channel. tokio's timers would not fire while a statement keeps every worker busy, since nothing
//! drives them then, and an interrupt would go unnoticed.

use std::future::Future;
use std::sync::Mutex;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::Duration;

use metalake_client::Error;
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::task::AbortHandle;

use crate::errors::to_python;

/// How often a wait looks for an interrupt.
pub const PATIENCE: Duration = Duration::from_millis(100);

/// The runtime, and the process it is for.
static RUNTIME: Mutex<Option<(u32, &'static Runtime)>> = Mutex::new(None);

fn runtime() -> &'static Runtime {
    let mut current = RUNTIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let process = std::process::id();
    match *current {
        Some((owner, runtime)) if owner == process => runtime,
        _ => {
            let built = Builder::new_multi_thread().enable_all().build();
            let runtime: &'static Runtime = Box::leak(Box::new(built.expect("a runtime starts")));
            *current = Some((process, runtime));
            runtime
        }
    }
}

/// Runs `future` on the runtime, and answers how to end it.
pub fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    runtime().spawn(future).abort_handle()
}

/// Runs `future` to its end with the GIL released, so that the rest of a Python program goes on. An interrupt ends it. A
/// write ended before its commit changed nothing, and one ended during it may or may not have taken effect.
pub fn wait<F>(py: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (sender, mut receiver) = channel();
    let task = spawn(async move {
        let _ = sender.send(future.await);
    });
    loop {
        let waited = &mut receiver;
        match py.detach(move || waited.recv_timeout(PATIENCE)) {
            Ok(answer) => return Ok(answer),
            Err(RecvTimeoutError::Timeout) => {
                if let Err(interrupted) = py.check_signals() {
                    task.abort();
                    return Err(interrupted);
                }
            }
            // A panic ends its task, and an `except Exception` must see it, which a panic that reached Python would pass.
            Err(RecvTimeoutError::Disconnected) => {
                return Err(to_python(
                    py,
                    Error::failed("a task of metalake panicked: this is a bug"),
                ));
            }
        }
    }
}
