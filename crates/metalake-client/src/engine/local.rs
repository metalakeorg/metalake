//! Engines that run on this machine, as a library: DuckDB. They are Python's, so they run where `metalake` was
//! installed with pip, whose extension says how when it loads ([`set_local_runner`]): the same call a notebook makes,
//! `lake.iceberg.duckdb()`, in a process of its own, so that an engine that fails as native code fails ends that
//! process and not this one ([`LocalFailure::Stopped`]). A `metalake` built alone has no Python, and says so.
//!
//! The lake reaches the engine as a notebook's does: its Iceberg server on this machine, started if it is not, and the
//! engine's profile, which attaches the lake as `lake`; then `USE lake.default`, so that `demo.t` is the lake's `demo.t`.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;

use super::Using;
use crate::iceberg::Engine;
use crate::{Error, Lake, Prepared, Result};

/// The engines that run on this machine, each statement in a worker process.
pub const LOCAL: [&str; 1] = ["duckdb"];

/// Why a local engine did not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalFailure {
    /// The engine is not installed in this Python: what to install says how.
    Missing(String),
    /// The engine refused the statement, in its words.
    Failed(String),
    /// The process the engine runs in ended while it ran the statement: how it ended, as [`how_it_ended`] says.
    Stopped(String),
}

/// How a process ended, in words: what an engine's native code does when it fails is end its process, by a code
/// Windows gives or a signal Unix sends.
pub fn how_it_ended(status: std::process::ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            let name = match signal {
                4 => "illegal instruction",
                6 => "aborted",
                7 | 10 => "bus error",
                9 => "killed",
                11 => "segmentation fault",
                _ => "signal",
            };
            return format!("{name} (signal {signal})");
        }
    }
    match status.code() {
        Some(code) => {
            let name = match code as u32 {
                0xC000_001D => "illegal instruction",
                0xC000_0005 => "access violation",
                0xC000_00FD => "stack overflow",
                0xC000_0409 => "stack buffer overrun",
                _ if code < 0 => "fault",
                _ => "exited",
            };
            if code < 0 {
                format!("{name} (0x{:08x})", code as u32)
            } else {
                format!("{name} (code {code})")
            }
        }
        None => "ended".into(),
    }
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
        LocalFailure::Stopped(how) => Error::failed(format!(
            "{engine} stopped: {how}; the next USING {engine} starts it again",
            engine = using.engine
        )),
    })?;
    Prepared::from_batches(schema, batches)
}

#[cfg(test)]
mod tests {
    use super::how_it_ended;

    #[cfg(windows)]
    #[test]
    fn a_process_ended_by_windows_is_said_by_its_code() {
        use std::os::windows::process::ExitStatusExt;
        let ended = |code: u32| how_it_ended(std::process::ExitStatus::from_raw(code));
        assert_eq!(ended(0xC000_001D), "illegal instruction (0xc000001d)");
        assert_eq!(ended(0xC000_0005), "access violation (0xc0000005)");
        assert_eq!(ended(1), "exited (code 1)");
    }

    #[cfg(unix)]
    #[test]
    fn a_process_ended_by_a_signal_is_said_by_it() {
        use std::os::unix::process::ExitStatusExt;
        let ended = |raw: i32| how_it_ended(std::process::ExitStatus::from_raw(raw));
        assert_eq!(ended(4), "illegal instruction (signal 4)");
        assert_eq!(ended(11), "segmentation fault (signal 11)");
        assert_eq!(ended(1 << 8), "exited (code 1)");
    }
}
