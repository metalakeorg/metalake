//! How this process runs a local engine for `USING`: in a worker, `python -m metalake._worker` with this process's
//! Python, which makes the call a notebook's `lake.iceberg.duckdb()` makes and answers the rows as an Arrow IPC stream.
//!
//! An engine is native code, and native code that fails ends the process it runs in, which no one can catch: in the
//! worker, it ends the worker alone. The statement it ran fails, saying how the worker ended, and the next starts
//! another. One worker serves this whole process, a statement at a time, as a DuckDB connection takes one.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use metalake_client::{LocalFailure, how_it_ended};

/// The module the worker runs, unless `METALAKE_LOCAL_WORKER` names another (as a test does, for one that fails).
const WORKER: &str = "metalake._worker";
const WORKER_VAR: &str = "METALAKE_LOCAL_WORKER";

static PYTHON: OnceLock<OsString> = OnceLock::new();
static RUNNING: Mutex<Option<Worker>> = Mutex::new(None);

/// The Python a worker runs in: this process's own, said once as the extension loads.
pub fn set_python(python: OsString) {
    let _ = PYTHON.set(python);
}

struct Worker {
    module: String,
    child: Child,
    requests: ChildStdin,
    answers: BufReader<ChildStdout>,
}

impl Worker {
    fn start(module: &str) -> Result<Worker, LocalFailure> {
        let python = PYTHON
            .get()
            .ok_or_else(|| LocalFailure::Failed("no Python to run the engine in".into()))?;
        let mut child = Command::new(python)
            .args(["-m", module])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| LocalFailure::Failed(format!("cannot start the engine's process: {e}")))?;
        let requests = child.stdin.take().expect("piped");
        let answers = BufReader::new(child.stdout.take().expect("piped"));
        Ok(Worker {
            module: module.to_string(),
            child,
            requests,
            answers,
        })
    }

    /// One statement on the worker. An error of the pipe is the worker gone: how it ended says why.
    fn run(&mut self, engine: &str, setup: &[String], sql: &str) -> Result<Answer, std::io::Error> {
        let request = serde_json::json!({ "engine": engine, "setup": setup, "sql": sql });
        self.requests.write_all(format!("{request}\n").as_bytes())?;
        self.requests.flush()?;
        let mut header = String::new();
        if self.answers.read_line(&mut header)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let header: serde_json::Value =
            serde_json::from_str(&header).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(kind) = header.get("error").and_then(|k| k.as_str()) {
            let message = header
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or_default()
                .to_string();
            return Ok(Answer::Refused(if kind == "missing" {
                LocalFailure::Missing(message)
            } else {
                LocalFailure::Failed(message)
            }));
        }
        let reader = StreamReader::try_new(&mut self.answers, None).map_err(std::io::Error::other)?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .map_err(std::io::Error::other)?;
        Ok(Answer::Rows(schema, batches))
    }

    /// How the worker ended, once it is gone: waited for, or ended here if it is still there with its pipe broken.
    fn ended(mut self) -> String {
        drop(self.requests);
        for _ in 0..50 {
            if let Ok(Some(status)) = self.child.try_wait() {
                return how_it_ended(status);
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let _ = self.child.kill();
        match self.child.wait() {
            Ok(status) => how_it_ended(status),
            Err(e) => e.to_string(),
        }
    }
}

enum Answer {
    Rows(SchemaRef, Vec<RecordBatch>),
    Refused(LocalFailure),
}

/// One statement on a local engine, in the worker: started if there is none, or none of the module asked for.
pub fn run(engine: &str, setup: &[String], sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), LocalFailure> {
    let module = std::env::var(WORKER_VAR).unwrap_or_else(|_| WORKER.to_string());
    let mut running = RUNNING.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if running.as_ref().is_some_and(|worker| worker.module != module) {
        let _ = running.take().map(Worker::ended);
    }
    if running.is_none() {
        *running = Some(Worker::start(&module)?);
    }
    let worker = running.as_mut().expect("started");
    match worker.run(engine, setup, sql) {
        Ok(Answer::Rows(schema, batches)) => Ok((schema, batches)),
        Ok(Answer::Refused(failure)) => Err(failure),
        Err(_) => Err(LocalFailure::Stopped(running.take().expect("running").ended())),
    }
}
