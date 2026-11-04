//! Every way a verb fails, as what a person reads and what a program acts on: one place for the exit codes.

use std::fmt;
use std::io;

use datafusion::error::DataFusionError;
use metalake_pg::ErrorKind;

use crate::out::Format;

/// How the process ends. A program acts on these, so they do not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Failed = 1,
    Usage = 2,
    /// A write that conflicts with another was refused; nothing changed. Writing again is the caller's choice.
    Conflict = 3,
    /// The connection was lost, and a write that was sent may or may not have taken effect.
    Unknown = 4,
}

impl Exit {
    fn name(self) -> &'static str {
        match self {
            Exit::Failed => "failed",
            Exit::Usage => "usage",
            Exit::Conflict => "conflict",
            Exit::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
pub struct Failure {
    pub exit: Exit,
    message: String,
    rule: Option<String>,
    code: Option<String>,
    retryable: bool,
}

impl Failure {
    pub fn new(exit: Exit, message: impl Into<String>) -> Self {
        Self {
            exit,
            message: message.into(),
            rule: None,
            code: None,
            retryable: false,
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(Exit::Failed, message)
    }

    pub fn usage(message: impl Into<String>) -> Self {
        Self::new(Exit::Usage, message)
    }

    /// Prints the failure on stderr, so that stdout holds only what was asked for.
    pub fn report(&self, format: Option<Format>) {
        match format {
            Some(Format::Json) => eprintln!(
                "{}",
                serde_json::json!({
                    "error": self.exit.name(),
                    "rule": self.rule,
                    "code": self.code,
                    "retryable": self.retryable,
                    "message": self.message,
                })
            ),
            _ => eprintln!("metalake: {self}"),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<&metalake::Error> for Failure {
    fn from(e: &metalake::Error) -> Self {
        let mut failure = Failure::failed(e.to_string());
        if let metalake::Error::Protocol(p) = e {
            failure.exit = match p.kind() {
                ErrorKind::Conflict | ErrorKind::Serialization => Exit::Conflict,
                ErrorKind::Unknown => Exit::Unknown,
                _ => Exit::Failed,
            };
            failure.rule = p.rule().map(str::to_string);
            failure.code = p.code().map(str::to_string);
            failure.retryable = p.is_retryable();
        }
        failure
    }
}

impl From<metalake::Error> for Failure {
    fn from(e: metalake::Error) -> Self {
        Self::from(&e)
    }
}

impl From<DataFusionError> for Failure {
    /// What DataFusion carries of the core's, wrapped as it may be, so that a statement fails as a verb does.
    fn from(e: DataFusionError) -> Self {
        if let DataFusionError::External(inner) = e.find_root()
            && let Some(core) = inner.downcast_ref::<metalake::Error>()
        {
            return core.into();
        }
        Failure::failed(e.to_string())
    }
}

impl From<io::Error> for Failure {
    fn from(e: io::Error) -> Self {
        Failure::failed(e.to_string())
    }
}
