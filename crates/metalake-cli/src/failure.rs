//! How a failure shows on a command line: one place for the exit codes.

use metalake_client::{Error, Kind};

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

impl From<Kind> for Exit {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::Failed => Exit::Failed,
            Kind::Usage => Exit::Usage,
            Kind::Conflict => Exit::Conflict,
            Kind::Unknown => Exit::Unknown,
        }
    }
}

/// Prints a failure on stderr, so that stdout holds only what was asked for.
pub fn report(error: &Error, format: Option<Format>) {
    match format {
        Some(Format::Json) => eprintln!(
            "{}",
            serde_json::json!({
                "error": error.kind.name(),
                "rule": error.rule,
                "code": error.code,
                "retryable": error.retryable,
                "message": error.message,
            })
        ),
        _ => eprintln!("metalake: {error}"),
    }
}
