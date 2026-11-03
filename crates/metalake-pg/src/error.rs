//! The one error of the SDK: a kind that says what to do, the rule the protocol named, and where it came from.

use std::fmt;

/// What went wrong, as far as the caller must act on it (ADR 0003 §3.4, §3.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The catalog's format, writer version or protocol is not the one this SDK speaks.
    Unsupported,
    /// Postgres refused or failed, and the protocol named no rule.
    Postgres,
}

/// An error of the SDK.
#[derive(Debug, thiserror::Error)]
pub struct Error {
    kind: ErrorKind,
    rule: Option<String>,
    message: String,
    #[source]
    source: Option<tokio_postgres::Error>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            rule: None,
            message: message.into(),
            source: None,
        }
    }

    /// Adds where a failure happened, such as the source that was being installed.
    pub(crate) fn context(mut self, what: &str) -> Self {
        self.message = format!("{what}: {}", self.message);
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The rule of the specification the protocol refused by (`CMT-7`, `DV-4`…), if it named one.
    pub fn rule(&self) -> Option<&str> {
        self.rule.as_deref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Some(rule) => write!(f, "{:?} ({rule}): {}", self.kind, self.message),
            None => write!(f, "{:?}: {}", self.kind, self.message),
        }
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(source: tokio_postgres::Error) -> Self {
        let message = match source.as_db_error() {
            Some(db) => format!("{} {}", db.code().code(), db.message()),
            None => source.to_string(),
        };
        Self {
            kind: ErrorKind::Postgres,
            rule: None,
            message,
            source: Some(source),
        }
    }
}
