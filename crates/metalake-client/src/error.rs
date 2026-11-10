//! Every way a verb fails, as what a person reads and what a program acts on. A face decides how each kind shows: an
//! exit code, an exception.

use std::fmt;
use std::io;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use metalake_pg::ErrorKind;

pub type Result<T> = std::result::Result<T, Error>;

/// What a failure means to whoever called. These do not change, since programs act on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Failed,
    Usage,
    /// A write that conflicts with another was refused; nothing changed. Writing again is the caller's choice.
    Conflict,
    /// The connection was lost, and a write that was sent may or may not have taken effect.
    Unknown,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Failed => "failed",
            Kind::Usage => "usage",
            Kind::Conflict => "conflict",
            Kind::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub kind: Kind,
    pub message: String,
    pub rule: Option<String>,
    pub code: Option<String>,
    pub retryable: bool,
}

impl Error {
    pub fn new(kind: Kind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            rule: None,
            code: None,
            retryable: false,
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(Kind::Failed, message)
    }

    pub fn usage(message: impl Into<String>) -> Self {
        Self::new(Kind::Usage, message)
    }

    /// A write that would conflict, by this rule.
    pub fn conflict(rule: Option<String>, message: impl Into<String>) -> Self {
        Self {
            rule,
            ..Self::new(Kind::Conflict, message)
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<&metalake::Error> for Error {
    fn from(e: &metalake::Error) -> Self {
        let mut error = Error::failed(e.to_string());
        if let metalake::Error::Protocol(p) = e {
            error.kind = match p.kind() {
                ErrorKind::Conflict | ErrorKind::Serialization => Kind::Conflict,
                ErrorKind::Unknown => Kind::Unknown,
                _ => Kind::Failed,
            };
            error.rule = p.rule().map(str::to_string);
            error.code = p.code().map(str::to_string);
            error.retryable = p.is_retryable();
        }
        error
    }
}

/// A table named wrongly is a wrong use, as any other argument.
impl From<metalake::InvalidPath> for Error {
    fn from(e: metalake::InvalidPath) -> Self {
        Error::usage(e.to_string())
    }
}

impl From<metalake::Error> for Error {
    fn from(e: metalake::Error) -> Self {
        Self::from(&e)
    }
}

impl From<DataFusionError> for Error {
    /// What DataFusion carries of the core's, wrapped as it may be, so that a statement fails as a verb does.
    fn from(e: DataFusionError) -> Self {
        if let DataFusionError::External(inner) = e.find_root()
            && let Some(core) = inner.downcast_ref::<metalake::Error>()
        {
            return core.into();
        }
        Error::failed(e.to_string())
    }
}

impl From<metalake_local::Error> for Error {
    fn from(e: metalake_local::Error) -> Self {
        Error::failed(e.to_string())
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::failed(e.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::failed(e.to_string())
    }
}
