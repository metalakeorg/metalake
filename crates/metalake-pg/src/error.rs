//! The one error of the SDK: a kind that says what to do, the rule the protocol named, and where it came from
//! (ADR 0003 §3.4, §3.6).

use std::fmt;

/// What went wrong, as far as the caller must act on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The protocol refused a write that conflicts with another (`40001`, CMT-7, DV-4, MRG-3…). The rules ran on the
    /// server: writing again is the caller's choice.
    Conflict,
    /// Postgres itself failed to serialize the transaction (`40001` or `40P01`, no rule): the same call may be repeated.
    Serialization,
    /// The protocol refused what it was given (`22023`).
    Invalid,
    /// The catalog's format, writer version or protocol is not one this SDK speaks, or the protocol refused something
    /// the format does not support (`0A000`).
    Unsupported,
    /// The catalog breaks the format (`XX000`).
    Corrupt,
    /// The role may not do this (`42501`).
    Permission,
    /// The connection was lost: whether a write that was sent took effect is not known. With an idempotency key,
    /// repeating it is safe; reads may always be repeated.
    Unknown,
    /// Any other failure of Postgres.
    Postgres,
}

/// An error of the SDK.
#[derive(Debug, thiserror::Error)]
pub struct Error {
    kind: ErrorKind,
    code: Option<String>,
    rule: Option<String>,
    message: String,
    #[source]
    source: Option<tokio_postgres::Error>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            code: None,
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

    /// The SQLSTATE Postgres answered, if it answered.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// The rule of the specification the protocol refused by (`CMT-7`, `DV-4`…), if it named one.
    pub fn rule(&self) -> Option<&str> {
        self.rule.as_deref()
    }

    /// Whether the same call may be repeated as it is, on any session: only when Postgres failed to serialize it.
    /// A call whose connection was lost ([`ErrorKind::Unknown`]) may be repeated if it reads or is idempotent, which
    /// only the caller knows.
    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Serialization
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.kind)?;
        match (&self.code, &self.rule) {
            (Some(code), Some(rule)) => write!(f, " ({code}, {rule})")?,
            (Some(code), None) => write!(f, " ({code})")?,
            (None, Some(rule)) => write!(f, " ({rule})")?,
            (None, None) => {}
        }
        write!(f, ": {}", self.message)
    }
}

/// The kind of a refusal of Postgres. The protocol's own refusals say `metalake` and carry their rule in `DETAIL`;
/// a `40001` without them is Postgres's.
pub(crate) fn classify(code: &str, from_protocol: bool) -> ErrorKind {
    match code {
        "40001" if from_protocol => ErrorKind::Conflict,
        "40001" | "40P01" => ErrorKind::Serialization,
        "22023" => ErrorKind::Invalid,
        "0A000" => ErrorKind::Unsupported,
        "XX000" if from_protocol => ErrorKind::Corrupt,
        "42501" => ErrorKind::Permission,
        _ => ErrorKind::Postgres,
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(source: tokio_postgres::Error) -> Self {
        match source.as_db_error() {
            Some(db) => {
                let code = db.code().code().to_string();
                let from_protocol = db.message().starts_with("metalake");
                Self {
                    kind: classify(&code, from_protocol),
                    rule: db.detail().filter(|d| is_rule(d)).map(str::to_string),
                    message: db.message().to_string(),
                    code: Some(code),
                    source: Some(source),
                }
            }
            None => Self {
                kind: if source.is_closed() {
                    ErrorKind::Unknown
                } else {
                    ErrorKind::Postgres
                },
                code: None,
                rule: None,
                message: source.to_string(),
                source: Some(source),
            },
        }
    }
}

/// A rule id of the specification: letters, a dash, digits (`CMT-7`, `TT-1`).
fn is_rule(detail: &str) -> bool {
    detail.split_once('-').is_some_and(|(a, n)| {
        !a.is_empty()
            && a.chars().all(|c| c.is_ascii_uppercase())
            && !n.is_empty()
            && n.chars().all(|c| c.is_ascii_digit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_sqlstate() {
        assert_eq!(classify("40001", true), ErrorKind::Conflict);
        assert_eq!(classify("40001", false), ErrorKind::Serialization);
        assert_eq!(classify("40P01", false), ErrorKind::Serialization);
        assert_eq!(classify("22023", true), ErrorKind::Invalid);
        assert_eq!(classify("0A000", true), ErrorKind::Unsupported);
        assert_eq!(classify("XX000", true), ErrorKind::Corrupt);
        assert_eq!(classify("XX000", false), ErrorKind::Postgres);
        assert_eq!(classify("42501", false), ErrorKind::Permission);
        assert_eq!(classify("23505", false), ErrorKind::Postgres);
    }

    #[test]
    fn rule_id_format() {
        assert!(is_rule("CMT-7") && is_rule("TT-1") && is_rule("MRG-12"));
        assert!(!is_rule("cmt-7") && !is_rule("CMT-") && !is_rule("-7") && !is_rule("CMT-7a") && !is_rule("CMT7"));
    }

    #[test]
    fn retryable_error_kinds() {
        assert!(Error::new(ErrorKind::Serialization, "").is_retryable());
        for kind in [
            ErrorKind::Conflict,
            ErrorKind::Unknown,
            ErrorKind::Invalid,
            ErrorKind::Postgres,
        ] {
            assert!(!Error::new(kind, "").is_retryable());
        }
    }
}
