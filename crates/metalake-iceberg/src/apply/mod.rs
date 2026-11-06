//! What a client asks to change, turned into what the protocol commits. Nothing here knows HTTP.

pub mod update;

/// Why a request is not committed. Neither has changed anything (REST-2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The request asks what the service does not do: it would fail the same way again.
    Invalid(String),
    /// The table is not as the client saw it: it may read the table again and rebuild its commit.
    Conflict(String),
}
