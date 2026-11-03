//! The core's error: the protocol's, as the SDK classified it, or one of the core's own.

/// An error of the core.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The protocol, or Postgres, refused or failed; its kind says what to do.
    #[error(transparent)]
    Protocol(#[from] metalake_pg::Error),
    /// No ref, table or object of that name.
    #[error("not found: {0}")]
    NotFound(String),
    /// A definition the core cannot turn into Arrow, or the reverse.
    #[error("schema: {0}")]
    Schema(String),
    /// Object storage failed.
    #[error(transparent)]
    Storage(#[from] object_store::Error),
    /// Something the format allows that this version of the core does not do yet.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// A location or path that is not one the storage of a table can hold.
    #[error("location: {0}")]
    Location(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
