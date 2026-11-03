//! The protocol's SDK (ADR 0003 §3.2, §3.6).
//!
//! MetaLake's protocol is the API, and it runs in Postgres: SQL functions installed in every catalog, which lock,
//! check the rules of the format and write. This crate decides nothing. It installs the protocol in Postgres
//! ([`init`], [`migrate`]), calls it with types from wherever it is linked, and translates what it answers: rows
//! into values, refusals into errors that say what to do.
//!
//! No other crate of MetaLake talks to Postgres.

mod error;
mod install;
mod sources;
mod version;

pub use error::{Error, ErrorKind};
pub use install::{Roles, init, migrate};
pub use sources::{quote_ident, render};
pub use version::{FORMAT_VERSION, PROTOCOL_VERSION, Versions, WRITER_VERSION, check_versions, versions};

pub type Result<T, E = Error> = std::result::Result<T, E>;
