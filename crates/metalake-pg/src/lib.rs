//! The protocol's SDK (ADR 0003 §3.2, §3.6).
//!
//! MetaLake's protocol is the API, and it runs in Postgres: SQL functions installed in every catalog, which lock,
//! check the rules of the format and write. This crate decides nothing. It installs the protocol in Postgres
//! ([`init`], [`migrate`]), calls it with types from wherever it is linked, and translates what it answers: rows
//! into values, refusals into errors that say what to do.
//!
//! No other crate of MetaLake talks to Postgres.

mod calls;
mod error;
mod install;
mod pool;
pub mod records;
mod session;
mod sources;
pub mod tls;
mod version;

pub use calls::{BATCH, Commit, CreateRef, INTERFACE, Merge, Protocol, Prune, Retention, Sweep};
pub use error::{Error, ErrorKind};
pub use install::{Roles, init, migrate};
pub use pool::{Credential, Password, Pool, PooledSession};
pub use session::Session;
pub use sources::{quote_ident, render};
pub use version::{FORMAT_VERSION, PROTOCOL_VERSION, Versions, WRITER_VERSION, check_versions, versions, warehouse};

pub type Result<T, E = Error> = std::result::Result<T, E>;
