//! A Postgres of a lake's own, for a lake on one machine (ADR 0003 §7, C10e). Postgres cannot be embedded: it is a
//! server in processes of its own, so this crate holds what that takes: its binaries and its cluster, inside the lake.
//! The rest of MetaLake gets a connection string, and cannot tell this Postgres from any other.

mod binaries;
mod cluster;

pub use binaries::{Binaries, MAJOR};
pub use cluster::Cluster;

/// What can go wrong with a lake's Postgres.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The binaries could not be fetched.
    #[error("Postgres binaries: {0}")]
    Archive(#[from] postgresql_archive::Error),
    /// The binaries are not what they should be, or are not there.
    #[error("Postgres binaries: {0}")]
    Binaries(String),
    /// A cluster of a version these binaries cannot run.
    #[error("the lake's Postgres is version {found}, and this metalake runs {MAJOR}")]
    Version { found: String },
    /// Postgres or one of its tools failed: what it said, for a person to act on.
    #[error("{what}: {detail}")]
    Failed { what: String, detail: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
