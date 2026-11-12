//! MetaLake's core (ADR 0003 §3): a library that runs in the engine's process, neutral to the engine.
//!
//! It holds no rule of the format: the rules run in Postgres, as the protocol, which the SDK (`metalake-pg`) calls.
//! The core does what they leave to a reader and a writer, computation and I/O, and hands out Arrow.
//!
//! ```no_run
//! # async fn example() -> metalake::Result<()> {
//! use metalake::{Catalog, CatalogConfig};
//!
//! let catalog = Catalog::connect(CatalogConfig::new("host=localhost user=postgres", "lake")).await?;
//! let events = catalog.branch("main").await?.table(&["db"], "events").await?;
//! println!("{} has {} fields", events.name(), events.schema().fields().len());
//! # Ok(())
//! # }
//! ```

pub mod bounds;
mod catalog;
pub mod credential;
pub mod environment;
mod error;
mod evolve;
mod follow;
mod maintain;
pub mod partition;
pub mod path;
mod scan;
pub mod schema;
pub mod stats;
mod storage;
mod stream;
mod table;
pub mod vectors;
mod write;

/// The table property that says when a table's Iceberg metadata is written (REST-9): `on-load`, when a client loads the
/// table, or `on-commit`, after each commit, by the Iceberg service. A table property, so it is set on a ref.
pub const ICEBERG_METADATA: &str = "metalake.iceberg.metadata";

pub use catalog::{Catalog, CatalogConfig, LatestPoints, Ref, RefValue};
pub use error::{Error, Result};
pub use evolve::Evolve;
pub use follow::{FeedCursor, Lead};
pub use maintain::Maintenance;
pub use metalake_pg::records::{
    CommitEntry, FeedCommit, FileAdd, ListedFile, NamespaceRow, RefRow, SnapshotEntry, TableName, TableSnapshot,
};
pub use metalake_pg::{ErrorKind, Retention};
pub use path::{InvalidPath, TablePath};
pub use scan::{Scan, ScanTask, TaskReader, TaskVector, listed_vector};
pub use storage::Storage;
pub use stream::{
    CHANGE_TYPE, COMMIT_SEQ, COMMIT_TIMESTAMP, RowChange, Start, StreamCursor, StreamStep, StreamTask, TableChanges,
    TableStream, TaskRows,
};
pub use table::{Field, Table};
pub use write::{Merge, MergeReport, NewTable, Outcome, RowLocation, TableWriter, Transaction, WriteOptions};
