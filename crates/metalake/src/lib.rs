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
mod error;
mod evolve;
mod maintain;
pub mod partition;
mod scan;
pub mod schema;
pub mod stats;
mod storage;
mod stream;
mod table;
pub mod vectors;
mod write;

pub use catalog::{Catalog, CatalogConfig, LatestPoints, Ref};
pub use error::{Error, Result};
pub use evolve::Evolve;
pub use maintain::Maintenance;
pub use metalake_pg::Retention;
pub use metalake_pg::records::{
    CommitEntry, FileAdd, ListedFile, NamespaceRow, RefRow, SnapshotEntry, TableName, TableSnapshot,
};
pub use scan::{Scan, ScanTask, TaskReader, TaskVector, listed_vector};
pub use storage::Storage;
pub use stream::{
    CHANGE_TYPE, COMMIT_SEQ, COMMIT_TIMESTAMP, RowChange, Start, StreamCursor, StreamStep, StreamTask, TableStream,
    TaskRows,
};
pub use table::{Field, Table};
pub use write::{Merge, MergeReport, NewTable, Outcome, TableWriter, Transaction, WriteOptions};
