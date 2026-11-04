//! Writing: the writer, which turns Arrow into Parquet files and their statistics, and transactions, which commit them.

mod delete;
mod merge;
mod transaction;
mod writer;

pub use merge::{Merge, MergeReport};
pub use transaction::{NewTable, Outcome, Transaction};
pub(crate) use writer::target_size;
pub use writer::{TableWriter, WriteOptions};
