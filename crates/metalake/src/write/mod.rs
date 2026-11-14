//! Writing: the writer, which turns Arrow into Parquet files and their statistics, and transactions, which commit them.

mod alter;
mod delete;
mod merge;
mod prepared;
mod transaction;
mod writer;

pub use alter::TableChange;
pub use merge::{Merge, MergeReport};
pub use transaction::{NewTable, Outcome, Transaction};
pub(crate) use writer::target_size;
pub use writer::{RowLocation, TableWriter, WriteOptions};
