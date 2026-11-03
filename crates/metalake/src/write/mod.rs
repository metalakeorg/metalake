//! Writing: the writer, which turns Arrow into Parquet files and their statistics, and transactions, which commit them.

mod transaction;
mod writer;

pub use transaction::{NewTable, Outcome, Transaction};
pub use writer::{TableWriter, WriteOptions};
