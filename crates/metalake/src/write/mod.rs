//! Writing: transactions, and (C4b) the writer that turns Arrow into Parquet files and their statistics.

mod transaction;

pub use transaction::{NewTable, Outcome, Transaction};
