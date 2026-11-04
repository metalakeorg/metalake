//! A file per verb, each a thin translation: arguments to calls, results to `Out`.

pub mod init;
pub mod log;
pub mod maintain;
pub mod merge;
pub mod migrate;
pub mod refs;
pub mod retain;
pub mod sql;
pub mod status;

use metalake::{Ref, Table};

use crate::failure::Failure;

/// A table of a ref, by `namespace.table`.
pub(crate) async fn table(on: &Ref, name: &str) -> Result<Table, Failure> {
    let (namespace, table) = name
        .rsplit_once('.')
        .ok_or_else(|| Failure::usage(format!("{name}: a table is namespace.table")))?;
    let namespace: Vec<&str> = namespace.split('.').collect();
    Ok(on.table(&namespace, table).await?)
}
