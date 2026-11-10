//! `metalake changes`: the rows a range of a table's commits inserted and deleted, printed as a statement's are.

use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::TryStreamExt;
use metalake_client::{Error, Lake, TablePath};

use crate::args::Changes;
use crate::out::Out;

pub async fn run(args: &Changes, lake: &Lake, out: &Out) -> Result<(), Error> {
    let path = TablePath::parse(&args.table)?;
    let (schema, rows) = lake.changes(&path, args.after, args.until).await?;
    let rows = rows.map_err(|e| DataFusionError::External(Box::new(e)));
    out.rows(Box::pin(RecordBatchStreamAdapter::new(schema, rows))).await
}
