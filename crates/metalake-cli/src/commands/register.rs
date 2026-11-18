//! `metalake register`: the Parquet files under a prefix of the lake's store as a table's, in place. Its report is
//! printed whatever happened; refused files make it fail, having committed nothing.

use metalake_client::{Error, Lake, RegisterRequest};

use crate::args::Register;
use crate::out::Out;

pub async fn run(args: &Register, lake: &Lake, out: &Out) -> Result<(), Error> {
    let partition = args
        .partition
        .iter()
        .map(|text| RegisterRequest::partition_column(text))
        .collect::<Result<Vec<_>, _>>()?;
    let report = lake
        .register(&RegisterRequest {
            source: args.source.clone(),
            table: args.table.clone(),
            partition,
            adopt: args.adopt,
            location: args.location.clone(),
            check: args.check,
            on: args.on.clone(),
        })
        .await?;
    out.value(&report)?;
    match report.refused.len() {
        0 => Ok(()),
        n => Err(Error::usage(format!(
            "{n} files refused, and nothing registered: see the report"
        ))),
    }
}
