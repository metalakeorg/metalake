//! `metalake register`: the Parquet files under a prefix of the lake's store as a table's, in place. Its report is
//! printed whatever happened; refused files make it fail, having committed nothing.

use metalake_client::{Error, Lake, RegisterRequest, RegisterSchemaRequest};

use crate::args::{Register, RegisterSchema};
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

/// `metalake register-schema`: each prefix under one a table of a schema. Its report is printed whatever happened; a
/// table refused or failing makes it fail, the others registered.
pub async fn schema(args: &RegisterSchema, lake: &Lake, out: &Out) -> Result<(), Error> {
    let report = lake
        .register_schema(&RegisterSchemaRequest {
            source: args.source.clone(),
            schema: args.schema.clone(),
            adopt: args.adopt,
            check: args.check,
            on: args.on.clone(),
        })
        .await?;
    out.value(&report)?;
    if report.clean() {
        return Ok(());
    }
    let unregistered = report
        .tables
        .iter()
        .filter(|t| t.error.is_some() || t.report.as_ref().is_some_and(|r| !r.refused.is_empty()))
        .count();
    Err(Error::usage(format!(
        "{unregistered} of {} tables not registered: see the report",
        report.tables.len()
    )))
}
