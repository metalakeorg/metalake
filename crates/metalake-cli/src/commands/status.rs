//! `metalake status`: what the lake is. The catalog's connection is here for any Postgres client, since the metadata's
//! views are queried in Postgres's own SQL (ADR 0003 §2.8).

use std::fmt;

use serde::Serialize;

use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

#[derive(Debug, Serialize)]
pub struct Status {
    postgres: String,
    schema: String,
    warehouse: Option<String>,
    format_version: String,
    writer_version: String,
    protocol_version: Option<String>,
    refs: Vec<Named>,
}

#[derive(Debug, Serialize)]
struct Named {
    name: String,
    kind: String,
}

impl Status {
    pub async fn of(lake: &Lake) -> Result<Self, Failure> {
        let versions = lake.catalog.versions();
        let refs = lake.catalog.refs().await?;
        Ok(Self {
            postgres: lake.postgres(),
            schema: lake.schema().to_string(),
            warehouse: lake.catalog.warehouse().map(str::to_string),
            format_version: versions.format_version.clone(),
            writer_version: versions.writer_version.clone(),
            protocol_version: versions.protocol_version.clone(),
            refs: refs
                .iter()
                .map(|r| Named {
                    name: r.name().to_string(),
                    kind: r.kind().to_string(),
                })
                .collect(),
        })
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "postgres   {}", self.postgres)?;
        writeln!(f, "schema     {}", self.schema)?;
        writeln!(f, "warehouse  {}", self.warehouse.as_deref().unwrap_or("none"))?;
        writeln!(f, "format     {}", self.format_version)?;
        writeln!(f, "writer     {}", self.writer_version)?;
        writeln!(f, "protocol   {}", self.protocol_version.as_deref().unwrap_or("none"))?;
        let refs: Vec<String> = self.refs.iter().map(|r| format!("{} ({})", r.name, r.kind)).collect();
        write!(f, "refs       {}", refs.join(", "))
    }
}

pub async fn run(lake: &Lake, out: &Out) -> Result<(), Failure> {
    out.value(&Status::of(lake).await?)
}
