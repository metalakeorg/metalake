//! What a lake is. The catalog's connection is here for any Postgres client, since the metadata's views are queried in
//! Postgres's own SQL (ADR 0003 §2.8).

use std::fmt;

use serde::Serialize;

use crate::{Lake, Result};

#[derive(Debug, Serialize)]
pub struct Status {
    pub postgres: Option<String>,
    pub local: Option<String>,
    pub schema: String,
    pub warehouse: Option<String>,
    pub format_version: String,
    pub writer_version: String,
    pub protocol_version: Option<String>,
    pub refs: Vec<Named>,
}

#[derive(Debug, Serialize)]
pub struct Named {
    pub name: String,
    pub kind: String,
}

impl Lake {
    pub async fn status(&self) -> Result<Status> {
        let versions = self.catalog.versions();
        let refs = self.catalog.refs().await?;
        Ok(Status {
            postgres: self.postgres(),
            local: self.local().map(|l| l.display().to_string()),
            schema: self.schema().to_string(),
            warehouse: self.catalog.warehouse().map(str::to_string),
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
        match (&self.postgres, &self.local) {
            (Some(postgres), _) => writeln!(f, "postgres   {postgres}")?,
            (None, local) => writeln!(f, "local      {}", local.as_deref().unwrap_or("none"))?,
        }
        writeln!(f, "schema     {}", self.schema)?;
        writeln!(f, "warehouse  {}", self.warehouse.as_deref().unwrap_or("none"))?;
        writeln!(f, "format     {}", self.format_version)?;
        writeln!(f, "writer     {}", self.writer_version)?;
        writeln!(f, "protocol   {}", self.protocol_version.as_deref().unwrap_or("none"))?;
        let refs: Vec<String> = self.refs.iter().map(|r| format!("{} ({})", r.name, r.kind)).collect();
        write!(f, "refs       {}", refs.join(", "))
    }
}
