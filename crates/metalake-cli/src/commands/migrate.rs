//! `metalake migrate`: brings the catalog to this binary's protocol, once per upgrade, by whoever administers the lake.
//! A command never does it by itself: it holds every ref, and one person's upgrade is not everyone's.

use std::fmt;
use std::path::Path;

use serde::Serialize;

use crate::out::Out;
use metalake_client::Lake;
use metalake_client::{Error, Status};

#[derive(Debug, Serialize)]
struct Migrated {
    /// The sequence of a migration of the format's rows, if there was one.
    format_migration: Option<i64>,
    #[serde(flatten)]
    status: Status,
}

impl fmt::Display for Migrated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(seq) = self.format_migration {
            writeln!(f, "migrated   at {seq}")?;
        }
        self.status.fmt(f)
    }
}

pub async fn run(dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let (lake, format_migration) = Lake::migrate(dir).await?;
    out.value(&Migrated {
        format_migration,
        status: lake.status().await?,
    })
}
