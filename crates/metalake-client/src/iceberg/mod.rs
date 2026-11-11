//! The lake's Iceberg face: when each table's Iceberg metadata is written (REST-6, REST-9). Every table is served to
//! Iceberg clients, its metadata written when one loads it; a table set to `on-commit` on a ref has it written by the
//! Iceberg service after each of its commits there, so that a load finds it there, and a reader of the metadata file
//! alone finds it current. Setting it is a commit of the tables' definitions, on that ref: one commit for all of them.

mod duckdb;
mod flink;
mod jars;
mod profile;
mod pyiceberg;
mod spark;
mod trino;

use std::collections::BTreeMap;

use metalake::{ICEBERG_METADATA, Table};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

pub use profile::{Artifact, Engine, FLINK, ICEBERG_JAVA, Profile, SCALA, SPARK, Store, Target, profile};

use crate::server::{self, Runtime};
use crate::{Error, Lake, Result};

/// The value of [`ICEBERG_METADATA`] that has it written after each commit.
const ON_COMMIT: &str = "on-commit";
/// How many of a table's latest snapshots `status` looks at to count what is behind.
const LOOKED_AT: i64 = 1000;

/// The lake's Iceberg face, from [`Lake::iceberg`].
#[derive(Debug, Clone, Copy)]
pub struct Iceberg<'a> {
    lake: &'a Lake,
}

/// Where the Iceberg metadata of a table set to `on-commit` stands, on a ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IcebergTable {
    /// `namespace.table`.
    pub table: String,
    /// The table's latest snapshot on the ref.
    pub head: Option<i64>,
    /// The sequence its latest metadata file serves, if one was written.
    pub written: Option<i64>,
    /// Its commits on the ref that metadata does not serve yet, of the latest [`LOOKED_AT`].
    pub behind: i64,
    /// When the first of those was committed.
    pub since: Option<String>,
    /// The latest metadata file, for a reader that reads it alone.
    pub metadata: Option<String>,
}

impl Lake {
    /// The lake's Iceberg face.
    pub fn iceberg(&self) -> Iceberg<'_> {
        Iceberg { lake: self }
    }
}

impl Iceberg<'_> {
    /// The lake's Iceberg REST server on this machine: the one that runs, or one started now, detached, which outlives
    /// this process until stopped. Engines connect to its URL.
    pub async fn ensure(&self) -> Result<Runtime> {
        let (identity, schema) = (self.lake.identity(), self.lake.schema().to_string());
        let postgres = self.lake.catalog_postgres()?;
        blocking(move || server::ensure(&identity, &postgres, &schema)).await
    }

    /// The URL engines connect to, the server started if it is not.
    pub async fn uri(&self) -> Result<String> {
        Ok(self.ensure().await?.url)
    }

    /// Stops the lake's server, if one runs on this machine, and says whether one did.
    pub async fn stop(&self) -> Result<bool> {
        let (identity, schema) = (self.lake.identity(), self.lake.schema().to_string());
        blocking(move || server::stop(&identity, &schema)).await
    }

    /// Has the Iceberg metadata of `tables`, each `namespace.table`, written after each of their commits on `on`, in one
    /// commit, and answers its sequence; none if every one was so already. A table `on` does not see refuses all.
    pub async fn on_commit(&self, on: &str, tables: &[&str]) -> Result<Option<i64>> {
        self.set(on, tables, Some(ON_COMMIT)).await
    }

    /// Has it written when a client loads them again, as every table has unless set: the property goes, so that a branch
    /// that inherited `on-commit` no longer has it.
    pub async fn on_load(&self, on: &str, tables: &[&str]) -> Result<Option<i64>> {
        self.set(on, tables, None).await
    }

    async fn set(&self, on: &str, tables: &[&str], value: Option<&str>) -> Result<Option<i64>> {
        if tables.is_empty() {
            return Err(Error::usage("name a table, namespace.table"));
        }
        let on = self.lake.find_ref(on).await?;
        let mut found: BTreeMap<Uuid, (String, Table)> = BTreeMap::new();
        let mut missing = Vec::new();
        for name in tables {
            let (namespace, short) = name
                .rsplit_once('.')
                .ok_or_else(|| Error::usage(format!("{name}: a table is namespace.table")))?;
            let namespace: Vec<&str> = namespace.split('.').collect();
            match on.table(&namespace, short).await {
                Ok(t) => {
                    found.insert(t.id(), (name.to_string(), t));
                }
                Err(metalake::Error::NotFound(_)) => missing.push(name.to_string()),
                Err(e) => return Err(e.into()),
            }
        }
        if !missing.is_empty() {
            return Err(Error::failed(format!(
                "{} sees no {}: nothing changed",
                on.name(),
                missing.join(", ")
            )));
        }
        let mut changed = Vec::new();
        let mut transaction = on.transaction();
        for (id, (name, t)) in &found {
            let current = on.definition(t, None).await?;
            let now = current["properties"][ICEBERG_METADATA].as_str();
            let already = match value {
                Some(value) => now == Some(value),
                None => now.is_none(),
            };
            if !already {
                transaction = transaction.define(*id, json!({"properties": {ICEBERG_METADATA: value}}));
                changed.push(name.as_str());
            }
        }
        if changed.is_empty() {
            return Ok(None);
        }
        let when = value.unwrap_or("on-load");
        let message = format!("iceberg metadata {when}: {}", changed.join(", "));
        Ok(Some(transaction.message(message).commit().await?.seq()))
    }

    /// The tables set to `on-commit` on `on`, and where their Iceberg metadata stands: what it serves, and how many of
    /// their commits it does not yet. It reads the metadata files themselves, so it says so whether the service runs.
    pub async fn status(&self, on: &str) -> Result<Vec<IcebergTable>> {
        let on = self.lake.find_ref(on).await?;
        let marked = self
            .lake
            .catalog
            .tables_with_property(ICEBERG_METADATA, ON_COMMIT)
            .await?;
        let storage = self.lake.storage()?;
        let mut tables = Vec::new();
        for (_, id) in marked.into_iter().filter(|(ref_id, _)| *ref_id == on.id()) {
            let t = on.table_with_id(id).await?;
            let (written, metadata) = latest_metadata(&storage, t.location()).await?;
            let snapshots = on.snapshots(&t, LOOKED_AT).await?;
            let unwritten: Vec<_> = snapshots
                .iter()
                .filter(|s| written.is_none_or(|w| s.snapshot_id > w))
                .collect();
            tables.push(IcebergTable {
                table: format!("{}.{}", t.namespace().join("."), t.name()),
                head: t.snapshot_id(),
                written,
                behind: unwritten.len() as i64,
                since: unwritten.last().and_then(|s| s.committed_at.clone()),
                metadata,
            });
        }
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        Ok(tables)
    }
}

/// The latest metadata file of the table at `location`, by the sequence its name starts with, and that sequence.
async fn latest_metadata(storage: &metalake::Storage, location: &str) -> Result<(Option<i64>, Option<String>)> {
    let dir = format!("{}/metalake-metadata", location.trim_end_matches('/'));
    let found = storage.list(&dir).await?;
    let latest = found
        .into_iter()
        .filter_map(|(path, _)| {
            let name = path.rsplit('/').next()?;
            let seq = name.strip_prefix('v')?.split('-').next()?.parse::<i64>().ok()?;
            Some((seq, path))
        })
        .max_by_key(|(seq, _)| *seq);
    Ok(latest.map_or((None, None), |(seq, path)| (Some(seq), Some(path))))
}

/// Runs what waits on files, locks and processes off the runtime's threads.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Error::failed(e.to_string()))?
}
