//! A pipeline as the lake keeps it: `_pipelines/<name>/pipeline.json` in its warehouse, beside `state/`, where Arroyo
//! keeps its checkpoints. Both last as long as the lake, so that a pipeline is resumed from any machine that reaches it.
//! A name is taken once: the record is created only if it is not there.

use std::time::{SystemTime, UNIX_EPOCH};

use metalake::Storage;
use object_store::{PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// What was asked of a pipeline: whether it runs is the runner's to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Desired {
    Running,
    Suspended,
}

/// How a pipeline runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunnerKind {
    /// `arroyo run`, a process of the machine.
    Process,
    /// The same, in a container of Arroyo's image.
    Docker,
}

/// Where a pipeline runs: its process or its container, on a machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handle {
    pub kind: RunnerKind,
    /// A process id, or a container's name.
    pub id: String,
    pub host: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub name: String,
    pub query: String,
    pub parallelism: u32,
    pub desired: Desired,
    /// Seconds since the epoch.
    pub created_at: u64,
    pub reads: Vec<String>,
    pub writes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner: Option<Handle>,
}

impl Record {
    pub fn new(name: &str, query: &str, parallelism: u32, reads: Vec<String>, writes: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            query: query.to_string(),
            parallelism,
            desired: Desired::Running,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()),
            reads,
            writes,
            runner: None,
        }
    }
}

/// A pipeline's name: a letter, then letters, digits, `_` and `-`, as a container's name and a path take it.
pub fn check_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let first = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    let rest = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if first && rest && name.len() <= 63 {
        return Ok(());
    }
    Err(Error::usage(format!(
        "{name:?} is not a pipeline's name: a lowercase letter, then lowercase letters, digits, `_` or `-`, at most 63"
    )))
}

/// The pipelines of a lake, in its warehouse.
pub struct Records {
    storage: Storage,
    base: String,
}

const RECORD: &str = "pipeline.json";

impl Records {
    pub fn new(storage: Storage) -> Self {
        let base = format!("{}/_pipelines", storage.base().trim_end_matches('/'));
        Self { storage, base }
    }

    fn dir(&self, name: &str) -> String {
        format!("{}/{name}", self.base)
    }

    fn url(&self, name: &str) -> String {
        format!("{}/{RECORD}", self.dir(name))
    }

    /// Where Arroyo keeps the pipeline's checkpoints and its own state.
    pub fn state(&self, name: &str) -> String {
        format!("{}/state", self.dir(name))
    }

    /// Takes the name, if no pipeline has it.
    pub async fn create(&self, record: &Record) -> Result<()> {
        let path = self.storage.path(&self.url(&record.name))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.storage.store().put_opts(&path, payload(record)?, options).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(Error::usage(format!("a pipeline {} exists", record.name)))
            }
            Err(e) => Err(Error::failed(format!(
                "the pipeline {} could not be kept: {e}",
                record.name
            ))),
        }
    }

    pub async fn put(&self, record: &Record) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(record).map_err(|e| Error::failed(e.to_string()))?;
        Ok(self.storage.put(&self.url(&record.name), bytes.into()).await?)
    }

    pub async fn get(&self, name: &str) -> Result<Record> {
        let url = self.url(name);
        if !self.storage.exists(&url).await? {
            return Err(Error::usage(format!("there is no pipeline {name}")));
        }
        let bytes = self.storage.get(&url).await?;
        serde_json::from_slice(&bytes).map_err(|e| Error::failed(format!("the pipeline {name} is unreadable: {e}")))
    }

    /// Every pipeline, by name.
    pub async fn list(&self) -> Result<Vec<Record>> {
        let mut names: Vec<String> = self
            .storage
            .list(&self.base)
            .await?
            .into_iter()
            .filter_map(|(url, _)| {
                // By what follows the directory: a store may spell its base with one slash more.
                let (_, rest) = url.rsplit_once("/_pipelines/")?;
                let name = rest.strip_suffix(&format!("/{RECORD}"))?;
                (!name.contains('/')).then(|| name.to_string())
            })
            .collect();
        names.sort();
        let mut records = Vec::with_capacity(names.len());
        for name in names {
            records.push(self.get(&name).await?);
        }
        Ok(records)
    }

    /// The record and the state: what is left of the pipeline in the lake.
    pub async fn delete(&self, name: &str) -> Result<()> {
        for (url, _) in self.storage.list(&self.dir(name)).await? {
            self.storage.delete(&url).await?;
        }
        Ok(())
    }
}

fn payload(record: &Record) -> Result<PutPayload> {
    let bytes = serde_json::to_vec_pretty(record).map_err(|e| Error::failed(e.to_string()))?;
    Ok(PutPayload::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> Record {
        Record::new(
            name,
            "INSERT INTO t SELECT * FROM s",
            1,
            vec!["s".into()],
            vec!["t".into()],
        )
    }

    #[tokio::test]
    async fn a_name_is_taken_once() {
        let records = Records::new(Storage::in_memory());
        records.create(&record("totals")).await.unwrap();
        let again = records.create(&record("totals")).await.unwrap_err();
        assert!(again.message.contains("exists"), "{}", again.message);

        let mut changed = records.get("totals").await.unwrap();
        changed.desired = Desired::Suspended;
        records.put(&changed).await.unwrap();
        assert_eq!(records.get("totals").await.unwrap(), changed);
    }

    #[tokio::test]
    async fn listed_and_dropped() {
        let records = Records::new(Storage::in_memory());
        for name in ["b", "a"] {
            records.create(&record(name)).await.unwrap();
        }
        let state = format!("{}/checkpoint-1", records.state("a"));
        records.storage.put(&state, Vec::new().into()).await.unwrap();
        let names: Vec<String> = records.list().await.unwrap().into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["a", "b"], "a checkpoint is not a pipeline");

        records.delete("a").await.unwrap();
        assert!(!records.storage.exists(&state).await.unwrap(), "its state goes with it");
        let names: Vec<String> = records.list().await.unwrap().into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["b"]);
        assert!(records.get("a").await.unwrap_err().message.contains("no pipeline"));
    }

    #[test]
    fn names() {
        for good in ["totals", "a", "orders-by_day2"] {
            check_name(good).unwrap();
        }
        for bad in ["", "Totals", "1x", "a b", "a/b", "_x"] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }
}
