//! A pipeline as the lake keeps it: `_pipelines/<name>/pipeline.json` in its warehouse, beside `state/`, where Arroyo
//! keeps its checkpoints. Both last as long as the lake, so that a pipeline is resumed from any machine that reaches it.
//! A name is taken once: the record is created only if it is not there.

use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

use metalake::Storage;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
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
const LOCK: &str = "lock";
/// Longer than any change of a pipeline takes: a stop waits 90 seconds for its checkpoint.
const LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(300);

impl Records {
    /// The pipelines of the lake whose warehouse is `warehouse`, a URL.
    pub fn new(storage: Storage, warehouse: &str) -> Self {
        let base = format!("{}/_pipelines", warehouse.trim_end_matches('/'));
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

    /// Whether a pipeline has the name.
    pub async fn has(&self, name: &str) -> Result<bool> {
        Ok(self.storage.exists(&self.url(name)).await?)
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

    /// Runs `work` holding the pipeline's lock, which it releases however `work` ends: with its result, with an error,
    /// or dropped before its end, as a caller interrupted drops it.
    pub async fn locked<T>(&self, name: &str, work: impl Future<Output = Result<T>>) -> Result<T> {
        let lock = self.lock(name).await?;
        let done = work.await;
        lock.release().await;
        done
    }

    /// Takes the pipeline's lock, so that one caller at a time changes it: two resuming it at once would start two
    /// runners on one state. A lock older than any change takes (a stop's checkpoint, a start's settling) was left by
    /// a caller that died, and is taken over by replacing that very lock, so that two callers cannot both take it.
    async fn lock(&self, name: &str) -> Result<Lock> {
        let url = format!("{}/{LOCK}", self.dir(name));
        let path = self.storage.path(&url)?;
        let store = self.storage.store().clone();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let token = format!("{} {} {now}", uuid::Uuid::new_v4().simple(), super::runner::host());
        let taken = |mode| {
            let store = store.clone();
            let path = path.clone();
            let token = token.clone();
            async move {
                let options = PutOptions {
                    mode,
                    ..Default::default()
                };
                store.put_opts(&path, PutPayload::from(token), options).await
            }
        };
        let busy = |held: &str| {
            // `token host time`, or `host time` as a lock was first written.
            let parts: Vec<&str> = held.split(' ').collect();
            let host = match parts.as_slice() {
                [_, host, _] | [host, _] if !host.is_empty() => host,
                _ => "another caller",
            };
            Error::conflict(
                None,
                format!("the pipeline {name} is being changed by {host}: try again once it is done"),
            )
        };
        let failed = |e: object_store::Error| Error::failed(format!("the pipeline {name} could not be locked: {e}"));
        let lock = Lock {
            store: store.clone(),
            path: path.clone(),
            token: token.clone(),
            released: false,
        };
        match taken(PutMode::Create).await {
            Ok(_) => return Ok(lock),
            Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(e) => return Err(failed(e)),
        }
        let held = match store.get(&path).await {
            Ok(held) => held,
            // Released meanwhile: take it now, or say who did.
            Err(object_store::Error::NotFound { .. }) => {
                return match taken(PutMode::Create).await {
                    Ok(_) => Ok(lock),
                    Err(object_store::Error::AlreadyExists { .. }) => Err(busy("")),
                    Err(e) => Err(failed(e)),
                };
            }
            Err(e) => return Err(failed(e)),
        };
        let version = UpdateVersion {
            e_tag: held.meta.e_tag.clone(),
            version: held.meta.version.clone(),
        };
        let text = String::from_utf8_lossy(&held.bytes().await.map_err(failed)?).to_string();
        let since: u64 = text.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0);
        if now.saturating_sub(since) < LOCK_TTL.as_secs() {
            return Err(busy(&text));
        }
        match taken(PutMode::Update(version)).await {
            Ok(_) => Ok(lock),
            Err(object_store::Error::Precondition { .. }) => Err(busy("")),
            // A store with no conditional update, as a local directory: the stale lock is removed and taken anew,
            // where two callers taking it at once is the one window left.
            Err(object_store::Error::NotImplemented { .. }) => {
                let _ = store.delete(&path).await;
                match taken(PutMode::Create).await {
                    Ok(_) => Ok(lock),
                    Err(object_store::Error::AlreadyExists { .. }) => Err(busy("")),
                    Err(e) => Err(failed(e)),
                }
            }
            Err(e) => Err(failed(e)),
        }
    }

    /// The record and the state: what is left of the pipeline in the lake. Its lock is its holder's to release.
    pub async fn delete(&self, name: &str) -> Result<()> {
        for (url, _) in self.storage.list(&self.dir(name)).await? {
            if url.ends_with(&format!("/{LOCK}")) {
                continue;
            }
            self.storage.delete(&url).await?;
        }
        Ok(())
    }
}

/// A pipeline's lock, held until released or dropped.
pub struct Lock {
    store: std::sync::Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    /// What this holder wrote, so that it releases its own lock and never one another took over.
    token: String,
    released: bool,
}

impl Lock {
    async fn release(mut self) {
        self.released = true;
        release(self.store.clone(), self.path.clone(), self.token.clone()).await;
    }
}

impl Drop for Lock {
    /// Dropped before its release, as when a caller interrupts a change: released in the background.
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(release(self.store.clone(), self.path.clone(), self.token.clone()));
        }
    }
}

/// Removes the lock if it is still the one `token` wrote.
async fn release(store: std::sync::Arc<dyn ObjectStore>, path: object_store::path::Path, token: String) {
    let Ok(held) = store.get(&path).await else {
        return;
    };
    if held.bytes().await.is_ok_and(|held| held.as_ref() == token.as_bytes()) {
        let _ = store.delete(&path).await;
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
        let records = Records::new(Storage::in_memory(), "memory:///wh");
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
        let records = Records::new(Storage::in_memory(), "memory:///wh");
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

    #[tokio::test]
    async fn dropping_one_leaves_another_of_a_longer_name() {
        let records = Records::new(Storage::in_memory(), "memory:///wh");
        for name in ["a", "ab"] {
            records.create(&record(name)).await.unwrap();
            let state = format!("{}/checkpoint-1", records.state(name));
            records.storage.put(&state, Vec::new().into()).await.unwrap();
        }
        records.delete("a").await.unwrap();
        assert!(records.has("ab").await.unwrap());
        let state = format!("{}/checkpoint-1", records.state("ab"));
        assert!(records.storage.exists(&state).await.unwrap(), "its state stays");
    }

    #[tokio::test]
    async fn one_caller_at_a_time() {
        let records = Records::new(Storage::in_memory(), "memory:///wh");
        records.create(&record("t")).await.unwrap();
        let held = records
            .locked("t", async {
                let other = records.locked("t", async { Ok(()) }).await.unwrap_err();
                assert!(other.message.contains("being changed"), "{}", other.message);
                assert!(
                    other.message.contains(&super::super::runner::host()),
                    "says who: {}",
                    other.message
                );
                Ok(1)
            })
            .await
            .unwrap();
        assert_eq!(held, 1);
        let failed: Result<()> = records.locked("t", async { Err(Error::failed("x")) }).await;
        assert!(failed.is_err());
        records
            .locked("t", async { Ok(()) })
            .await
            .expect("released however the work ended");

        // A lock a dead caller left is taken over once it is old.
        let lock = format!("{}/{LOCK}", records.dir("t"));
        records
            .storage
            .put(&lock, "gone 1".as_bytes().to_vec().into())
            .await
            .unwrap();
        records
            .locked("t", async { Ok(()) })
            .await
            .expect("a stale lock is taken over");
        assert_eq!(records.list().await.unwrap().len(), 1, "a lock is not a pipeline");
    }

    #[tokio::test]
    async fn a_lock_outlives_the_drop_it_guards() {
        let records = Records::new(Storage::in_memory(), "memory:///wh");
        records.create(&record("t")).await.unwrap();
        records
            .locked("t", async {
                records.delete("t").await?;
                // Dropped, its name is free, but not to change while its dropper holds it.
                let other = records.locked("t", async { Ok(()) }).await.unwrap_err();
                assert!(other.message.contains("being changed"), "{}", other.message);
                Ok(())
            })
            .await
            .unwrap();
        records
            .locked("t", async { Ok(()) })
            .await
            .expect("released by its holder");
    }

    #[tokio::test]
    async fn a_lock_is_released_by_its_holder_alone() {
        let records = Records::new(Storage::in_memory(), "memory:///wh");
        let lock = format!("{}/{LOCK}", records.dir("t"));
        let mine = records.lock("t").await.unwrap();
        // Another took it over meanwhile, as after a takeover: releasing mine leaves theirs.
        records
            .storage
            .put(&lock, "theirs host 9999999999".as_bytes().to_vec().into())
            .await
            .unwrap();
        mine.release().await;
        assert!(records.storage.exists(&lock).await.unwrap());
    }

    #[tokio::test]
    async fn an_interrupted_change_releases_its_lock() {
        let records = Records::new(Storage::in_memory(), "memory:///wh");
        let interrupted = records.locked("t", std::future::pending::<Result<()>>());
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), interrupted).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        records
            .locked("t", async { Ok(()) })
            .await
            .expect("released in the background");
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
