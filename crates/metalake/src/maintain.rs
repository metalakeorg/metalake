//! Maintenance: what takes history and objects away once no read can reach them (RET-1 to RET-9, TT-1). The protocol
//! decides what goes; Postgres cannot touch storage, so the core deletes the objects, and forgets them only after.
//! Each task is a loop of the protocol's batches, safe to repeat and to run beside commits; when to run it is the
//! caller's. Flushing and compacting rewrite files instead: a `replace` commit (CMT-11) of the same live rows, which
//! the protocol refuses if a delete got in first, since it counts them.

use std::collections::BTreeSet;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt, stream};
use metalake_pg::BATCH;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::scan::{Scan, ScanTask};
use crate::storage::Storage;
use crate::table::Table;
use crate::write::{TableWriter, WriteOptions, target_size};

/// The maintenance of a catalog.
#[derive(Debug, Clone)]
pub struct Maintenance {
    catalog: Catalog,
    batch: i32,
}

impl Maintenance {
    pub(crate) fn new(catalog: Catalog) -> Self {
        Self { catalog, batch: BATCH }
    }

    /// The rows or objects of one batch at most, which bounds how long a batch holds the refs (RET-5) or its
    /// transaction stays open while objects are deleted.
    pub fn batch(mut self, batch: i32) -> Self {
        self.batch = batch;
        self
    }

    /// Deletes the rows no retained read reaches and queues their objects (RET-4), and answers how many rows went.
    pub async fn expire(&self) -> Result<u64> {
        let p = self.catalog.protocol();
        let mut rows = 0;
        loop {
            let n = p.expire(&self.catalog.session().await?, Some(self.batch)).await?;
            if n == 0 {
                return Ok(rows);
            }
            rows += n as u64;
        }
    }

    /// Deletes from `storage` the queued objects past the grace (RET-9), and answers how many went. Each batch holds
    /// its objects in one transaction, so a sweeper beside it skips them. If an object cannot be deleted, those
    /// deleted are forgotten, the rest stay queued and the error is returned: sweeping again repeats what is left.
    pub async fn sweep(&self, storage: &Storage) -> Result<u64> {
        let p = self.catalog.protocol();
        let mut swept = 0;
        loop {
            let mut session = self.catalog.session().await?;
            let sweep = p.sweep(&mut session, Some(self.batch)).await?;
            if sweep.due.is_empty() {
                return Ok(swept);
            }
            let (deleted, failed) = storage.delete_all(&sweep.due).await;
            swept += sweep.done(&deleted).await? as u64;
            if let Some(e) = failed {
                return Err(e);
            }
        }
    }

    /// Deletes from `storage` the objects of expired intents that no commit took (RET-7), and answers how many went.
    pub async fn orphans(&self, storage: &Storage) -> Result<u64> {
        let p = self.catalog.protocol();
        let session = self.catalog.session().await?;
        let mut forgotten = 0;
        for paths in p.orphans(&session, None).await?.chunks(self.batch.max(1) as usize) {
            let (deleted, failed) = storage.delete_all(paths).await;
            forgotten += p.forget_intents(&session, &deleted).await? as u64;
            if let Some(e) = failed {
                return Err(e);
            }
        }
        Ok(forgotten)
    }

    /// Turns the objects under a table's location that are older than `age` and that nothing knows into expired intents,
    /// which `orphans` then deletes, and answers how many (RET-7b). A writer that commits one late is refused (CMT-9).
    /// Only this lists storage, so it is asked for by table, with `age` beyond any write in progress.
    pub async fn strays(&self, table: &Table, storage: &Storage, age: Duration) -> Result<u64> {
        let listed = storage.list(table.location()).await?;
        // A store that spelled a path otherwise than the catalog would make a live file look unknown, and lose it.
        let spelled: BTreeSet<&str> = listed.iter().map(|(url, _)| url.as_str()).collect();
        for task in self.files(table).await? {
            let path = task.path.filter(|p| p.starts_with(table.location()));
            if let Some(path) = path.filter(|p| !spelled.contains(p.as_str())) {
                return Err(Error::Location(format!(
                    "{path} is not among what {} lists",
                    table.location()
                )));
            }
        }
        let generated = format!("{}/metalake-", table.location().trim_end_matches('/'));
        let old: Vec<String> = listed
            .into_iter()
            .filter(|(url, seconds)| *seconds >= age.as_secs() as i64 && !url.starts_with(&generated))
            .map(|(url, _)| url)
            .collect();
        let (p, session) = (self.catalog.protocol(), self.catalog.session().await?);
        let mut adopted = 0;
        for paths in old.chunks(self.batch.max(1) as usize) {
            adopted += p.adopt_orphans(&session, table.ref_id(), table.id(), paths).await? as u64;
        }
        Ok(adopted)
    }

    /// Moves a table's inline files to storage if they are due (INL-2), and answers the commit, or none if they are
    /// not. `table` is the state read: if a delete has touched one of its files since, the commit is refused.
    pub async fn flush(&self, table: &Table, storage: &Storage) -> Result<Option<i64>> {
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        if !p.inline_flush_due(&session, table.ref_id(), table.id()).await? {
            return Ok(None);
        }
        drop(session);
        let mut tasks = self.files(table).await?;
        tasks.retain(|t| t.path.is_none());
        self.rewrite(table, storage, tasks).await.map(Some)
    }

    /// Rewrites a table's files below its target size and those with a deletion vector, so that a read opens fewer
    /// files and skips no deleted rows, and answers the commit, or none if a rewrite would change nothing. `table`
    /// is the state read, as in [`Maintenance::flush`].
    pub async fn compact(&self, table: &Table, storage: &Storage) -> Result<Option<i64>> {
        let target = target_size(table) as i64;
        let mut tasks = self.files(table).await?;
        tasks.retain(|t| t.vector.is_some() || t.file_size < target);
        if tasks.len() < 2 && tasks.iter().all(|t| t.vector.is_none()) {
            return Ok(None);
        }
        self.rewrite(table, storage, tasks).await.map(Some)
    }

    async fn files(&self, table: &Table) -> Result<Vec<ScanTask>> {
        Scan::new(&self.catalog, table).plan().await?.try_collect().await
    }

    /// The live rows of `tasks` in new stored files, the table's latest shape, replacing them in one commit.
    async fn rewrite(&self, table: &Table, storage: &Storage, tasks: Vec<ScanTask>) -> Result<i64> {
        let ids: Vec<i64> = tasks.iter().map(|t| t.file_id).collect();
        let on = self
            .catalog
            .refs()
            .await?
            .into_iter()
            .find(|r| r.id() == table.ref_id());
        let on = on.ok_or_else(|| Error::NotFound(format!("the ref of {}", table.name())))?;
        let options = WriteOptions {
            stored: true,
            ..Default::default()
        };
        let mut writer = TableWriter::new(&self.catalog, table, storage.clone(), options)?;
        let scan = Scan::new(&self.catalog, table);
        let mut batches = scan.read(stream::iter(tasks).map(Ok).boxed(), storage);
        while let Some(batch) = batches.try_next().await? {
            writer.write(&batch).await?;
        }
        let files = writer.finish().await?;
        let done = on
            .transaction()
            .remove(table, ids)
            .append(files)
            .replace()
            .commit()
            .await?;
        Ok(done.seq())
    }

    /// Copies commit times into the catalog before Postgres discards them (TT-1), and answers how many. It should run
    /// continuously.
    pub async fn fill_commit_times(&self) -> Result<u64> {
        let p = self.catalog.protocol();
        let mut filled = 0;
        loop {
            let n = p
                .fill_committed_at(&self.catalog.session().await?, Some(self.batch))
                .await?;
            if n == 0 {
                return Ok(filled);
            }
            filled += n as u64;
        }
    }
}
