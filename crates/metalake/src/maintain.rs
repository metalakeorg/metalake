//! Maintenance: what takes history and objects away once no read can reach them (RET-1 to RET-9, TT-1). The protocol
//! decides what goes; Postgres cannot touch storage, so the core deletes the objects, and forgets them only after.
//! Each task is a loop of the protocol's batches, safe to repeat and to run beside commits; when to run it is the
//! caller's. Flushing and compacting rewrite files instead: a `replace` commit (CMT-11) of the same live rows, which
//! the protocol refuses if a delete got in first, since it counts them. Filling footers changes no row: it keeps in
//! the catalog the footers of files that came without one (FTR-1).

use std::collections::BTreeSet;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt, stream};
use metalake_pg::BATCH;
use parquet::file::metadata::ParquetMetaDataReader;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::scan::{Scan, ScanTask};
use crate::stats;
use crate::storage::Storage;
use crate::table::Table;
use crate::write::{TableWriter, WriteOptions, target_size};

/// The bytes read from a file's end for its footer: the longest footer the catalog keeps, its length and `PAR1`.
const FOOTER_TAIL: u64 = 64 * 1024 + 8;
/// Files whose tails are read at once.
const TAILS: usize = 16;

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

    /// Keeps in the catalog the footers of a table's files that came with no footer and no `footer_size`, as a file
    /// another engine wrote does (STA-1, FTR-1), so that a read finds it there and plans its row groups: one request
    /// per file, for its tail. A footer longer than the catalog keeps is left, and its readers fetch it as before.
    /// Answers how many it stored. Safe to repeat and to run beside commits: a file gone meanwhile, or given its footer,
    /// is left.
    pub async fn footers(&self, table: &Table, storage: &Storage) -> Result<u64> {
        let mut tasks = self.files(table).await?;
        tasks.retain(|t| t.footer_size == 0 && t.footer.is_none());
        let files = tasks
            .into_iter()
            .filter_map(|t| Some((t.path?, u64::try_from(t.file_size).ok()?)))
            .collect();
        self.keep_footers(table.id(), files, storage).await
    }

    /// Keeps the footers of `files` of table `table_id`, each a path and its size, read from their tails: what
    /// [`Maintenance::footers`] does for files it found, for a writer that knows which files came without one, as
    /// the Iceberg service does after a commit. Answers how many it stored.
    pub async fn keep_footers(&self, table_id: Uuid, files: Vec<(String, u64)>, storage: &Storage) -> Result<u64> {
        let read = files.into_iter().map(|(path, size)| {
            let storage = storage.clone();
            async move {
                match storage.tail(&path, size, FOOTER_TAIL.min(size)).await {
                    Ok(tail) => Ok(footer_of(&tail).map(|footer| (path, footer))),
                    Err(e) if e.is_missing_object() => Ok(None),
                    Err(e) => Err(e),
                }
            }
        });
        let found: Vec<(String, Vec<u8>)> = stream::iter(read)
            .buffer_unordered(TAILS)
            .try_filter_map(|found| async move { Ok(found) })
            .try_collect()
            .await?;

        let p = self.catalog.protocol();
        let mut stored = 0;
        for chunk in found.chunks(self.batch.max(1) as usize) {
            let (paths, footers): (Vec<String>, Vec<Vec<u8>>) = chunk.iter().cloned().unzip();
            stored += p
                .add_footers(&self.catalog.session().await?, table_id, &paths, &footers)
                .await? as u64;
        }
        Ok(stored)
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

/// The footer at the end of `tail`, a file's last bytes, if they hold it all and it reads as Parquet's.
fn footer_of(tail: &[u8]) -> Option<Vec<u8>> {
    let size = usize::try_from(stats::footer_size(tail)?).ok()?;
    let n = tail.len();
    if size == 0 || size + 8 > n {
        return None;
    }
    let footer = &tail[n - 8 - size..n - 8];
    ParquetMetaDataReader::decode_metadata(footer).ok()?;
    Some(footer.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_of_partial_or_other_tail_skips() {
        // a length that the tail does not hold, a footer of no bytes, no PAR1, and bytes that are not a footer
        let tail = |footer: &[u8], length: u32, magic: &[u8; 4]| {
            let mut t = footer.to_vec();
            t.extend_from_slice(&length.to_le_bytes());
            t.extend_from_slice(magic);
            t
        };
        assert_eq!(footer_of(&tail(&[1, 2], 9, b"PAR1")), None);
        assert_eq!(footer_of(&tail(&[], 0, b"PAR1")), None);
        assert_eq!(footer_of(&tail(&[1, 2], 2, b"NOPE")), None);
        assert_eq!(footer_of(&tail(&[1, 2], 2, b"PAR1")), None);
    }
}
