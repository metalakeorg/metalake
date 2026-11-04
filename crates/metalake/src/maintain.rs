//! Maintenance: what takes history and objects away once no read can reach them (RET-1 to RET-9, TT-1). The protocol
//! decides what goes; Postgres cannot touch storage, so the core deletes the objects, and forgets them only after.
//! Each task is a loop of the protocol's batches, safe to repeat and to run beside commits; when to run it is the
//! caller's.

use metalake_pg::BATCH;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::storage::Storage;

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
