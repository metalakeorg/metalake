//! What a sink does after a failure with the files it prepared and had not committed (CMT-10).
//!
//! An engine keeps a [`FileAdd`] for each file in its checkpoint, and commits them under an idempotency key once that
//! checkpoint is complete. After a failure, that commit may come long after the files were written.

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt, stream};
use metalake_pg::records::FileAdd;
use object_store::ObjectStoreExt;
use uuid::Uuid;

use crate::catalog::Ref;
use crate::error::{Error, Result};
use crate::storage::Storage;

/// Objects asked for at once.
const CONCURRENCY: usize = 16;

impl Ref {
    /// Makes `files`, prepared for version `version` of the key `app_id`, ready to commit: renews their intents for `ttl`,
    /// and checks that their objects are there. It does nothing if that version already committed: the commit answers that
    /// by its key, and its files are the catalog's now. Objects that are gone fail it, and the epoch has to be written
    /// again.
    ///
    /// An orphan collector that had listed a file before its intent was renewed may still delete it. It runs on intents
    /// past their expiry, so a collector has to leave a restoring writer time, as it does a writer at work.
    pub async fn restore_prepared(
        &self,
        app_id: &str,
        version: i64,
        files: &[FileAdd],
        storage: &Storage,
        ttl: Duration,
    ) -> Result<()> {
        self.writable()?;
        let catalog = self.catalog();
        let session = catalog.session().await?;
        let p = catalog.protocol();
        if p.key_version(&session, self.id(), app_id).await? >= Some(version) {
            return Ok(());
        }
        let mut by_table: BTreeMap<Uuid, Vec<String>> = BTreeMap::new();
        for file in files {
            if let Some(path) = &file.path {
                by_table.entry(file.table_id).or_default().push(path.clone());
            }
        }
        for (table_id, paths) in &by_table {
            p.register_intents(&session, self.id(), *table_id, paths, ttl).await?;
        }
        stream::iter(by_table.values().flatten())
            .map(|path| check(storage, path))
            .buffer_unordered(CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }
}

async fn check(storage: &Storage, path: &str) -> Result<()> {
    match storage.store().head(&storage.path(path)?).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Err(Error::NotFound(format!(
            "{path}, prepared and not committed: that epoch has to be written again"
        ))),
        Err(e) => Err(e.into()),
    }
}
