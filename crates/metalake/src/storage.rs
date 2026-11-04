//! Object storage, behind one handle per location (`object_store`). The catalog stores locations and paths as URLs
//! (`gs://lake/t/a.parquet`); a handle reaches every path under its location's store. Local disk and memory are
//! always there; S3, GCS and Azure come with the crate's features of the same names.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::{Error, Result};

/// The store of a location, and the URL prefix of every path in it.
#[derive(Debug, Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    /// The scheme and authority of the location, `gs://lake` or `file://`, which every path it holds starts with.
    base: String,
}

impl Storage {
    /// The store of `location`, a URL, with the options of its service (credentials, region…), which stay with the
    /// caller and are never stored in the catalog.
    pub fn new<I, K, V>(location: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let url = url::Url::parse(location).map_err(|e| Error::Location(format!("{location}: {e}")))?;
        let (store, _) = object_store::parse_url_opts(&url, options)?;
        let base = format!("{}://{}", url.scheme(), url.authority());
        Ok(Self {
            store: Arc::from(store),
            base,
        })
    }

    /// A store the engine already has, for the paths under `location`, a URL.
    pub fn from_store(location: &str, store: Arc<dyn ObjectStore>) -> Result<Self> {
        let url = url::Url::parse(location).map_err(|e| Error::Location(format!("{location}: {e}")))?;
        Ok(Self {
            store,
            base: format!("{}://{}", url.scheme(), url.authority()),
        })
    }

    /// A store in memory, for tests and for files that never leave the process.
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            base: "memory://".into(),
        }
    }

    /// The store, for an engine that reads through `object_store` itself.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The path in the store of `url`, which must be on this store's scheme and authority.
    pub fn path(&self, url: &str) -> Result<Path> {
        let parsed = url::Url::parse(url).map_err(|e| Error::Location(format!("{url}: {e}")))?;
        if format!("{}://{}", parsed.scheme(), parsed.authority()) != self.base {
            return Err(Error::Location(format!("{url} is not in {}", self.base)));
        }
        Path::from_url_path(parsed.path()).map_err(|e| Error::Location(format!("{url}: {e}")))
    }

    pub async fn put(&self, url: &str, bytes: Bytes) -> Result<()> {
        self.store.put(&self.path(url)?, PutPayload::from(bytes)).await?;
        Ok(())
    }

    pub async fn get(&self, url: &str) -> Result<Bytes> {
        Ok(self.store.get(&self.path(url)?).await?.bytes().await?)
    }

    /// Deletes an object; deleting one that is not there is not an error, since sweeping may be repeated (RET-4).
    pub async fn delete(&self, url: &str) -> Result<()> {
        match self.store.delete(&self.path(url)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes objects through the store's batches, and answers those that are gone and the error that stopped it, if
    /// any. A batch may report an object already gone as a failure, without saying which: what it leaves goes one at
    /// a time, up to the first that truly fails.
    pub(crate) async fn delete_all(&self, urls: &[String]) -> (Vec<String>, Option<Error>) {
        let mut pending = BTreeMap::new();
        let mut failed = None;
        for url in urls {
            match self.path(url) {
                Ok(path) => {
                    pending.insert(path, url);
                }
                Err(e) => {
                    failed.get_or_insert(e);
                }
            }
        }
        let paths: Vec<_> = pending.keys().cloned().map(Ok).collect();
        let mut results = self.store.delete_stream(stream::iter(paths).boxed());
        let mut gone = Vec::new();
        while let Some(result) = results.next().await {
            if let Some(url) = result.ok().and_then(|path| pending.remove(&path)) {
                gone.push(url.clone());
            }
        }
        for url in pending.into_values() {
            match self.delete(url).await {
                Ok(()) => gone.push(url.clone()),
                Err(e) => {
                    failed.get_or_insert(e);
                    break;
                }
            }
        }
        (gone, failed)
    }
}
