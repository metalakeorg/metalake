//! What the core's tests share: the catalog to test against, and a store that counts its requests.

#![allow(dead_code)]

use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};

pub fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

pub fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("core_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A store in memory that counts its GET requests, each range of a read being one, as a remote store's are.
#[derive(Debug, Default)]
pub struct Counting {
    inner: InMemory,
    gets: AtomicUsize,
    refused: Arc<Mutex<Vec<Path>>>,
    missing: Arc<Mutex<Vec<Path>>>,
}

impl Counting {
    pub fn take(&self) -> usize {
        self.gets.swap(0, Ordering::SeqCst)
    }

    /// From now on, deleting these objects reports them not found, as some stores do for an object already gone.
    pub fn report_missing(&self, paths: Vec<Path>) {
        *self.missing.lock().unwrap() = paths;
    }

    /// From now on, deleting these objects fails, as a store that is down does.
    pub fn refuse_deletes(&self, paths: Vec<Path>) {
        *self.refused.lock().unwrap() = paths;
    }
}

impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let refused = self.refused.clone();
        let missing = self.missing.clone();
        let locations = locations
            .map(move |location| {
                let location = location?;
                if refused.lock().unwrap().contains(&location) {
                    return Err(object_store::Error::Generic {
                        store: "Counting",
                        source: format!("refused {location}").into(),
                    });
                }
                if missing.lock().unwrap().contains(&location) {
                    return Err(object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "gone".into(),
                    });
                }
                Ok(location)
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
