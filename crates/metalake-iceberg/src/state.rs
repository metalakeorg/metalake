//! What every request shares: the catalog, reached once, the ref an Iceberg catalog's tables are read on, the worker
//! that keeps the footers of what clients commit, and the one that materializes tables marked on commit.

use std::time::Duration;

use metalake::{Catalog, CatalogConfig, Error, Ref, Result, Storage};

use crate::footers::{Footers, Job, QUEUE};
use crate::load::load;
use crate::materialize::Materializer;

/// How long stopping waits for footers still queued, which are maintenance's after it.
const FOOTERS_DRAIN: Duration = Duration::from_secs(10);

/// A connection to a catalog, for as long as the server runs.
#[derive(Debug, Clone)]
pub struct State {
    catalog: Catalog,
    main: Ref,
    footers: Footers,
    materializer: Option<Materializer>,
}

impl State {
    /// Reaches the catalog of `schema` in `postgres`, and refuses one whose versions it does not speak.
    pub async fn connect(postgres: &str, schema: &str) -> Result<Self> {
        let catalog = Catalog::connect(CatalogConfig::new(postgres, schema)).await?;
        let main = catalog.branch("main").await?;
        let keeper = catalog.clone();
        let footers = Footers::start(QUEUE, move |job: Job| {
            let catalog = keeper.clone();
            async move {
                let storage = warehouse(&catalog).map_err(|e| e.to_string())?;
                let maintenance = catalog.maintenance();
                let kept = maintenance.keep_footers(job.table_id, job.files, &storage).await;
                kept.map_err(|e| e.to_string())
            }
        });
        Ok(Self {
            catalog,
            main,
            footers,
            materializer: None,
        })
    }

    /// The same, materializing the tables marked `on-commit` after each of their commits (REST-6), when this process
    /// leads the catalog's, as the service does unless told not to. A load is what a client's load is, of the table on
    /// `main`, as the service serves its tables there.
    pub fn materializing(mut self) -> Self {
        let serving = self.clone();
        let load = move |table: uuid::Uuid| {
            let state = serving.clone();
            async move {
                let table = state.main().table_with_id(table).await?;
                load(&state, &table).await.map(|_| ())
            }
        };
        self.materializer = Some(Materializer::start(self.catalog.clone(), load));
        self
    }

    /// Tells the worker that materializes on commit, if this process runs one, that a commit changed which tables are
    /// marked: it looks at the feed at once, rather than within its pause.
    pub(crate) fn wake_materializer(&self) {
        if let Some(materializer) = &self.materializer {
            materializer.wake();
        }
    }

    pub(crate) fn footers(&self) -> &Footers {
        &self.footers
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// A ref by name, branch or tag.
    pub(crate) async fn named(&self, name: &str) -> Result<Ref> {
        match self.catalog.branch(name).await {
            Err(Error::NotFound(_)) => self.catalog.tag(name).await,
            found => found,
        }
    }

    /// The store of the catalog's warehouse, with the credentials of the environment, as a native client has them.
    pub(crate) fn storage(&self) -> Result<Storage> {
        warehouse(&self.catalog)
    }

    pub(crate) fn main(&self) -> &Ref {
        &self.main
    }

    /// Stops materializing, lets the footers queued finish for a while, then closes the catalog's connections.
    pub(crate) async fn close(&self) {
        if let Some(materializer) = &self.materializer {
            materializer.stop().await;
        }
        self.footers.stop(FOOTERS_DRAIN).await;
        self.catalog.close();
    }
}

fn warehouse(catalog: &Catalog) -> Result<Storage> {
    const CREDENTIALS: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];
    let warehouse = catalog
        .warehouse()
        .ok_or_else(|| Error::Location("the catalog has no warehouse".into()))?;
    let options =
        std::env::vars_os().filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let options = options
        .filter(|(key, _)| CREDENTIALS.iter().any(|prefix| key.starts_with(prefix)))
        .map(|(key, value)| (key.to_ascii_lowercase(), value));
    Storage::new(warehouse, options)
}
