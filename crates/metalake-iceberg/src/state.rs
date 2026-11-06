//! What every request shares: the catalog, reached once, and the ref an Iceberg catalog's tables are read on.

use metalake::{Catalog, CatalogConfig, Error, Ref, Result, Storage};

/// A connection to a catalog, for as long as the server runs.
#[derive(Debug, Clone)]
pub struct State {
    catalog: Catalog,
    main: Ref,
}

impl State {
    /// Reaches the catalog of `schema` in `postgres`, and refuses one whose versions it does not speak.
    pub async fn connect(postgres: &str, schema: &str) -> Result<Self> {
        let catalog = Catalog::connect(CatalogConfig::new(postgres, schema)).await?;
        let main = catalog.branch("main").await?;
        Ok(Self { catalog, main })
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
        const CREDENTIALS: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];
        let warehouse = self
            .catalog
            .warehouse()
            .ok_or_else(|| Error::Location("the catalog has no warehouse".into()))?;
        let options =
            std::env::vars_os().filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
        let options = options
            .filter(|(key, _)| CREDENTIALS.iter().any(|prefix| key.starts_with(prefix)))
            .map(|(key, value)| (key.to_ascii_lowercase(), value));
        Storage::new(warehouse, options)
    }

    pub(crate) fn main(&self) -> &Ref {
        &self.main
    }

    pub(crate) fn close(&self) {
        self.catalog.close();
    }
}
