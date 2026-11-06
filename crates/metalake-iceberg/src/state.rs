//! What every request shares: the catalog, reached once, and the ref an Iceberg catalog's tables are read on.

use metalake::{Catalog, CatalogConfig, Ref, Result};

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

    pub(crate) fn main(&self) -> &Ref {
        &self.main
    }

    pub(crate) fn close(&self) {
        self.catalog.close();
    }
}
