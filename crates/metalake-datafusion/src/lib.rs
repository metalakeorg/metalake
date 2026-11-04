//! MetaLake in DataFusion. A translation, and nothing more: every rule stays in the protocol, every computation in the
//! core. A ref is a catalog, a namespace a schema, and a table is planned by the protocol when DataFusion asks for it.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use metalake::{Catalog, CatalogConfig, Storage};
//! use metalake_datafusion::MetaLake;
//!
//! let catalog = Catalog::connect(CatalogConfig::new("host=localhost user=postgres", "lake")).await?;
//! let storage = Storage::new("gs://lake", [("google_service_account", "key.json")])?;
//! let context = MetaLake::new(catalog, storage).await?.context()?;
//! context.sql("select count(*) from main.db.events").await?.show().await?;
//! # Ok(())
//! # }
//! ```

mod adapter;
mod catalog;
mod filters;
mod merge;
mod point;
mod read;
mod sink;
mod stats;
mod table;

pub use catalog::MetaLake;
pub use table::MetaTable;
