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
mod changes;
#[cfg(feature = "proto")]
mod codec;
mod ddl;
mod details;
mod filters;
mod measure;
mod merge;
mod options;
mod point;
mod read;
mod sink;
mod source;
mod stats;
mod table;
mod written;

pub use catalog::{MetaLake, Source};
#[cfg(feature = "proto")]
pub use codec::{MetaLakeLogicalCodec, MetaLakePhysicalCodec};
pub use measure::{Measure, measure};
pub use options::{Engine, MetaLakeOptions};
pub use written::{WrittenRow, written_schema};
pub use table::MetaTable;
