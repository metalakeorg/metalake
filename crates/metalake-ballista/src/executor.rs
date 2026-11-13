//! An executor of MetaLake's plans: storage, and a catalog only to write. It decodes each stage's MetaLake scans with the
//! lake's storage, under its own identity, and reads without asking Postgres; a write's sink commits where it runs.

use std::sync::Arc;

use ballista_core::serde::{BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use ballista_core::utils::default_config_producer;
use ballista_core::{ConfigProducer, RuntimeProducer};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use metalake::{Catalog, Storage};
use metalake_datafusion::{MetaLakeOptions, MetaLakePhysicalCodec};

/// What a Ballista executor is made of for a lake: its sessions' configuration, a runtime with the lake's store, and
/// the codec that decodes MetaLake's scans. As `ExecutorProcessConfig`'s `override_config_producer`,
/// `override_runtime_producer` and `override_physical_codec` take them, or a standalone executor.
pub struct ExecutorParts {
    pub config_producer: ConfigProducer,
    pub runtime_producer: RuntimeProducer,
    pub codec: BallistaCodec,
}

/// The parts of an executor that reads the lake of `storage` and, with `catalog`, also writes it: a write's sink
/// commits where it runs (milestone 4's first cut). Without one, it refuses a write's plan.
pub fn executor(storage: Storage, catalog: Option<Catalog>) -> Result<ExecutorParts> {
    let url = ObjectStoreUrl::parse(storage.base())?;
    let store = storage.store().clone();
    let runtime_producer: RuntimeProducer = Arc::new(move |_| {
        let runtime = RuntimeEnvBuilder::new().build()?;
        runtime.register_object_store(url.as_ref(), store.clone());
        Ok(Arc::new(runtime))
    });
    let mut physical = MetaLakePhysicalCodec::new(storage, Arc::new(BallistaPhysicalExtensionCodec::default()));
    if let Some(catalog) = catalog {
        physical = physical.with_catalog(catalog);
    }
    Ok(ExecutorParts {
        // MetaLake's options known, so that a job's are taken and not dropped.
        config_producer: Arc::new(|| MetaLakeOptions::register(default_config_producer())),
        runtime_producer,
        codec: BallistaCodec::new(Arc::new(BallistaLogicalExtensionCodec::default()), Arc::new(physical)),
    })
}
