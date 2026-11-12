//! A scheduler of MetaLake's plans: a catalog of its own. It decodes a client's logical plan, MetaLake's tables as
//! values rebuilt with its catalog and storage, plans it (the protocol plans each scan's files here), and encodes each
//! stage's physical plan, MetaLake's scans with their tasks.

use std::sync::Arc;

use ballista_core::ConfigProducer;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::{BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use ballista_scheduler::scheduler_server::SessionBuilder;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use metalake_datafusion::MetaLake;

/// What a Ballista scheduler is made of for a lake: a session per job with the lake's catalogs, its sessions'
/// configuration, and the codecs of MetaLake's plans. As `SchedulerConfig`'s `override_session_builder`,
/// `override_config_producer`, `override_logical_codec` and `override_physical_codec` take them, or a standalone
/// scheduler.
pub struct SchedulerParts {
    pub session_builder: SessionBuilder,
    pub config_producer: ConfigProducer,
    pub codec: BallistaCodec,
}

/// The parts of a scheduler that plans on `lake`.
pub fn scheduler(lake: MetaLake) -> Result<SchedulerParts> {
    // The lake's own session: refs as catalogs, one point per statement, its options and store.
    let base = lake.context()?.state();
    let config = base.config().clone().upgrade_for_ballista();
    let session_builder: SessionBuilder = Arc::new(move |config| {
        Ok(SessionStateBuilder::new_from_existing(base.clone())
            .with_config(config)
            .build())
    });
    let logical = lake.logical_codec(Arc::new(BallistaLogicalExtensionCodec::default()));
    let physical = lake.physical_codec(Arc::new(BallistaPhysicalExtensionCodec::default()));
    Ok(SchedulerParts {
        session_builder,
        config_producer: Arc::new(move || config.clone()),
        codec: BallistaCodec::new(Arc::new(logical), Arc::new(physical)),
    })
}
