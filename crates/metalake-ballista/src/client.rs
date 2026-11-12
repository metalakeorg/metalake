//! A client of a Ballista cluster over a lake: the lake's own session, whose statements go to the scheduler. The same
//! SQL, catalogs and tables as a local session; its logical plans carry MetaLake's tables as values.

use std::sync::Arc;

use ballista::extension::SessionContextExt;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::error::Result;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionContext;
use metalake_datafusion::MetaLake;

/// A session of `lake` whose statements run on the Ballista scheduler at `scheduler` (`df://host:port`).
pub async fn session(lake: &MetaLake, scheduler: &str) -> Result<SessionContext> {
    SessionContext::remote_with_state(scheduler, with_codecs(lake, lake.context()?.state())).await
}

/// `state` with MetaLake's codecs, each over Ballista's own.
pub(crate) fn with_codecs(lake: &MetaLake, state: SessionState) -> SessionState {
    let logical = lake.logical_codec(Arc::new(BallistaLogicalExtensionCodec::default()));
    let physical = lake.physical_codec(Arc::new(BallistaPhysicalExtensionCodec::default()));
    let config = state
        .config()
        .clone()
        .with_ballista_logical_extension_codec(Arc::new(logical))
        .with_ballista_physical_extension_codec(Arc::new(physical));
    SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build()
}
