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

use crate::router::Router;

/// A session of `lake` that reaches the Ballista scheduler at `scheduler` (`df://host:port`): its statements run there,
/// or where the session is, as `metalake.engine` says.
pub async fn session(lake: &MetaLake, scheduler: &str) -> Result<SessionContext> {
    let state = with_codecs(lake, lake.context()?.state());
    let local = state.query_planner().clone();
    let remote = SessionContext::remote_with_state(scheduler, state).await?.state();
    let router = Router::new(local, remote.query_planner().clone());
    let state = SessionStateBuilder::new_from_existing(remote)
        .with_query_planner(Arc::new(router))
        .build();
    Ok(SessionContext::new_with_state(state))
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
