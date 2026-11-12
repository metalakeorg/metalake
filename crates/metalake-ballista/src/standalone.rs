//! A Ballista cluster in this process: a scheduler with a catalog of its own and an executor with storage and nothing
//! registered, as a real cluster's, talking gRPC and Arrow Flight on local ports. Ballista's own `standalone_with_state`
//! would give both the client's session, catalog and store already there, and prove nothing of the processes apart.

use std::time::Duration;

use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_executor::new_standalone_executor_from_builder;
use ballista_scheduler::standalone::new_standalone_scheduler_with_builder;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use metalake::Storage;
use metalake_datafusion::MetaLake;

use crate::{client, executor, scheduler};

/// How many tasks the executor runs at once.
const SLOTS: usize = 4;

/// A session of `client` on a cluster in this process: a scheduler planning on `scheduler`, a lake connected apart,
/// and an executor reading with `executor`.
pub async fn standalone(client: &MetaLake, scheduler: MetaLake, executor: Storage) -> Result<SessionContext> {
    let parts = scheduler::scheduler(scheduler)?;
    let address = new_standalone_scheduler_with_builder(parts.session_builder, parts.config_producer, parts.codec)
        .await
        .map_err(external)?;
    let url = format!("http://localhost:{}", address.port());
    let grpc = loop {
        match SchedulerGrpcClient::connect(url.clone()).await {
            Ok(grpc) => break grpc,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };

    let parts = executor::executor(executor)?;
    new_standalone_executor_from_builder(
        grpc,
        SLOTS,
        parts.config_producer,
        parts.runtime_producer,
        parts.codec,
        BallistaFunctionRegistry::default(),
    )
    .await
    .map_err(external)?;

    client::session(client, &format!("df://localhost:{}", address.port())).await
}

fn external(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
