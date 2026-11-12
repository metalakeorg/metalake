//! A Ballista cluster in this process: a scheduler with a catalog of its own and an executor with storage and nothing
//! registered, as a real cluster's, talking gRPC and Arrow Flight on local ports. Ballista's own `standalone_with_state`
//! would give both the client's session, catalog and store already there, and prove nothing of the processes apart.
//!
//! Ballista's servers run until their runtime ends, and give no handle to stop them: they run in a runtime of the
//! cluster's own, which ends when the cluster is dropped, or when starting it fails half way.

use std::ops::Deref;
use std::time::Duration;

use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_executor::new_standalone_executor_from_builder;
use ballista_scheduler::standalone::new_standalone_scheduler_with_builder;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use metalake::Storage;
use metalake_datafusion::MetaLake;
use tokio::runtime::Runtime;

use crate::{client, executor, scheduler};

/// How many tasks the executor runs at once.
const SLOTS: usize = 4;

/// A cluster in this process and a session of it. It derefs to its session; dropped, its servers stop.
pub struct Standalone {
    session: SessionContext,
    scheduler: String,
    _servers: Servers,
}

/// The runtime the servers run in, which ends when this is dropped: without blocking, as it may be dropped inside
/// another runtime, where a runtime's own drop would panic.
struct Servers(Option<Runtime>);

impl Drop for Servers {
    fn drop(&mut self) {
        if let Some(servers) = self.0.take() {
            servers.shutdown_background();
        }
    }
}

impl Standalone {
    /// The scheduler's address, `df://localhost:port`.
    pub fn scheduler(&self) -> &str {
        &self.scheduler
    }
}

impl Deref for Standalone {
    type Target = SessionContext;

    fn deref(&self) -> &SessionContext {
        &self.session
    }
}

/// A session of `client` on a cluster in this process: a scheduler planning on `scheduler`, a lake connected apart,
/// and an executor reading with `executor`.
pub async fn standalone(client: &MetaLake, scheduler: MetaLake, executor: Storage) -> Result<Standalone> {
    let servers = Servers(Some(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("metalake-standalone")
            .enable_all()
            .build()
            .map_err(external)?,
    ));
    let scheduler_parts = scheduler::scheduler(scheduler)?;
    let executor_parts = executor::executor(executor)?;
    let started = servers.0.as_ref().expect("just built").spawn(async move {
        let address = new_standalone_scheduler_with_builder(
            scheduler_parts.session_builder,
            scheduler_parts.config_producer,
            scheduler_parts.codec,
        )
        .await
        .map_err(external)?;
        let url = format!("http://localhost:{}", address.port());
        let grpc = loop {
            match SchedulerGrpcClient::connect(url.clone()).await {
                Ok(grpc) => break grpc,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        new_standalone_executor_from_builder(
            grpc,
            SLOTS,
            executor_parts.config_producer,
            executor_parts.runtime_producer,
            executor_parts.codec,
            BallistaFunctionRegistry::default(),
        )
        .await
        .map_err(external)?;
        Ok::<_, DataFusionError>(address)
    });
    // Should this fail, `servers` is dropped on the way out, and what started stops with it.
    let address = match started.await {
        Ok(address) => address?,
        Err(e) => return Err(external(e)),
    };
    let scheduler = format!("df://localhost:{}", address.port());
    let session = client::session(client, &scheduler).await?;
    Ok(Standalone {
        session,
        scheduler,
        _servers: servers,
    })
}

fn external(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
