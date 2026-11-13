//! A scheduler and an executor of a lake as processes of their own, configured by the environment alone, as the SDK is:
//! what the binaries `metalake-scheduler` and `metalake-executor` run. Nothing here knows where they are deployed.
//!
//! The scheduler is the cluster's one entry point: it plans with the lake's catalog, and serves the results of a job
//! itself, so that a client reaches the cluster by the scheduler's address alone and executors may live where it
//! cannot reach them.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use datafusion::error::{DataFusionError, Result};
use metalake::environment::{self, storage_options, with_password};
use metalake::{Catalog, CatalogConfig, Storage};
use metalake_datafusion::MetaLake;

use crate::{address, executor, scheduler};

/// Where the scheduler listens, unless `METALAKE_SCHEDULER_BIND` says.
const DEFAULT_SCHEDULER_BIND: &str = "0.0.0.0:50050";
/// Where an executor listens, unless `METALAKE_EXECUTOR_BIND` says.
const DEFAULT_EXECUTOR_BIND: &str = "0.0.0.0";
/// An executor's ports: Arrow Flight, for the results of its tasks, and gRPC.
const DEFAULT_EXECUTOR_PORT: u16 = 50051;
const DEFAULT_EXECUTOR_GRPC_PORT: u16 = 50052;

/// The scheduler's address the binaries also take: `df://host:port`, as the SDK's `METALAKE_SCHEDULER`.
pub const SCHEDULER: &str = environment::SCHEDULER;
/// Where the scheduler listens: `host:port`.
pub const SCHEDULER_BIND: &str = "METALAKE_SCHEDULER_BIND";
/// The name the scheduler is reached by, which it gives executors and clients.
pub const SCHEDULER_HOST: &str = "METALAKE_SCHEDULER_HOST";
/// The lake's warehouse, which an executor without Postgres cannot ask the catalog for.
pub const WAREHOUSE: &str = "METALAKE_WAREHOUSE";
/// Where an executor listens: a host.
pub const EXECUTOR_BIND_HOST: &str = "METALAKE_EXECUTOR_BIND";
/// The name an executor is reached by, which it gives the scheduler.
pub const EXECUTOR_HOST: &str = "METALAKE_EXECUTOR_HOST";
/// An executor's Arrow Flight port and gRPC port.
pub const EXECUTOR_PORT: &str = "METALAKE_EXECUTOR_PORT";
pub const EXECUTOR_GRPC_PORT: &str = "METALAKE_EXECUTOR_GRPC_PORT";
/// How many tasks an executor runs at once: its cores, unless this says.
pub const EXECUTOR_SLOTS: &str = "METALAKE_EXECUTOR_SLOTS";
/// Where an executor keeps the files a shuffle writes.
pub const EXECUTOR_WORK_DIR: &str = "METALAKE_EXECUTOR_WORK_DIR";

/// What a scheduler is started with.
#[derive(Debug, Clone)]
pub struct SchedulerSettings {
    pub catalog: CatalogConfig,
    pub bind: SocketAddr,
    pub host: String,
}

impl SchedulerSettings {
    /// From the environment: `METALAKE_POSTGRES` (and `PGPASSWORD`, `METALAKE_SCHEMA`), where it listens, and the name
    /// it is reached by.
    pub fn from_env() -> Result<Self> {
        let bind = var(SCHEDULER_BIND).unwrap_or_else(|| DEFAULT_SCHEDULER_BIND.into());
        let bind: SocketAddr = parsed(SCHEDULER_BIND, &bind)?;
        Ok(Self {
            catalog: catalog().ok_or_else(|| missing(environment::POSTGRES, "the scheduler plans with the catalog"))?,
            bind,
            host: var(SCHEDULER_HOST).unwrap_or_else(|| "localhost".into()),
        })
    }
}

/// What an executor is started with.
#[derive(Debug, Clone)]
pub struct ExecutorSettings {
    /// The scheduler it registers with: host and port.
    pub scheduler: (String, u16),
    /// The warehouse it reads; none, the catalog's, when it has one.
    pub warehouse: Option<String>,
    /// The catalog it commits with, if it writes (milestone 7 takes it away).
    pub catalog: Option<CatalogConfig>,
    pub bind_host: String,
    pub host: Option<String>,
    pub port: u16,
    pub grpc_port: u16,
    pub slots: usize,
    pub work_dir: Option<PathBuf>,
}

impl ExecutorSettings {
    /// From the environment: `METALAKE_SCHEDULER`, `METALAKE_WAREHOUSE` or `METALAKE_POSTGRES` (both when it writes),
    /// where it listens, the name it is reached by, its slots and work directory.
    pub fn from_env() -> Result<Self> {
        let scheduler = var(SCHEDULER).ok_or_else(|| missing(SCHEDULER, "an executor registers with the scheduler"))?;
        let catalog = catalog();
        let warehouse = var(WAREHOUSE);
        if warehouse.is_none() && catalog.is_none() {
            return Err(missing(WAREHOUSE, "an executor without Postgres reads the warehouse it names"));
        }
        let number = |name: &str, default: usize| match var(name) {
            Some(value) => parsed::<usize>(name, &value),
            None => Ok(default),
        };
        let port = |name: &str, default: u16| match var(name) {
            Some(value) => parsed::<u16>(name, &value),
            None => Ok(default),
        };
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Ok(Self {
            scheduler: address::host_port(&scheduler)?,
            warehouse,
            catalog,
            bind_host: var(EXECUTOR_BIND_HOST).unwrap_or_else(|| DEFAULT_EXECUTOR_BIND.into()),
            host: var(EXECUTOR_HOST),
            port: port(EXECUTOR_PORT, DEFAULT_EXECUTOR_PORT)?,
            grpc_port: port(EXECUTOR_GRPC_PORT, DEFAULT_EXECUTOR_GRPC_PORT)?,
            slots: number(EXECUTOR_SLOTS, cores)?,
            work_dir: var(EXECUTOR_WORK_DIR).map(PathBuf::from),
        })
    }
}

/// Runs a scheduler until its process ends.
pub async fn run_scheduler(settings: SchedulerSettings) -> Result<()> {
    let catalog = Catalog::connect(settings.catalog).await.map_err(external)?;
    let warehouse = catalog
        .warehouse()
        .ok_or_else(|| DataFusionError::Configuration("the catalog has no warehouse".into()))?
        .to_string();
    let storage = storage(&warehouse)?;
    let lake = MetaLake::new(catalog, storage).await.map_err(external)?;
    let parts = scheduler::scheduler(lake)?;
    let mut config = SchedulerConfig::default()
        .with_hostname(settings.host)
        .with_port(settings.bind.port())
        .with_enable_embedded_flight_proxy(true)
        .with_override_config_producer(parts.config_producer)
        .with_override_session_builder(parts.session_builder);
    config.bind_host = settings.bind.ip().to_string();
    config.override_logical_codec = Some(parts.logical);
    config.override_physical_codec = Some(parts.physical);
    let cluster = BallistaCluster::new_from_config(&config).await.map_err(external)?;
    start_server(cluster, settings.bind, Arc::new(config)).await.map_err(external)
}

/// Runs an executor until its process ends: a signal, or the scheduler asks it to stop.
pub async fn run_executor(settings: ExecutorSettings) -> Result<()> {
    let catalog = match settings.catalog {
        Some(config) => Some(Catalog::connect(config).await.map_err(external)?),
        None => None,
    };
    let warehouse = match (&settings.warehouse, &catalog) {
        (Some(warehouse), _) => warehouse.clone(),
        (None, Some(catalog)) => catalog
            .warehouse()
            .ok_or_else(|| DataFusionError::Configuration("the catalog has no warehouse".into()))?
            .to_string(),
        (None, None) => unreachable!("settings name a warehouse or a catalog"),
    };
    let parts = executor::executor(storage(&warehouse)?, catalog)?;
    let config = ExecutorProcessConfig {
        scheduler_host: settings.scheduler.0,
        scheduler_port: settings.scheduler.1,
        bind_host: settings.bind_host,
        external_host: settings.host,
        port: settings.port,
        grpc_port: settings.grpc_port,
        vcores: settings.slots,
        work_dir: settings.work_dir.map(|d| d.to_string_lossy().into_owned()),
        override_config_producer: Some(parts.config_producer),
        override_runtime_producer: Some(parts.runtime_producer),
        override_physical_codec: Some(parts.physical),
        ..ExecutorProcessConfig::default()
    };
    start_executor_process(Arc::new(config)).await.map_err(external)
}

/// The catalog the environment names, with its password.
fn catalog() -> Option<CatalogConfig> {
    let postgres = var(environment::POSTGRES)?;
    let postgres = with_password(&postgres, var(environment::PASSWORD).as_deref());
    let schema = var(environment::SCHEMA).unwrap_or_else(|| environment::DEFAULT_SCHEMA.into());
    Some(CatalogConfig::new(postgres, &schema))
}

/// The store of `warehouse`, with the environment's credentials.
fn storage(warehouse: &str) -> Result<Storage> {
    Storage::new(warehouse, storage_options(env::vars_os())).map_err(external)
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

fn parsed<T: std::str::FromStr>(name: &str, value: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|e| DataFusionError::Configuration(format!("{name} {value}: {e}")))
}

fn missing(name: &str, why: &str) -> DataFusionError {
    DataFusionError::Configuration(format!("{name} is not set: {why}"))
}

fn external(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
