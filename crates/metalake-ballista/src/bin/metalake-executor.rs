//! An executor of MetaLake's plans on Ballista, configured by the environment: `METALAKE_SCHEDULER` (`df://host:port`),
//! `METALAKE_WAREHOUSE` and its credentials (`AWS_*`, `GOOGLE_*`, `AZURE_*`), `METALAKE_POSTGRES` only to write,
//! `METALAKE_EXECUTOR_BIND`, `METALAKE_EXECUTOR_HOST` (the name it is reached by), `METALAKE_EXECUTOR_PORT` (50051),
//! `METALAKE_EXECUTOR_GRPC_PORT` (50052), `METALAKE_EXECUTOR_SLOTS` (its cores) and `METALAKE_EXECUTOR_WORK_DIR`.
//! `RUST_LOG` sets what it logs. It stops on a signal, or when the scheduler asks.

use metalake_ballista::serve::{ExecutorSettings, run_executor};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let run = async { run_executor(ExecutorSettings::from_env()?).await };
    match run.await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            log::error!("metalake-executor: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
