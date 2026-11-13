//! A scheduler of MetaLake's plans on Ballista, configured by the environment: `METALAKE_POSTGRES` (and `PGPASSWORD`,
//! `METALAKE_SCHEMA`), the warehouse's credentials (`AWS_*`, `GOOGLE_*`, `AZURE_*`), `METALAKE_SCHEDULER_BIND`
//! (`0.0.0.0:50050`) and `METALAKE_SCHEDULER_HOST`, the name it is reached by. `RUST_LOG` sets what it logs.

use metalake_ballista::serve::{SchedulerSettings, run_scheduler};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let run = async { run_scheduler(SchedulerSettings::from_env()?).await };
    match run.await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            log::error!("metalake-scheduler: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
