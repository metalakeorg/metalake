//! The server, as a process: where the catalog is, where to listen, and how to stop.

use std::process::ExitCode;

use clap::Parser;
use metalake_iceberg::{State, serve};
use tokio::net::TcpListener;

/// MetaLake's Iceberg REST server.
#[derive(Parser)]
#[command(name = "metalake-iceberg", version)]
struct Args {
    /// The catalog's Postgres: a libpq connection string or URL.
    #[arg(long, env = "METALAKE_POSTGRES", hide_env_values = true)]
    postgres: String,
    /// The catalog's schema in it.
    #[arg(long, default_value = "metalake")]
    schema: String,
    /// Where to listen; port 0 takes a free one, which the first line says.
    #[arg(long, default_value = "127.0.0.1:8181")]
    listen: String,
}

/// Ctrl-C, or, where there is one, the signal a service manager stops a process with.
async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("a signal handler installs");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let run = async {
        let state = State::connect(&args.postgres, &args.schema).await?;
        let listener = TcpListener::bind(&args.listen).await?;
        println!("listening on {}", listener.local_addr()?);
        serve(listener, state, stopped()).await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    match run.await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("metalake-iceberg: {error}");
            ExitCode::FAILURE
        }
    }
}
