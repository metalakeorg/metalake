//! `metalake iceberg`: when Iceberg clients find each table's metadata written (REST-9). Every table is served to them
//! and written when one loads it; a table set `on-commit` on a ref is written by the Iceberg service after each commit.

use std::path::Path;

use serde_json::json;

use crate::args::{IcebergServe, IcebergTables, IcebergVerb, Switch};
use crate::out::Out;
use metalake_client::server::{self, Runtime, SHUTDOWN_TOKEN, VERSION};
use metalake_client::{Error, Lake};
use metalake_iceberg::State;
use tokio::net::TcpListener;

pub async fn run(verb: &IcebergVerb, lake: &Lake, out: &Out) -> Result<(), Error> {
    let iceberg = lake.iceberg();
    let (args, when, commit) = match verb {
        IcebergVerb::Serve(args) => return serve(args, lake).await,
        IcebergVerb::Stop => unreachable!("stopping reaches no catalog: see `stop`"),
        IcebergVerb::Status { on } => {
            let columns = ["table", "head", "written", "behind", "since", "metadata"];
            return out.records(&columns, &iceberg.status(on).await?).await;
        }
        IcebergVerb::OnCommit(args) => (args, "on-commit", iceberg.on_commit(&args.on, &names(args)).await?),
        IcebergVerb::OnLoad(args) => (args, "on-load", iceberg.on_load(&args.on, &names(args)).await?),
    };
    let done = json!({"ref": args.on, "metadata": when, "tables": args.tables.join(", "), "commit": commit});
    out.records(&["ref", "metadata", "tables", "commit"], &[done]).await
}

/// The lake's Iceberg REST server, until Ctrl-C, the signal a service manager stops a process with, or, given a token,
/// a request to `POST /metalake/shutdown` that bears it. With a runtime file, it says there where it listens once it
/// does, and removes it as it stops.
async fn serve(args: &IcebergServe, lake: &Lake) -> Result<(), Error> {
    let mut state = State::connect(&lake.catalog_postgres()?, lake.schema()).await?;
    if args.materialize == Switch::On {
        state = state.materializing();
    }
    let listener = TcpListener::bind(&args.listen).await?;
    let address = listener.local_addr()?;
    let token = std::env::var(SHUTDOWN_TOKEN).ok().filter(|token| !token.is_empty());
    let pid = std::process::id();
    if let Some(path) = &args.runtime_file {
        let token = token
            .clone()
            .ok_or_else(|| Error::usage(format!("a runtime file needs a shutdown token in {SHUTDOWN_TOKEN}")))?;
        let runtime = Runtime {
            url: format!("http://{address}"),
            port: address.port(),
            pid,
            version: VERSION.to_string(),
            token,
        };
        runtime.write(path)?;
    }
    // Once its file says so: a caller that reads this line finds the file.
    println!("listening on {address}");
    let served = metalake_iceberg::serve_with(listener, state, stopped(), token.as_deref()).await;
    if let Some(path) = &args.runtime_file {
        server::remove_if_mine(path, pid);
    }
    Ok(served?)
}

/// Stops the lake's server on this machine, if one runs.
pub async fn stop(dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let stopped = Lake::stop_server(dir)?;
    out.records(&["stopped"], &[json!({"stopped": stopped})]).await
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

fn names(args: &IcebergTables) -> Vec<&str> {
    args.tables.iter().map(String::as_str).collect()
}
