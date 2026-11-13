//! `metalake iceberg`: when Iceberg clients find each table's metadata written (REST-9). Every table is served to them
//! and written when one loads it; a table set `on-commit` on a ref is written by the Iceberg service after each commit.

use std::path::Path;

use serde_json::json;

use crate::args::{IcebergConnect, IcebergServe, IcebergTables, IcebergVerb, Switch};
use crate::out::Out;
use metalake_client::server::{self, Runtime, SHUTDOWN_TOKEN, VERSION};
use metalake_client::{Error, Lake};
use metalake_iceberg::State;
use tokio::net::TcpListener;

pub async fn run(verb: &IcebergVerb, lake: &Lake, out: &Out) -> Result<(), Error> {
    let iceberg = lake.iceberg();
    let (args, when, commit) = match verb {
        IcebergVerb::Serve(args) => return serve(args, lake).await,
        IcebergVerb::Connect(args) => return connect(args, lake, out).await,
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
    let mut state = State::connect_with(&lake.catalog_postgres()?, lake.schema(), lake.catalog_auth()?).await?;
    if args.materialize == Switch::On {
        state = state.materializing();
    }
    let listener = match TcpListener::bind(&args.listen).await {
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && args.or_any_port => {
            let taken: std::net::SocketAddr = args.listen.parse().map_err(|_| e)?;
            TcpListener::bind((taken.ip(), 0)).await?
        }
        bound => bound?,
    };
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
            // Which start of it this is, as the client that started it says.
            start: std::env::var(server::START).unwrap_or_default(),
            auth: lake.catalog_auth()?.name().to_string(),
        };
        runtime.write(path)?;
    }
    // Once its file says so: a caller that reads this line finds the file.
    println!("listening on {address}");
    let served = metalake_iceberg::serve_with(listener, state, super::signal::stopped(), token.as_deref()).await;
    if let Some(path) = &args.runtime_file {
        server::remove_if_mine(path, pid);
    }
    Ok(served?)
}

/// An engine's profile for the lake, printed; where the server runs, said apart, so that the profile alone is what a
/// redirection writes.
async fn connect(args: &IcebergConnect, lake: &Lake, out: &Out) -> Result<(), Error> {
    let profile = lake.iceberg().profile(args.engine.into(), &args.name).await?;
    eprintln!(
        "the lake's server runs at {}: `metalake iceberg stop` stops it",
        profile.target.uri
    );
    out.value(&profile)
}

/// Stops the lake's server on this machine, if one runs.
pub async fn stop(dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let stopped = Lake::stop_server(dir)?;
    out.records(&["stopped"], &[json!({"stopped": stopped})]).await
}

fn names(args: &IcebergTables) -> Vec<&str> {
    args.tables.iter().map(String::as_str).collect()
}
