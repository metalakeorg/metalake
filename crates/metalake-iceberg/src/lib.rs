//! MetaLake's Iceberg REST server: what an Iceberg client asks of a catalog, answered from the catalog's rows, and what it
//! commits turned into one commit of the protocol (§16 of the specification). Postgres decides, and this translates.

pub mod apply;
mod footers;
mod load;
mod materialize;
pub mod project;
mod rest;
mod state;
pub mod wire;

use std::future::Future;

pub use state::State;
use tokio::net::TcpListener;

/// Answers on `listener` until `stop` ends, lets what is running finish, and the footers queued for a while, and closes
/// the catalog's connections.
pub async fn serve(
    listener: TcpListener,
    state: State,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let served = axum::serve(listener, rest::router(state.clone()))
        .with_graceful_shutdown(stop)
        .await;
    state.close().await;
    served
}
