//! MetaLake's Iceberg REST server: what an Iceberg client asks of a catalog, answered from the catalog's rows, and what it
//! commits turned into one commit of the protocol (§16 of the specification). Postgres decides, and this translates.

mod rest;
mod state;

use std::future::Future;

pub use state::State;
use tokio::net::TcpListener;

/// Answers on `listener` until `stop` ends, lets what is running finish, and closes the catalog's connections.
pub async fn serve(
    listener: TcpListener,
    state: State,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let served = axum::serve(listener, rest::router(state.clone()))
        .with_graceful_shutdown(stop)
        .await;
    state.close();
    served
}
