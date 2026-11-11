//! MetaLake's Iceberg REST server: what an Iceberg client asks of a catalog, answered from the catalog's rows, and what it
//! commits turned into one commit of the protocol (§16 of the specification). Postgres decides, and this translates.

pub mod apply;
pub mod caller;
mod footers;
mod generations;
mod load;
mod materialize;
pub mod project;
mod prune;
mod rest;
pub mod shutdown;
mod state;
pub mod vend;
pub mod wire;

use std::future::Future;
use std::sync::Arc;

pub use state::State;
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// Answers on `listener` until `stop` ends, lets what is running finish, and the footers queued for a while, and closes
/// the catalog's connections.
pub async fn serve(
    listener: TcpListener,
    state: State,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_with(listener, state, stop, None).await
}

/// As [`serve`], and, given a token, until a request bearing it reaches [`shutdown::PATH`]: how the process that started
/// the server stops it.
pub async fn serve_with(
    listener: TcpListener,
    state: State,
    stop: impl Future<Output = ()> + Send + 'static,
    shutdown_token: Option<&str>,
) -> std::io::Result<()> {
    let shut = Arc::new(Notify::new());
    let mut app = rest::router(state.clone());
    if let Some(token) = shutdown_token {
        // The token stops the server, and makes a request that bears it a writer's (REST-11).
        app = shutdown::router(token, shut.clone()).merge(app);
        let token: Arc<str> = token.into();
        app = app.layer(axum::middleware::from_fn_with_state(token, caller::identify));
    }
    let stopped = async move {
        tokio::select! {
            () = stop => {}
            () = shut.notified() => {}
        }
    };
    let served = axum::serve(listener, app).with_graceful_shutdown(stopped).await;
    state.close().await;
    served
}
