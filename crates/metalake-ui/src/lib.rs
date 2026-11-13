//! MetaLake's UI (ADR 0007): a face of the lake, as the command line and Python are. An axum router that serves the
//! frontend embedded in it, which `metalake ui` runs on the loopback in the foreground ([`serve`]).
//!
//! The frontend is built apart, by `pnpm build` in `web/`, and embedded when the crate is compiled; a crate compiled
//! without it serves a page that says how to build it ([`built`] says which).

pub mod api;
mod assets;
pub mod browser;
mod frontend;
mod guard;
#[cfg(test)]
mod tests;
pub mod token;

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use assets::{Dist, Files};
use guard::Access;
pub use token::Token;

/// How long requests in flight have to finish once the UI is told to stop. A browser keeps idle connections open, and
/// none of them may hold the process past it.
pub const GRACE: Duration = Duration::from_secs(5);

/// The UI's router on `port`, which only the run's `token` opens (ADR 0007 §4.2): the frontend, and its API under
/// `/api`.
pub fn router(token: Token, port: u16) -> Router {
    router_with(Arc::new(Dist), Access { token, port })
}

/// The URL a run of the UI on `address` is opened at: its token in the fragment, which a browser never sends to a
/// server, so that it is in no log and no `Referer`; the frontend keeps it and takes it out of the address bar.
pub fn url(address: std::net::SocketAddr, token: &Token) -> String {
    format!("http://{address}/#token={}", token.as_str())
}

/// Whether the frontend was embedded when this crate was compiled.
pub fn built() -> bool {
    Dist.built()
}

/// Serves [`router`] on `listener`, to the bearers of `token`, until `stop` resolves; then lets requests in flight
/// finish for at most [`GRACE`].
pub async fn serve(
    listener: TcpListener,
    token: Token,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let port = listener.local_addr()?.port();
    serve_router(listener, router(token, port), stop, GRACE).await
}

/// As [`serve`], over `router` and with `grace`, which a test gives.
pub(crate) async fn serve_router(
    listener: TcpListener,
    router: Router,
    stop: impl Future<Output = ()> + Send + 'static,
    grace: Duration,
) -> std::io::Result<()> {
    let (stopping, stopped) = oneshot::channel::<()>();
    let stop = async move {
        stop.await;
        let _ = stopping.send(());
    };
    let served = axum::serve(listener, router).with_graceful_shutdown(stop).into_future();
    // The grace starts once `stop` resolves; a sender dropped unsent means the server ended first, which `served` says.
    let grace = async {
        if stopped.await.is_ok() {
            tokio::time::sleep(grace).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        served = served => served,
        () = grace => Ok(()),
    }
}

/// The router over `files`, which a test gives in place of the embedded ones, guarded by `access`. Layers run from the
/// last added: the headers wrap every answer, refusals included; then the `Host`; then, for every path under `/api` by
/// its prefix, whatever route it reaches, the token and the `Origin`.
pub(crate) fn router_with(files: Arc<dyn Files>, access: Access) -> Router {
    let access = Arc::new(access);
    Router::new()
        .nest("/api", api::router())
        .merge(frontend_router(files))
        .layer(axum::middleware::from_fn_with_state(access.clone(), guard::origin))
        .layer(axum::middleware::from_fn_with_state(access.clone(), guard::bearer))
        .layer(axum::middleware::from_fn_with_state(access, guard::host))
        .layer(axum::middleware::map_response(guard::secured))
}

/// The frontend alone, unguarded: every path the API does not have.
pub(crate) fn frontend_router(files: Arc<dyn Files>) -> Router {
    Router::new().fallback(frontend::serve).with_state(files)
}
