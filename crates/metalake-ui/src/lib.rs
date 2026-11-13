//! MetaLake's UI (ADR 0007): a face of the lake, as the command line and Python are. An axum router that serves the
//! frontend embedded in it, which `metalake ui` runs on the loopback in the foreground ([`serve`]).
//!
//! The frontend is built apart, by `pnpm build` in `web/`, and embedded when the crate is compiled; a crate compiled
//! without it serves a page that says how to build it ([`built`] says which).

mod assets;
pub mod browser;
mod frontend;
#[cfg(test)]
mod tests;

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use assets::{Dist, Files};

/// How long requests in flight have to finish once the UI is told to stop. A browser keeps idle connections open, and
/// none of them may hold the process past it.
pub const GRACE: Duration = Duration::from_secs(5);

/// The UI's router: the frontend, and, under `/api`, nothing yet.
pub fn router() -> Router {
    router_with(Arc::new(Dist))
}

/// Whether the frontend was embedded when this crate was compiled.
pub fn built() -> bool {
    Dist.built()
}

/// Serves [`router`] on `listener` until `stop` resolves, then lets requests in flight finish for at most [`GRACE`].
pub async fn serve(listener: TcpListener, stop: impl Future<Output = ()> + Send + 'static) -> std::io::Result<()> {
    serve_router(listener, router(), stop, GRACE).await
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

/// The router over `files`, which a test gives in place of the embedded ones.
pub(crate) fn router_with(files: Arc<dyn Files>) -> Router {
    Router::new().fallback(frontend::serve).with_state(files)
}
