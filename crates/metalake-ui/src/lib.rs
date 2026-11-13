//! MetaLake's UI (ADR 0007): a face of the lake, as the command line and Python are. An axum router that serves the
//! frontend embedded in it, which `metalake ui` runs on the loopback.
//!
//! The frontend is built apart, by `pnpm build` in `web/`, and embedded when the crate is compiled; a crate compiled
//! without it serves a page that says how to build it ([`built`] says which).

mod assets;
mod frontend;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;

use assets::{Dist, Files};

/// The UI's router: the frontend, and, under `/api`, nothing yet.
pub fn router() -> Router {
    router_with(Arc::new(Dist))
}

/// Whether the frontend was embedded when this crate was compiled.
pub fn built() -> bool {
    Dist.built()
}

/// The router over `files`, which a test gives in place of the embedded ones.
pub(crate) fn router_with(files: Arc<dyn Files>) -> Router {
    Router::new().fallback(frontend::serve).with_state(files)
}
