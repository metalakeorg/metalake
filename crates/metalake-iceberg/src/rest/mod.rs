//! The routes: each takes a request, calls what answers it, and turns the answer or the failure into HTTP. What a client
//! is told lives here, and what it means does not.

mod config;
mod errors;
mod namespaces;

use axum::Router;
use axum::routing::get;

use crate::state::State;
use errors::Failure;

pub(crate) fn router(state: State) -> Router {
    Router::new()
        .route("/v1/config", get(config::config))
        .route("/v1/namespaces", get(namespaces::list))
        .fallback(async || Failure::no_route())
        .with_state(state)
}
