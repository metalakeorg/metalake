//! The routes: each takes a request, calls what answers it, and turns the answer or the failure into HTTP. What a client
//! is told lives here, and what it means does not.

mod config;
mod errors;
mod namespaces;
mod tables;

use axum::Router;
use axum::routing::{get, post};

use crate::state::State;
use errors::Failure;

pub(crate) fn router(state: State) -> Router {
    Router::new()
        .route("/v1/config", get(config::config))
        .route("/v1/namespaces", get(namespaces::list).post(namespaces::create))
        .route(
            "/v1/namespaces/{namespace}",
            get(namespaces::load).head(namespaces::exists).delete(namespaces::drop),
        )
        .route("/v1/namespaces/{namespace}/properties", post(namespaces::update))
        .route("/v1/tables/rename", post(tables::rename))
        .route("/v1/transactions/commit", post(tables::commit_transaction))
        .route(
            "/v1/namespaces/{namespace}/tables",
            get(tables::list).post(tables::create),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            get(tables::get)
                .head(tables::exists)
                .post(tables::update)
                .delete(tables::drop),
        )
        .fallback(async || Failure::no_route())
        .with_state(state)
}
