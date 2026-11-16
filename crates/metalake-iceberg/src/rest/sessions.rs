//! `PUT /metalake/sessions/{id}`: a session says which ref it is on, so that a client connected to it (`sessions/<id>`,
//! [`super::prefix`]) reads and writes that ref from its next request (I6). MetaLake's own route, outside Iceberg's, as
//! `/metalake/shutdown` is; it answers only a request that bears the lake's token.

use axum::Extension;
use axum::extract::{Path, State as Shared};
use axum::{Json, body::Bytes};
use metalake::Error;
use serde::Deserialize;

use super::errors::Failure;
use super::tables::decoded;
use crate::caller::Caller;
use crate::state::State;

/// The route's path.
pub(crate) const PATH: &str = "/metalake/sessions/{id}";

#[derive(Deserialize)]
struct Said {
    #[serde(rename = "ref")]
    on: String,
}

/// Session `id` is on the ref the body names, `{"ref": "dev"}`, from now on. A ref the lake has not is refused, and the
/// session keeps the one it had.
pub(crate) async fn put(
    Shared(state): Shared<State>,
    caller: Option<Extension<Caller>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, Failure> {
    if !matches!(caller, Some(Extension(Caller::Writer))) {
        return Err(Failure::session_refused());
    }
    let Said { on } = decoded(&body)?;
    match state.named(&on).await {
        Ok(_) => {}
        Err(Error::NotFound(_)) => return Err(Failure::no_ref(&on)),
        Err(e) => return Err(e.into()),
    }
    state.follow(&id, &on);
    Ok(Json(serde_json::json!({"session": id, "ref": on})))
}
