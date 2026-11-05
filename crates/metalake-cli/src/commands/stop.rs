//! `metalake stop`: ends a lake's own Postgres, which otherwise stays up. A live server keeps its files open, and on
//! Windows that stops the lake being moved or deleted.

use std::path::Path;

use serde_json::json;

use crate::out::Out;
use metalake_client::Error;
use metalake_client::Lake;

pub async fn run(dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let stopped = Lake::stop(dir)?;
    let row = json!({"stopped": stopped.display().to_string()});
    out.records(&["stopped"], &[row]).await
}
