//! `metalake stop`: ends a lake's own Postgres, which otherwise stays up. A live server keeps its files open, and on
//! Windows that stops the lake being moved or deleted.

use std::path::Path;

use serde_json::json;

use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(dir: Option<&Path>, out: &Out) -> Result<(), Failure> {
    let stopped = Lake::stop(dir)?;
    let row = json!({"stopped": stopped.display().to_string()});
    out.records(&["stopped"], &[row]).await
}
