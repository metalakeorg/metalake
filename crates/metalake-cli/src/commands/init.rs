//! `metalake init`: a new lake, with the namespace a session's unqualified tables go in, which answers as `status`.

use std::path::Path;

use crate::args::Init;
use crate::commands::status::Status;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(init: &Init, dir: Option<&Path>, out: &Out) -> Result<(), Failure> {
    let lake = Lake::create(init, dir).await?;
    let main = lake.catalog.branch("main").await?;
    main.transaction().create_namespace(&["default"]).commit().await?;
    out.value(&Status::of(&lake).await?)
}
