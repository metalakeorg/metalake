//! `metalake init`: a new lake, which answers as `status` does.

use std::path::Path;

use crate::args::Init;
use crate::commands::status::Status;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(init: &Init, dir: Option<&Path>, out: &Out) -> Result<(), Failure> {
    let lake = Lake::create(init, dir).await?;
    out.value(&Status::of(&lake).await?)
}
