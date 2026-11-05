//! `metalake init`: a new lake, with the namespace a session's unqualified tables go in, which answers as `status`.

use std::path::Path;

use crate::args::Init;
use crate::commands::status::Status;
use crate::out::Out;
use metalake_client::Error;
use metalake_client::Lake;

pub async fn run(init: &Init, dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let lake = Lake::create(
        &init.catalog,
        init.warehouse.as_deref(),
        init.schema.as_deref(),
        dir,
        Lake::ONE_COMMAND,
    )
    .await?;
    out.value(&Status::of(&lake).await?)
}
