//! `metalake status`: what the lake is.

use metalake_client::{Error, Lake};

use crate::out::Out;

pub async fn run(lake: &Lake, out: &Out) -> Result<(), Error> {
    out.value(&lake.status().await?)
}
