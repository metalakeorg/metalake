//! `metalake merge`: merges a branch into its parent, or says what that would do.

use metalake_client::{Error, Lake, MergeRequest};

use crate::args::Merge;
use crate::out::Out;

pub async fn run(args: &Merge, lake: &Lake, out: &Out) -> Result<(), Error> {
    let done = lake
        .merge(&MergeRequest {
            branch: args.branch.clone(),
            tables: args.tables.clone(),
            split: args.split,
            message: args.message.clone(),
            author: args.author.clone(),
            check: args.check,
        })
        .await?;
    out.value(&done)
}
