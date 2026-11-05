//! `metalake branch`, `tag` and `refs`: the life of a ref, one operation of two kinds.

use metalake_client::{Error, Lake, RefKind};
use serde_json::json;

use crate::args::RefVerb;
use crate::out::Out;

const COLUMNS: [&str; 6] = ["name", "kind", "parent", "fork_seq", "created_seq", "protected"];

/// Every ref, or those of one kind.
pub async fn list(lake: &Lake, out: &Out, kind: Option<RefKind>) -> Result<(), Error> {
    out.records(&COLUMNS, &lake.refs(kind).await?).await
}

pub async fn run(verb: &RefVerb, kind: RefKind, lake: &Lake, out: &Out) -> Result<(), Error> {
    match verb {
        RefVerb::List => list(lake, out, Some(kind)).await,
        RefVerb::Create { name, from } => {
            let created = lake.create_ref(kind, name, from.as_deref()).await?;
            out.records(&COLUMNS, &[created]).await
        }
        RefVerb::Delete { name, yes } => {
            if !*yes {
                return Err(Error::usage(format!(
                    "a ref's name is never used again: pass --yes to delete {name}"
                )));
            }
            lake.delete_ref(kind, name).await?;
            out.records(&["deleted"], &[json!({"deleted": name})]).await
        }
    }
}
