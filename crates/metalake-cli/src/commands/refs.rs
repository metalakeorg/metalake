//! `metalake branch`, `tag` and `refs`: the life of a ref, one operation of two kinds. Reading and writing one is `sql`,
//! since a ref is a catalog.

use metalake::{Ref, RefRow};
use serde::Serialize;
use serde_json::json;

use crate::args::RefVerb;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

const COLUMNS: [&str; 6] = ["name", "kind", "parent", "fork_seq", "created_seq", "protected"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Branch,
    Tag,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Branch => "branch",
            Kind::Tag => "tag",
        }
    }
}

#[derive(Debug, Serialize)]
struct Described {
    name: String,
    kind: String,
    parent: Option<String>,
    fork_seq: Option<i64>,
    created_seq: i64,
    protected: bool,
}

impl From<RefRow> for Described {
    fn from(row: RefRow) -> Self {
        Self {
            name: row.name,
            kind: row.kind,
            parent: row.parent,
            fork_seq: row.fork_seq,
            created_seq: row.created_seq,
            protected: row.protected,
        }
    }
}

async fn described(lake: &Lake) -> Result<Vec<Described>, Failure> {
    let rows = lake.catalog.describe_refs().await?;
    Ok(rows.into_iter().map(Described::from).collect())
}

/// Every ref, or those of one kind.
pub async fn list(lake: &Lake, out: &Out, kind: Option<Kind>) -> Result<(), Failure> {
    let mut refs = described(lake).await?;
    if let Some(kind) = kind {
        refs.retain(|r| r.kind == kind.name());
    }
    out.records(&COLUMNS, &refs).await
}

pub async fn run(verb: &RefVerb, kind: Kind, lake: &Lake, out: &Out) -> Result<(), Failure> {
    match verb {
        RefVerb::List => list(lake, out, Some(kind)).await,
        RefVerb::Create { name, from } => {
            let from = source(lake, from.as_deref().unwrap_or("main")).await?;
            match kind {
                Kind::Branch => from.create_branch(name).await?,
                Kind::Tag => from.create_tag(name).await?,
            };
            let mut refs = described(lake).await?;
            refs.retain(|r| r.name == *name);
            out.records(&COLUMNS, &refs).await
        }
        RefVerb::Delete { name, yes } => {
            if !*yes {
                return Err(Failure::usage(format!(
                    "a ref's name is never used again: pass --yes to delete {name}"
                )));
            }
            let named = match kind {
                Kind::Branch => lake.catalog.branch(name).await?,
                Kind::Tag => lake.catalog.tag(name).await?,
            };
            named.delete().await?;
            out.records(&["deleted"], &[json!({"deleted": name})]).await
        }
    }
}

/// The ref and point a new one starts from: `main`, or `main@1234`. A ref's own name, `@` included, comes first, as in
/// SQL. A time has no one sequence to start from, so it is refused here, with what to do.
async fn source(lake: &Lake, from: &str) -> Result<Ref, Failure> {
    let refs = lake.catalog.refs().await?;
    let named = |name: &str| refs.iter().find(|r| r.name() == name);
    if let Some(on) = named(from) {
        return Ok(on.clone());
    }
    let (name, at) = from
        .split_once('@')
        .ok_or_else(|| Failure::failed(format!("no ref {from}")))?;
    let on = named(name).ok_or_else(|| Failure::failed(format!("no ref {name}")))?;
    let seq = at.parse().map_err(|_| {
        Failure::usage(format!(
            "{from}: a ref starts from a sequence, which `metalake log` shows"
        ))
    })?;
    Ok(on.at(seq))
}
