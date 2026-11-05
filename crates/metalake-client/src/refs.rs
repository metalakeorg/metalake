//! The life of a ref, one operation of two kinds. Reading and writing one is a statement, since a ref is a catalog.

use metalake::{Ref, RefRow, Table};
use serde::Serialize;

use crate::{Error, Lake, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefKind {
    Branch,
    Tag,
}

impl RefKind {
    fn name(self) -> &'static str {
        match self {
            RefKind::Branch => "branch",
            RefKind::Tag => "tag",
        }
    }
}

/// What a ref is.
#[derive(Debug, Serialize)]
pub struct RefInfo {
    pub name: String,
    pub kind: String,
    pub parent: Option<String>,
    pub fork_seq: Option<i64>,
    pub created_seq: i64,
    pub protected: bool,
}

impl From<RefRow> for RefInfo {
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

impl Lake {
    /// Every ref, or those of one kind.
    pub async fn refs(&self, kind: Option<RefKind>) -> Result<Vec<RefInfo>> {
        let rows = self.catalog.describe_refs().await?;
        let mut refs: Vec<RefInfo> = rows.into_iter().map(RefInfo::from).collect();
        if let Some(kind) = kind {
            refs.retain(|r| r.kind == kind.name());
        }
        Ok(refs)
    }

    /// A ref by its name, which must exist.
    pub async fn find_ref(&self, name: &str) -> Result<Ref> {
        let refs = self.catalog.refs().await?;
        let found = refs.into_iter().find(|r| r.name() == name);
        found.ok_or_else(|| Error::failed(format!("no ref {name}")))
    }

    /// A new ref, from `main` unless it says otherwise.
    pub async fn create_ref(&self, kind: RefKind, name: &str, from: Option<&str>) -> Result<RefInfo> {
        let from = self.source(from.unwrap_or("main")).await?;
        match kind {
            RefKind::Branch => from.create_branch(name).await?,
            RefKind::Tag => from.create_tag(name).await?,
        };
        let created = self.refs(None).await?.into_iter().find(|r| r.name == name);
        created.ok_or_else(|| Error::failed(format!("no ref {name}")))
    }

    /// Deletes a ref. Its name is never used again.
    pub async fn delete_ref(&self, kind: RefKind, name: &str) -> Result<()> {
        let named = match kind {
            RefKind::Branch => self.catalog.branch(name).await?,
            RefKind::Tag => self.catalog.tag(name).await?,
        };
        Ok(named.delete().await?)
    }

    /// The ref and point a new one starts from: `main`, or `main@1234`. A ref's own name, `@` included, comes first, as
    /// in SQL. A time has no one sequence to start from, so it is refused here, with what to do.
    async fn source(&self, from: &str) -> Result<Ref> {
        let refs = self.catalog.refs().await?;
        let named = |name: &str| refs.iter().find(|r| r.name() == name);
        if let Some(on) = named(from) {
            return Ok(on.clone());
        }
        let (name, at) = from
            .split_once('@')
            .ok_or_else(|| Error::failed(format!("no ref {from}")))?;
        let on = named(name).ok_or_else(|| Error::failed(format!("no ref {name}")))?;
        let seq = at.parse().map_err(|_| {
            Error::usage(format!(
                "{from}: a ref starts from a sequence, which `metalake log` shows"
            ))
        })?;
        Ok(on.at(seq))
    }
}

/// A table of a ref, by `namespace.table`.
pub async fn table(on: &Ref, name: &str) -> Result<Table> {
    let (namespace, table) = name
        .rsplit_once('.')
        .ok_or_else(|| Error::usage(format!("{name}: a table is namespace.table")))?;
    let namespace: Vec<&str> = namespace.split('.').collect();
    Ok(on.table(&namespace, table).await?)
}
