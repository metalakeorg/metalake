//! Merges a branch into its parent, or says what that would do. Postgres decides, but the union of two vectors changed on
//! both sides is bytes, so the core takes the store too.

use std::collections::HashMap;
use std::env;
use std::fmt;

use metalake::{MergeReport, Ref, Table};
use serde::Serialize;
use uuid::Uuid;

use crate::{Error, Lake, Result, table};

/// A merge to make or to check. Tables are `namespace.table`, and none means every table.
#[derive(Debug, Default)]
pub struct MergeRequest {
    pub branch: String,
    pub tables: Vec<String>,
    pub split: bool,
    pub message: Option<String>,
    pub author: Option<String>,
    /// Says what the merge would do, and changes nothing.
    pub check: bool,
}

#[derive(Debug, Serialize)]
pub struct MergeOutcome {
    pub branch: String,
    /// `merged`, `nothing`, or, with `check`, `would merge`.
    pub outcome: &'static str,
    pub commit: Option<i64>,
    pub merged: Vec<String>,
    pub rebased: Vec<String>,
}

impl fmt::Display for MergeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.branch, self.outcome)?;
        if let Some(commit) = self.commit {
            write!(f, " at {commit}")?;
        }
        if !self.merged.is_empty() {
            write!(f, "\nmerged   {}", self.merged.join(", "))?;
        }
        if !self.rebased.is_empty() {
            write!(f, "\nrebased  {}", self.rebased.join(", "))?;
        }
        Ok(())
    }
}

impl Lake {
    /// Merges a branch, or says what that would do. A check refused says so by a conflict, as the merge itself would.
    pub async fn merge(&self, request: &MergeRequest) -> Result<MergeOutcome> {
        let branch = self.catalog.branch(&request.branch).await?;
        let mut merge = branch.merge();
        let selected = selection(&branch, &request.tables).await?;
        if !selected.is_empty() {
            merge = merge.tables(&selected.iter().collect::<Vec<_>>());
        }
        if request.split {
            merge = merge.split();
        }
        if let Some(message) = &request.message {
            merge = merge.message(message);
        }
        if let Some(author) = author(request) {
            merge = merge.author(author);
        }
        if self.catalog.warehouse().is_some() {
            merge = merge.storage(&self.storage()?);
        }

        let (merged, rebased) = match merge.report().await? {
            MergeReport::Nothing => return Ok(outcome(request, "nothing")),
            MergeReport::Refused { rule, message } if request.check => return Err(Error::conflict(rule, message)),
            // A commit says why better than a report does: its refusal has a kind.
            MergeReport::Refused { .. } => (vec![], vec![]),
            MergeReport::Merges { merged, rebased } => (merged, rebased),
        };
        let names = self.names(&branch).await?;
        let named = |ids: Vec<_>| -> Vec<String> {
            ids.into_iter()
                .map(|id| names.get(&id).cloned().unwrap_or_else(|| id.to_string()))
                .collect()
        };
        let mut done = outcome(request, "would merge");
        done.merged = named(merged);
        done.rebased = named(rebased);
        if !request.check {
            done.commit = merge.commit().await?;
            done.outcome = if done.commit.is_some() { "merged" } else { "nothing" };
        }
        Ok(done)
    }

    /// The names of the tables of a branch and its parent, by id: a report speaks in ids, and a table may be on one
    /// alone.
    async fn names(&self, branch: &Ref) -> Result<HashMap<Uuid, String>> {
        let refs = self.catalog.describe_refs().await?;
        let parent = refs
            .into_iter()
            .find(|r| r.name == branch.name())
            .and_then(|r| r.parent);
        let mut tables = branch.tables().await?;
        if let Some(parent) = parent {
            tables.extend(self.catalog.branch(&parent).await?.tables().await?);
        }
        let name = |t: metalake::TableName| (t.table_id, format!("{}.{}", t.namespace.join("."), t.name));
        Ok(tables.into_iter().map(name).collect())
    }
}

fn outcome(request: &MergeRequest, outcome: &'static str) -> MergeOutcome {
    MergeOutcome {
        branch: request.branch.clone(),
        outcome,
        commit: None,
        merged: vec![],
        rebased: vec![],
    }
}

/// The tables named, by `namespace.table`.
async fn selection(branch: &Ref, tables: &[String]) -> Result<Vec<Table>> {
    let mut selected = Vec::with_capacity(tables.len());
    for name in tables {
        selected.push(table(branch, name).await?);
    }
    Ok(selected)
}

/// Who the commit names: as asked, or the person at the machine.
fn author(request: &MergeRequest) -> Option<String> {
    let named = ["METALAKE_AUTHOR", "USER", "USERNAME"];
    request
        .author
        .clone()
        .or_else(|| named.iter().find_map(|v| env::var(v).ok()))
}
