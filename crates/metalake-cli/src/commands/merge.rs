//! `metalake merge`: merges a branch into its parent, or says what that would do. Postgres decides, but the union of
//! two vectors changed on both sides is bytes, so the core takes the store too.

use std::collections::HashMap;
use std::env;
use std::fmt;

use metalake::{MergeReport, Ref, Table};
use serde::Serialize;
use uuid::Uuid;

use crate::args::Merge;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

#[derive(Debug, Serialize)]
struct Outcome {
    branch: String,
    /// `merged`, `nothing`, or, with `--check`, `would merge`.
    outcome: &'static str,
    commit: Option<i64>,
    merged: Vec<String>,
    rebased: Vec<String>,
}

impl fmt::Display for Outcome {
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

pub async fn run(args: &Merge, lake: &Lake, out: &Out) -> Result<(), Failure> {
    let branch = lake.catalog.branch(&args.branch).await?;
    let mut merge = branch.merge();
    let selected = selection(&branch, &args.tables).await?;
    if !selected.is_empty() {
        merge = merge.tables(&selected.iter().collect::<Vec<_>>());
    }
    if args.split {
        merge = merge.split();
    }
    if let Some(message) = &args.message {
        merge = merge.message(message);
    }
    if let Some(author) = author(args) {
        merge = merge.author(author);
    }
    if lake.catalog.warehouse().is_some() {
        merge = merge.storage(&lake.storage()?);
    }

    let (merged, rebased) = match merge.report().await? {
        MergeReport::Nothing => return out.value(&outcome(args, "nothing", None)),
        MergeReport::Refused { rule, message } if args.check => return Err(Failure::conflict(rule, message)),
        // A commit says why better than a report does: its refusal has a kind.
        MergeReport::Refused { .. } => (vec![], vec![]),
        MergeReport::Merges { merged, rebased } => (merged, rebased),
    };
    let names = names(lake, &branch).await?;
    let named = |ids: Vec<_>| -> Vec<String> {
        ids.into_iter()
            .map(|id| names.get(&id).cloned().unwrap_or_else(|| id.to_string()))
            .collect()
    };
    let mut done = outcome(args, "would merge", None);
    done.merged = named(merged);
    done.rebased = named(rebased);
    if !args.check {
        done.commit = merge.commit().await?;
        done.outcome = if done.commit.is_some() { "merged" } else { "nothing" };
    }
    out.value(&done)
}

fn outcome(args: &Merge, outcome: &'static str, commit: Option<i64>) -> Outcome {
    Outcome {
        branch: args.branch.clone(),
        outcome,
        commit,
        merged: vec![],
        rebased: vec![],
    }
}

/// The tables named, by `namespace.table`.
async fn selection(branch: &Ref, tables: &[String]) -> Result<Vec<Table>, Failure> {
    let mut selected = Vec::with_capacity(tables.len());
    for table in tables {
        let (namespace, name) = table
            .rsplit_once('.')
            .ok_or_else(|| Failure::usage(format!("{table}: a table is namespace.table")))?;
        let namespace: Vec<&str> = namespace.split('.').collect();
        selected.push(branch.table(&namespace, name).await?);
    }
    Ok(selected)
}

/// The names of the tables of a branch and its parent, by id: a report speaks in ids, and a table may be on one alone.
async fn names(lake: &Lake, branch: &Ref) -> Result<HashMap<Uuid, String>, Failure> {
    let refs = lake.catalog.describe_refs().await?;
    let parent = refs
        .into_iter()
        .find(|r| r.name == branch.name())
        .and_then(|r| r.parent);
    let mut tables = branch.tables().await?;
    if let Some(parent) = parent {
        tables.extend(lake.catalog.branch(&parent).await?.tables().await?);
    }
    let name = |t: metalake::TableName| (t.table_id, format!("{}.{}", t.namespace.join("."), t.name));
    Ok(tables.into_iter().map(name).collect())
}

/// Who the commit names: as asked, or the person at the machine.
fn author(args: &Merge) -> Option<String> {
    let named = ["METALAKE_AUTHOR", "USER", "USERNAME"];
    args.author
        .clone()
        .or_else(|| named.iter().find_map(|v| env::var(v).ok()))
}
