//! Merges: a branch, or some of its tables, into its parent, in one transaction of the protocol (MRG-1 to MRG-8). The
//! protocol decides everything but one thing: when both sides changed a file's vector and one of them is in Puffin,
//! Postgres cannot read it, so the core reads both and hands in their union (MRG-5). It does so only when the
//! protocol asks, with a refusal, and a merge without such vectors costs nothing more.

use std::collections::BTreeMap;

use futures_util::TryStreamExt;
use metalake_pg::Merge as Call;
use metalake_pg::records::{PlannedFile, VectorSet};
use serde_json::Value;
use uuid::Uuid;

use crate::catalog::{Catalog, Ref};
use crate::error::{Error, Result};
use crate::scan::{self, TaskVector};
use crate::storage::Storage;
use crate::table::Table;
use crate::write::delete;
use crate::write::transaction::{ATTEMPTS, repeatable};

/// What a merge would do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeReport {
    /// It would merge these tables, and rebase these others of the branch onto the merge.
    Merges { merged: Vec<Uuid>, rebased: Vec<Uuid> },
    /// The branch has nothing to merge.
    Nothing,
    /// It would be refused, by this rule of the specification.
    Refused { rule: Option<String>, message: String },
}

/// A merge of a branch into its parent: all of it, or the tables named.
#[derive(Debug, Clone)]
pub struct Merge {
    branch: Ref,
    tables: Option<Vec<Uuid>>,
    split: bool,
    author: String,
    message: Option<String>,
    storage: Option<Storage>,
}

impl Merge {
    pub(crate) fn new(branch: Ref) -> Self {
        Self {
            branch,
            tables: None,
            split: false,
            author: "metalake".into(),
            message: None,
            storage: None,
        }
    }

    /// Only these tables; the branch's others are rebased onto the merge (MRG-6, MRG-8).
    pub fn tables(mut self, tables: &[&Table]) -> Self {
        self.tables = Some(tables.iter().map(|t| t.id()).collect());
        self
    }

    /// Allows a selection that a commit of the branch crossed, by splitting that commit (MRG-7).
    pub fn split(mut self) -> Self {
        self.split = true;
        self
    }

    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = author.into();
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Where the tables' vectors are: a merge needs it only if both sides changed a vector and one is in Puffin.
    pub fn storage(mut self, storage: &Storage) -> Self {
        self.storage = Some(storage.clone());
        self
    }

    fn call(&self) -> Call {
        Call {
            only_tables: self.tables.clone(),
            split: self.split,
            author: self.author.clone(),
            message: self.message.clone(),
            ..Call::of(self.branch.id())
        }
    }

    /// What the merge would do, or why it would be refused, without doing it.
    pub async fn report(&self) -> Result<MergeReport> {
        let catalog = self.branch.catalog();
        let p = catalog.protocol();
        let mut call = self.call();
        let mut report = parse(p.merge_report(&catalog.session().await?, &call).await?);
        if matches!(&report, MergeReport::Refused { rule: Some(r), .. } if r == "MRG-5") {
            call.vectors = self.unions(false).await?;
            if !call.vectors.is_empty() {
                report = parse(p.merge_report(&catalog.session().await?, &call).await?);
            }
        }
        Ok(report)
    }

    /// Merges, in one transaction, and answers the merge's sequence, or none if the branch had nothing to merge.
    pub async fn commit(self) -> Result<Option<i64>> {
        self.branch.committable()?;
        let catalog = self.branch.catalog();
        let mut call = self.call();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = async { catalog.protocol().merge(&catalog.pg_session().await?, &call).await }.await;
            match result {
                Ok(merged) => return Ok(merged.map(|r| r.commit_seq)),
                Err(e) if attempt < ATTEMPTS && repeatable(e.kind(), false) => continue,
                Err(e) if attempt < ATTEMPTS && e.rule() == Some("MRG-5") => {
                    call.vectors = self.unions(true).await?;
                    if call.vectors.is_empty() {
                        return Err(e.into());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// The union of each vector both sides changed that Postgres cannot read (MRG-5), with `upload` written to the
    /// tables' storage. Without a storage there are none, and the protocol's refusal stands.
    async fn unions(&self, upload: bool) -> Result<Vec<VectorSet>> {
        let Some(storage) = &self.storage else {
            return Ok(Vec::new());
        };
        let catalog = self.branch.catalog();
        let rows = catalog.protocol().refs(&catalog.session().await?).await?;
        let row = rows.iter().find(|r| r.ref_id == self.branch.id());
        let (Some(parent), Some(fork)) = (row.and_then(|r| r.parent.clone()), row.and_then(|r| r.fork_seq)) else {
            return Ok(Vec::new());
        };
        let parent = rows
            .iter()
            .find(|r| r.name == parent)
            .ok_or_else(|| Error::NotFound(format!("ref {parent}")))?
            .ref_id;
        let mut sets = Vec::new();
        for name in self.branch.tables().await? {
            if self.tables.as_ref().is_some_and(|t| !t.contains(&name.table_id)) {
                continue;
            }
            let namespace: Vec<&str> = name.namespace.iter().map(String::as_str).collect();
            let table = self.branch.table(&namespace, &name.name).await?;
            sets.extend(union_of(catalog, &table, storage, parent, fork, upload).await?);
        }
        Ok(sets)
    }
}

/// The unions of `table`'s vectors its branch changed and its parent changed too since the fork. Two inline ones among
/// them are left to the protocol, which ignores what it is handed for those.
async fn union_of(
    catalog: &Catalog,
    table: &Table,
    storage: &Storage,
    parent: i32,
    fork: i64,
    upload: bool,
) -> Result<Vec<VectorSet>> {
    let mine = files(catalog, table.ref_id(), table, None).await?;
    let now = files(catalog, parent, table, None).await?;
    let then = files(catalog, parent, table, Some(fork)).await?;
    let mut vectors = Vec::new();
    for (file_id, file) in &mine {
        let (Some(ours), Some(theirs)) = (&file.vector, now.get(file_id).and_then(|f| f.vector.as_ref())) else {
            continue;
        };
        let at_fork = then.get(file_id).and_then(|f| f.vector.as_ref());
        let both_changed = at_fork != Some(ours) && at_fork != Some(theirs);
        if !both_changed {
            continue;
        }
        let read = |v: TaskVector| async move {
            let vector = scan::vector(*file_id, Some(&v), storage).await?;
            Ok::<_, Error>(vector.unwrap_or_default())
        };
        let (a, b) = (read(ours.clone().into()).await?, read(theirs.clone().into()).await?);
        vectors.push((*file_id, None, a.union(&b)));
    }
    let paths = mine.iter().filter_map(|(id, f)| Some((*id, f.path.clone()?))).collect();
    delete::stored(catalog, table, storage, &paths, vectors, upload).await
}

async fn files(catalog: &Catalog, ref_id: i32, table: &Table, at: Option<i64>) -> Result<BTreeMap<i64, PlannedFile>> {
    let session = catalog.session().await?;
    Ok(catalog
        .protocol()
        .plan(&session, ref_id, table.id(), at, None)
        .await?
        .map_ok(|f| (f.file_id, f))
        .try_collect()
        .await?)
}

fn parse(report: Value) -> MergeReport {
    let tables = |key: &str| -> Vec<Uuid> {
        report[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t.as_str()?.parse().ok())
            .collect()
    };
    if report["ok"] != Value::Bool(true) {
        return MergeReport::Refused {
            rule: report["rule"].as_str().map(str::to_string),
            message: report["message"].as_str().unwrap_or_default().to_string(),
        };
    }
    if report["merges"] != Value::Bool(true) {
        return MergeReport::Nothing;
    }
    MergeReport::Merges {
        merged: tables("merged"),
        rebased: tables("rebased"),
    }
}
