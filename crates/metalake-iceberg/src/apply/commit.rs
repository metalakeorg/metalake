//! `updateTable` and `transactions/commit` as one commit of the protocol (ICE-6, REST-9), on what `update` and `files`
//! established.

use std::collections::{BTreeMap, HashMap};

use metalake::{Error, FileAdd, Ref, Table};
use metalake_pg::ErrorKind;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::files::{self, ManifestFile};
use super::update::{Change, one_ref};
use super::{ApplyError, Refused, create};
use crate::footers::Job;
use crate::project::metadata::{definition_ids, is_empty_creation};
use crate::state::State;

/// The version of every Iceberg idempotency key: a key names one proposed snapshot (REST-3).
const APP_VERSION: i64 = 1;

/// The type of each field of a definition.
fn field_types(definition: &Json) -> HashMap<i32, String> {
    let fields = definition["fields"].as_array().map_or(&[][..], Vec::as_slice);
    let typed = |f: &Json| Some((f["field_id"].as_i64()? as i32, f["type"].as_str()?.to_string()));
    fields.iter().filter_map(typed).collect()
}

fn file_adds(table_id: Uuid, definition: &Json, files: &[ManifestFile]) -> Result<Vec<FileAdd>, Refused> {
    let types = field_types(definition);
    let add = |file: &ManifestFile| {
        let spec = definition["specs"][file.spec_id.to_string()].as_array();
        let spec = spec.ok_or_else(|| Refused::Invalid(format!("no partition spec {}", file.spec_id)))?;
        files::file_add(table_id, file, spec, &types)
    };
    files.iter().map(add).collect()
}

/// The id in a path the projection serves for an inline file (ICE-5).
fn inline_id(location: &str, path: &str) -> Option<i64> {
    let served = path.strip_prefix(location.trim_end_matches('/'))?;
    served
        .strip_prefix("/metalake-inline/")?
        .strip_suffix(".parquet")?
        .parse()
        .ok()
}

/// Splits removed paths into those the catalog holds as paths, and the ids of inline files.
fn by_kind(location: &str, removed: &[String]) -> (Vec<String>, Vec<i64>) {
    let (mut paths, mut ids) = (Vec::new(), Vec::new());
    for path in removed {
        match inline_id(location, path) {
            Some(id) => ids.push(id),
            None => paths.push(path.clone()),
        }
    }
    (paths, ids)
}

/// The ids of the files named, or `None` if one is not visible.
fn resolved(paths: &[String], ids: &[i64], found: &[(Option<String>, i64)]) -> Option<Vec<i64>> {
    let at: HashMap<&str, i64> = found.iter().filter_map(|(p, id)| Some((p.as_deref()?, *id))).collect();
    let mut resolved = Vec::with_capacity(paths.len() + ids.len());
    for path in paths {
        resolved.push(*at.get(path.as_str())?);
    }
    for id in ids {
        found.iter().find(|(_, f)| f == id)?;
        resolved.push(*id);
    }
    Some(resolved)
}

/// A table a commit changes, as a client names it.
#[derive(Debug)]
pub struct Target {
    pub namespace: Vec<String>,
    pub name: String,
    pub change: Change,
}

impl Target {
    fn levels(&self) -> Vec<&str> {
        self.namespace.iter().map(String::as_str).collect()
    }
}

/// What one table adds to a commit.
#[derive(Debug)]
struct Prepared {
    table_id: Uuid,
    table: Option<Table>,
    adds: Vec<FileAdd>,
    removed: Vec<i64>,
    definition: Option<Json>,
    expected: Option<Option<i64>>,
    summary: BTreeMap<String, String>,
    replace: bool,
}

/// The table a change names, or the id a stage-create gave it.
async fn resolve(on: &Ref, target: &Target) -> Result<(Uuid, Option<Table>), ApplyError> {
    if let Some(creation) = &target.change.creation {
        let id = Uuid::parse_str(&creation.table_id)
            .map_err(|_| Refused::Invalid(format!("{} is not a table uuid", creation.table_id)))?;
        return Ok((id, None));
    }
    let table = on.table(&target.levels(), &target.name).await?;
    target.change.check_table(&table.id().to_string())?;
    Ok((table.id(), Some(table)))
}

/// Holds what a change asserts of the table's definition to the table on the ref, as a load would serve it (REST-12).
async fn check_asserted(state: &State, on: &Ref, change: &Change, table: &Table) -> Result<(), ApplyError> {
    if change.asserted.is_empty() {
        return Ok(());
    }
    let definition = on.definition(table, None).await?;
    let head = definition["snapshot_id"].as_i64();
    let heads: Vec<i64> = head.into_iter().collect();
    let found = state.catalog().ancestors(table, &heads, 1, None).await?;
    // The current snapshot's schema, as the load finds it among the snapshots it serves.
    let current = found.iter().find(|s| Some(s.snapshot_id) == head);
    let ids = definition_ids(&definition, current.map(|s| s.schema_seq));
    for asserted in &change.asserted {
        asserted.check(&ids)?;
    }
    Ok(())
}

/// What a client that saw a table with no snapshot expects: the snapshot its creation wrote, which it is not served
/// (ICE-1). A table that has been written since keeps `None`, so the commit conflicts.
async fn base_of_unwritten(state: &State, table: &Table) -> Result<Option<i64>, ApplyError> {
    let heads: Vec<i64> = table.snapshot_id().into_iter().collect();
    let snapshots = state.catalog().ancestors(table, &heads, 1, None).await?;
    let unwritten = snapshots.iter().find(|s| is_empty_creation(s));
    Ok(unwritten.map(|s| s.snapshot_id))
}

/// The files of a change, and its definition: what a stage-create sends back, or the properties it changes.
async fn prepare(
    state: &State,
    on: &Ref,
    target: &Target,
    table_id: Uuid,
    table: Option<Table>,
) -> Result<Prepared, ApplyError> {
    let change = &target.change;
    if let Some(table) = &table {
        check_asserted(state, on, change, table).await?;
    }
    let definition = match &change.creation {
        Some(creation) => Some(create::definition(
            &target.levels(),
            &target.name,
            creation,
            &change.properties,
        )?),
        None if change.properties.is_empty() => None,
        None => Some(json!({"properties": create::properties(&change.properties)?})),
    };
    let (mut adds, mut removed) = (Vec::new(), Vec::new());
    if !change.snapshots.is_empty() {
        let storage = state.storage()?;
        let proposed = files::proposed(&storage, &change.snapshots).await?;
        let described = match (&table, &definition) {
            (Some(table), _) => on.definition(table, None).await?,
            (None, Some(new)) => create::described(new),
            (None, None) => unreachable!("a change without a table creates one"),
        };
        if let Some(table) = &table {
            let (paths, ids) = by_kind(table.location(), &proposed.removed);
            let found = match proposed.removed.is_empty() {
                true => Vec::new(),
                false => on.files_at_paths(table, &paths, &ids).await?,
            };
            let conflict = || Refused::Conflict("a file the commit removes is not visible on the ref".into());
            removed = resolved(&paths, &ids, &found).ok_or_else(conflict)?;
        } else if !proposed.removed.is_empty() {
            return Err(Refused::Invalid("a commit that creates a table removes no files".into()).into());
        }
        adds = file_adds(table_id, &described, &proposed.added)?;
    }
    let expected = match (&table, change.expected) {
        (None, _) => None,
        (Some(table), Some(None)) => Some(base_of_unwritten(state, table).await?),
        (Some(_), expected) => expected,
    };
    Ok(Prepared {
        table_id,
        table,
        adds,
        removed,
        definition,
        expected,
        summary: change.client_summary(),
        replace: change.operation() == Some("replace"),
    })
}

/// The commit's one key: a client keeps its snapshot ids across attempts, so the least of them is the same each time, and
/// any of them names the commit (REST-3).
fn commit_key(targets: &[Target], ids: &[Uuid]) -> Option<String> {
    let keys = targets
        .iter()
        .zip(ids)
        .filter_map(|(t, id)| t.change.app_id(&id.to_string()));
    keys.min()
}

/// Commits the changes of several tables as one commit on one ref (ICE-6, REST-9). A key already applied answers at once,
/// whatever the client has since done with its files (REST-3).
pub async fn apply(state: &State, targets: &[Target]) -> Result<Vec<Table>, ApplyError> {
    let ref_name = one_ref(targets.iter().map(|t| &t.change))?;
    let on = state.named(ref_name).await.map_err(|e| match e {
        Error::NotFound(_) => ApplyError::from(Refused::Invalid(format!("no ref {ref_name}"))),
        other => other.into(),
    })?;
    let mut resolved = Vec::with_capacity(targets.len());
    for target in targets {
        resolved.push(resolve(&on, target).await?);
    }
    let ids: Vec<Uuid> = resolved.iter().map(|(id, _)| *id).collect();
    let key = commit_key(targets, &ids);
    if !was_applied(&on, key.as_deref()).await? {
        let mut prepared = Vec::with_capacity(targets.len());
        for (target, (id, table)) in targets.iter().zip(resolved) {
            match prepare(state, &on, target, id, table).await {
                Ok(one) => prepared.push(one),
                // The files may be gone because a first attempt of this commit took them, after the check above.
                Err(ApplyError::Refused(Refused::Conflict(_))) if was_applied(&on, key.as_deref()).await? => {
                    return tables(&on, targets).await;
                }
                Err(failure) => return Err(failure),
            }
        }
        let jobs = match commit(&on, prepared, key).await {
            Err(failure) if is_conflict(&failure) => return Err(taken(&on, targets).await.unwrap_or(failure)),
            done => done?,
        };
        for job in jobs {
            state.footers().queue(job);
        }
        if targets.iter().any(|t| t.change.changes_materialize()) {
            state.wake_materializer();
        }
    }
    tables(&on, targets).await
}

fn is_conflict(failure: &ApplyError) -> bool {
    matches!(failure, ApplyError::Core(Error::Protocol(e)) if e.kind() == ErrorKind::Conflict)
}

/// Why a commit that creates a table conflicted, when another took its name first: as Iceberg's catalogs word the
/// failed `assert-create`, which a client reads.
async fn taken(on: &Ref, targets: &[Target]) -> Option<ApplyError> {
    for target in targets.iter().filter(|t| t.change.creation.is_some()) {
        if on.table(&target.levels(), &target.name).await.is_ok() {
            let name = format!("{}.{}", target.namespace.join("."), target.name);
            let message = format!("Requirement failed: table already exists: {name}");
            return Some(Refused::Conflict(message).into());
        }
    }
    None
}

async fn was_applied(on: &Ref, key: Option<&str>) -> Result<bool, ApplyError> {
    match key {
        Some(key) => Ok(on.commit_of_key(key).await?.is_some()),
        None => Ok(false),
    }
}

async fn tables(on: &Ref, targets: &[Target]) -> Result<Vec<Table>, ApplyError> {
    let mut found = Vec::with_capacity(targets.len());
    for target in targets {
        found.push(on.table(&target.levels(), &target.name).await?);
    }
    Ok(found)
}

/// Commits, and answers the files it added with no footer, for the worker to keep theirs (FTR-1).
async fn commit(on: &Ref, prepared: Vec<Prepared>, key: Option<String>) -> Result<Vec<Job>, ApplyError> {
    let idle = |p: &Prepared| p.adds.is_empty() && p.removed.is_empty() && p.definition.is_none();
    if key.is_none() && prepared.iter().all(idle) {
        return Ok(Vec::new());
    }
    let jobs: Vec<Job> = prepared
        .iter()
        .map(|p| Job {
            table_id: p.table_id,
            files: p
                .adds
                .iter()
                .filter(|f| f.footer_size == 0 && f.footer.is_none())
                .filter_map(|f| Some((f.path.clone()?, u64::try_from(f.file_size).ok()?)))
                .collect(),
        })
        .collect();
    let replaces = prepared.iter().all(|p| p.replace);
    let mut transaction = on.transaction().author("iceberg");
    for one in prepared {
        transaction = transaction.append(one.adds);
        if let Some(table) = &one.table {
            transaction = transaction.remove(table, one.removed);
        }
        if let Some(definition) = one.definition {
            transaction = transaction.define(one.table_id, definition);
        }
        if let Some(expected) = one.expected {
            transaction = transaction.expect_snapshot(one.table_id, expected);
        }
        if !one.summary.is_empty() {
            transaction = transaction.summary(one.table_id, one.summary);
        }
    }
    if replaces {
        transaction = transaction.replace();
    }
    if let Some(key) = key {
        transaction = transaction.idempotent(key, APP_VERSION);
    }
    transaction.commit().await?;
    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::wire::commit::ProposedSnapshot;

    fn target(snapshot: i64) -> Target {
        let proposed = ProposedSnapshot {
            snapshot_id: snapshot,
            parent_snapshot_id: None,
            manifest_list: String::new(),
            summary: BTreeMap::new(),
        };
        Target {
            namespace: vec!["db".to_string()],
            name: "t".to_string(),
            change: Change {
                ref_name: "main".to_string(),
                expected: None,
                table_uuid: None,
                creation: None,
                snapshots: vec![proposed],
                properties: BTreeMap::new(),
                asserted: Vec::new(),
            },
        }
    }

    #[test]
    fn rest9_commit_key_independent_of_order() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let one = commit_key(&[target(5), target(9)], &[a, b]);
        let other = commit_key(&[target(9), target(5)], &[b, a]);
        assert_eq!(one, other);
        assert_eq!(one, Some(format!("iceberg:{a}:5")));
    }

    #[test]
    fn properties_only_commit_has_no_key() {
        let mut only = target(1);
        only.change.snapshots.clear();
        assert_eq!(commit_key(&[only], &[Uuid::from_u128(1)]), None);
    }

    #[test]
    fn field_types_of_definition() {
        let definition = json!({"fields": [{"field_id": 1, "type": "long"}, {"field_id": 4, "type": "list"}]});
        let types = field_types(&definition);
        assert_eq!(types.len(), 2);
        assert_eq!(types[&4], "list");
        assert!(field_types(&json!({})).is_empty());
    }

    #[test]
    fn ice5_inline_path_by_kind() {
        let location = "s3://b/t/";
        let removed = [
            "s3://b/t/metalake-inline/42.parquet".to_string(),
            "s3://b/t/data/a.parquet".to_string(),
            "s3://b/other/metalake-inline/7.parquet".to_string(),
            "s3://b/t/metalake-inline/x.parquet".to_string(),
        ];
        let (paths, ids) = by_kind(location, &removed);
        assert_eq!(ids, [42]);
        assert_eq!(paths, removed[1..]);
    }

    #[test]
    fn ice6_removed_paths_resolved() {
        let paths = ["a".to_string(), "b".to_string()];
        let found = [(Some("b".to_string()), 2), (Some("a".to_string()), 1), (None, 9)];
        assert_eq!(resolved(&paths, &[9], &found), Some(vec![1, 2, 9]));
    }

    #[test]
    fn cmt7_invisible_path_unresolved() {
        let found = [(Some("a".to_string()), 1)];
        assert_eq!(resolved(&["a".to_string(), "gone".to_string()], &[], &found), None);
        assert_eq!(resolved(&[], &[9], &found), None);
        assert_eq!(resolved(&[], &[], &[]), Some(Vec::new()));
    }
}
