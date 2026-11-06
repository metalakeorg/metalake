//! One `updateTable` as one commit of the protocol (ICE-6), on what `update` and `files` established.

use std::collections::HashMap;

use metalake::{Error, FileAdd, Table};
use serde_json::Value as Json;
use uuid::Uuid;

use super::files::{self, ManifestFile};
use super::update::{Change, Creation};
use super::{ApplyError, Refused, create};
use crate::state::State;

/// The version of every Iceberg idempotency key: a key names one proposed snapshot (REST-3).
const APP_VERSION: i64 = 1;

fn not_yet(what: &str) -> Refused {
    Refused::Invalid(format!("{what} is not served yet"))
}

/// The type of each field of a definition.
fn field_types(definition: &Json) -> HashMap<i32, String> {
    let fields = definition["fields"].as_array().map_or(&[][..], Vec::as_slice);
    let typed = |f: &Json| Some((f["field_id"].as_i64()? as i32, f["type"].as_str()?.to_string()));
    fields.iter().filter_map(typed).collect()
}

fn file_adds(
    table_id: Uuid,
    definition: &Json,
    files: &[ManifestFile],
    footers: &[i32],
) -> Result<Vec<FileAdd>, Refused> {
    let types = field_types(definition);
    let add = |(file, footer): (&ManifestFile, &i32)| {
        let spec = definition["specs"][file.spec_id.to_string()].as_array();
        let spec = spec.ok_or_else(|| Refused::Invalid(format!("no partition spec {}", file.spec_id)))?;
        files::file_add(table_id, file, *footer, spec, &types)
    };
    files.iter().zip(footers).map(add).collect()
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

/// The commit a stage-create ends in: the definition and the first files, so that a table and its rows appear together
/// (REST-7). The client's id makes the key the same across attempts.
async fn create_with_files(
    state: &State,
    namespace: &[&str],
    name: &str,
    change: &Change,
    creation: &Creation,
) -> Result<Table, ApplyError> {
    let on = state.named(&change.ref_name).await?;
    let table_id = Uuid::parse_str(&creation.table_id)
        .map_err(|_| Refused::Invalid(format!("{} is not a table uuid", creation.table_id)))?;
    let key = change.app_id(&creation.table_id).expect("a creation has a key");
    if on.commit_of_key(&key).await?.is_some() {
        return Ok(on.table(namespace, name).await?);
    }
    let definition = create::definition(namespace, name, creation, &change.properties)?;
    let storage = state.storage()?;
    let proposed = files::proposed(&storage, &change.snapshots).await?;
    if !proposed.removed.is_empty() {
        return Err(Refused::Invalid("a commit that creates a table removes no files".into()).into());
    }
    let footers = files::footer_sizes(&storage, &proposed.added).await?;
    let adds = file_adds(table_id, &create::described(&definition), &proposed.added, &footers)?;
    on.transaction()
        .define(table_id, definition)
        .append(adds)
        .author("iceberg")
        .idempotent(key, APP_VERSION)
        .commit()
        .await?;
    Ok(on.table(namespace, name).await?)
}

/// Commits a change to the table `name` of `namespace`, and answers the table. A key already applied answers at once,
/// whatever the client has since done with its files (REST-3).
pub async fn apply(state: &State, namespace: &[&str], name: &str, change: &Change) -> Result<Table, ApplyError> {
    if let Some(creation) = &change.creation {
        return create_with_files(state, namespace, name, change, creation).await;
    }
    if !change.properties.is_empty() {
        return Err(not_yet("a change of properties").into());
    }
    let on = state.named(&change.ref_name).await.map_err(|e| match e {
        Error::NotFound(_) => ApplyError::from(Refused::Invalid(format!("no ref {}", change.ref_name))),
        other => other.into(),
    })?;
    let table = on.table(namespace, name).await?;
    change.check_table(&table.id().to_string())?;
    if change.snapshots.is_empty() {
        return Ok(table);
    }
    let key = change.app_id(&table.id().to_string());
    if let Some(key) = &key
        && on.commit_of_key(key).await?.is_some()
    {
        return Ok(table);
    }

    let storage = state.storage()?;
    let proposed = files::proposed(&storage, &change.snapshots).await?;
    let (paths, ids) = by_kind(table.location(), &proposed.removed);
    let found = match proposed.removed.is_empty() {
        true => Vec::new(),
        false => on.files_at_paths(&table, &paths, &ids).await?,
    };
    let Some(removed) = resolved(&paths, &ids, &found) else {
        // The files may be gone because a first attempt of this commit took them, after the check above.
        if let Some(key) = &key
            && on.commit_of_key(key).await?.is_some()
        {
            return Ok(table);
        }
        return Err(Refused::Conflict("a file the commit removes is not visible on the ref".into()).into());
    };
    let footers = files::footer_sizes(&storage, &proposed.added).await?;
    let definition = on.definition(&table, None).await?;
    let adds = file_adds(table.id(), &definition, &proposed.added, &footers)?;

    let mut transaction = on.transaction().append(adds).remove(&table, removed).author("iceberg");
    if change.operation() == Some("replace") {
        transaction = transaction.replace();
    }
    if let Some(app_id) = key {
        transaction = transaction.idempotent(app_id, APP_VERSION);
    }
    if let Some(expected) = change.expected {
        transaction = transaction.expect_snapshot(table.id(), expected);
    }
    transaction.commit().await?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
