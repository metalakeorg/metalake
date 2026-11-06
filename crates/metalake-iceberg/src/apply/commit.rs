//! One `updateTable` as one commit of the protocol (ICE-6), on what `update` and `files` established.

use std::collections::HashMap;

use metalake::{Error, FileAdd, Table};
use serde_json::Value as Json;

use super::files::{self, ManifestFile};
use super::update::Change;
use super::{ApplyError, Refused};
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
    table: &Table,
    definition: &Json,
    files: &[ManifestFile],
    footers: &[i32],
) -> Result<Vec<FileAdd>, Refused> {
    let types = field_types(definition);
    let add = |(file, footer): (&ManifestFile, &i32)| {
        let spec = definition["specs"][file.spec_id.to_string()].as_array();
        let spec = spec.ok_or_else(|| Refused::Invalid(format!("no partition spec {}", file.spec_id)))?;
        files::file_add(table.id(), file, *footer, spec, &types)
    };
    files.iter().zip(footers).map(add).collect()
}

/// Commits a change to the table `name` of `namespace`, and answers the table. Committing twice under one key applies
/// nothing the second time (REST-3).
pub async fn apply(state: &State, namespace: &[&str], name: &str, change: &Change) -> Result<Table, ApplyError> {
    if change.creation.is_some() {
        return Err(not_yet("assert-create").into());
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

    let storage = state.storage()?;
    let proposed = files::proposed(&storage, &change.snapshots).await?;
    if !proposed.removed.is_empty() {
        return Err(not_yet("removing files").into());
    }
    let footers = files::footer_sizes(&storage, &proposed.added).await?;
    let definition = on.definition(&table, None).await?;
    let adds = file_adds(&table, &definition, &proposed.added, &footers)?;

    let mut transaction = on.transaction().append(adds).author("iceberg");
    if let Some(app_id) = change.app_id(&table.id().to_string()) {
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
}
