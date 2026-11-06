//! Commits with no files: namespaces (ICE-7), and dropping and renaming tables (REST-8). Objects follow retention, as
//! another ref may read them, so nothing here reaches storage.

use std::collections::BTreeMap;

use metalake::Table;
use serde_json::{Map, Value, json};

use super::{ApplyError, Refused};
use crate::state::State;

/// Creates a namespace with its properties (NS-1).
pub async fn create_namespace(
    state: &State,
    namespace: &[String],
    properties: &BTreeMap<String, String>,
) -> Result<(), ApplyError> {
    let change = json!({"namespace": namespace, "create": true, "properties": properties});
    let transaction = state.main().transaction().change_namespace(change);
    transaction.author("iceberg").commit().await?;
    Ok(())
}

/// A namespace's property changes, a removal being a null (NS-4). A key both set and removed has no meaning.
pub fn property_changes(
    updates: &BTreeMap<String, String>,
    removals: &[String],
) -> Result<Map<String, Value>, Refused> {
    let mut changes: Map<String, Value> = updates.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    for key in removals {
        if updates.contains_key(key) {
            return Err(Refused::Invalid(format!("{key} is both set and removed")));
        }
        changes.insert(key.clone(), Value::Null);
    }
    Ok(changes)
}

/// What `updateProperties` answers: `current` is the namespace's properties before the change.
pub fn update_summary(current: &Value, updates: &BTreeMap<String, String>, removals: &[String]) -> Value {
    let (removed, missing): (Vec<&String>, Vec<&String>) = removals.iter().partition(|key| !current[key].is_null());
    let updated: Vec<&String> = updates.keys().collect();
    json!({"updated": updated, "removed": removed, "missing": missing})
}

/// Applies property changes to a namespace, unless there are none.
pub async fn update_namespace(
    state: &State,
    namespace: &[&str],
    changes: Map<String, Value>,
) -> Result<(), ApplyError> {
    if changes.is_empty() {
        return Ok(());
    }
    let change = json!({"namespace": namespace, "properties": changes});
    let transaction = state.main().transaction().change_namespace(change);
    transaction.author("iceberg").commit().await?;
    Ok(())
}

/// Drops a namespace. One that is not empty is a conflict the protocol raises (NS-3).
pub async fn drop_namespace(state: &State, namespace: &[&str]) -> Result<(), ApplyError> {
    let change = json!({"namespace": namespace, "drop": true});
    let transaction = state.main().transaction().change_namespace(change);
    transaction.author("iceberg").commit().await?;
    Ok(())
}

/// Drops a table on `main`. A purge changes nothing: its objects are retention's (REST-8).
pub async fn drop_table(state: &State, table: &Table) -> Result<(), ApplyError> {
    let transaction = state.main().transaction().define(table.id(), json!({"drop": true}));
    transaction.author("iceberg").commit().await?;
    Ok(())
}

/// Renames a table on `main`, which moves no data.
pub async fn rename_table(state: &State, table: &Table, namespace: &[String], name: &str) -> Result<(), ApplyError> {
    let rename = json!({"rename": {"namespace": namespace, "name": name}});
    let transaction = state.main().transaction().define(table.id(), rename);
    transaction.author("iceberg").commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn updates() -> BTreeMap<String, String> {
        BTreeMap::from([("a".to_string(), "1".to_string())])
    }

    #[test]
    fn ns4_property_changes() {
        let changes = property_changes(&updates(), &["b".to_string()]).unwrap();
        assert_eq!(Value::Object(changes), json!({"a": "1", "b": null}));
    }

    #[test]
    fn ns4_key_set_and_removed_refused() {
        assert!(property_changes(&updates(), &["a".to_string()]).is_err());
    }

    #[test]
    fn ice7_update_summary() {
        let current = json!({"b": "2"});
        let summary = update_summary(&current, &updates(), &["b".to_string(), "c".to_string()]);
        assert_eq!(summary, json!({"updated": ["a"], "removed": ["b"], "missing": ["c"]}));
    }
}
