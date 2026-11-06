//! What a client sends to change a catalog: `updateTable`, `createTable`, `renameTable` and `transactions/commit`, in the
//! OpenAPI's names. What is not listed here is kept by name, so that a refusal can say what it refused.

use std::collections::BTreeMap;

use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TableIdentifier {
    #[serde(default)]
    pub namespace: Vec<String>,
    pub name: String,
}

/// An entry of a list of requirements or updates: what the client calls it, and what it says if it is known here.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry<T> {
    pub name: String,
    pub known: T,
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Entry<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let name = ["action", "type"]
            .iter()
            .find_map(|tag| value.get(tag).and_then(Value::as_str))
            .ok_or_else(|| D::Error::custom("an entry names no action or type"))?
            .to_string();
        let known = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self { name, known })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", rename_all_fields = "kebab-case")]
pub enum Requirement {
    AssertCreate,
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        reference: String,
        #[serde(default)]
        snapshot_id: Option<i64>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", rename_all_fields = "kebab-case")]
pub enum Update {
    AssignUuid {
        uuid: String,
    },
    UpgradeFormatVersion {
        format_version: i32,
    },
    AddSchema {
        schema: Value,
    },
    SetCurrentSchema {},
    AddSpec {
        spec: Value,
    },
    SetDefaultSpec {},
    AddSortOrder {
        sort_order: Value,
    },
    SetDefaultSortOrder {},
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: BTreeMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    AddSnapshot {
        snapshot: ProposedSnapshot,
    },
    SetSnapshotRef {
        ref_name: String,
        #[serde(rename = "type")]
        kind: String,
        snapshot_id: i64,
    },
    #[serde(other)]
    Other,
}

/// A snapshot as a client proposes it: its id is the client's and its files are in `manifest_list`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ProposedSnapshot {
    pub snapshot_id: i64,
    #[serde(default)]
    pub parent_snapshot_id: Option<i64>,
    pub manifest_list: String,
    #[serde(default)]
    pub summary: BTreeMap<String, String>,
}

/// `updateTable`, or an entry of `transactions/commit`, which names its table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CommitTableRequest {
    #[serde(default)]
    pub identifier: Option<TableIdentifier>,
    #[serde(default)]
    pub requirements: Vec<Entry<Requirement>>,
    #[serde(default)]
    pub updates: Vec<Entry<Update>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(default)]
    pub location: Option<String>,
    pub schema: Value,
    #[serde(default)]
    pub partition_spec: Option<Value>,
    #[serde(default)]
    pub write_order: Option<Value>,
    #[serde(default)]
    pub stage_create: bool,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RenameTableRequest {
    pub source: TableIdentifier,
    pub destination: TableIdentifier,
}
