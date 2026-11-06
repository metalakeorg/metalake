//! A table as `loadTable` answers it (ICE-1).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// None for a staged table, which has no metadata yet (REST-7).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub config: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schemas: Vec<Value>,
    pub current_schema_id: i64,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    pub sort_orders: Vec<SortOrder>,
    pub default_sort_order_id: i32,
    pub properties: BTreeMap<String, String>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<Snapshot>,
    pub refs: BTreeMap<String, SnapshotRef>,
    pub snapshot_log: Vec<Value>,
    pub metadata_log: Vec<Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    pub transform: String,
    pub source_id: i32,
    pub direction: String,
    pub null_order: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub schema_id: i64,
    pub summary: BTreeMap<String, String>,
    pub manifest_list: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}
