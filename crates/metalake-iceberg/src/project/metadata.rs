//! The rows of a table as `TableMetadata` (ICE-1). The reference's `iceberg_metadata` is the oracle this is tested against.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use metalake::TableSnapshot;
use serde_json::{Value, json};

use crate::wire::metadata::{
    PartitionField, PartitionSpec, Snapshot, SnapshotRef, SortField, SortOrder, TableMetadata,
};

/// What the catalog says of a table.
#[derive(Debug)]
pub struct Rows {
    pub table_id: String,
    /// `describe` of the table on `main`, at its latest.
    pub definition: Value,
    /// Each schema served, by its `schema_seq`: the `fields` of `describe` at a snapshot that has it.
    pub schemas: BTreeMap<i64, Value>,
    pub snapshots: Vec<TableSnapshot>,
    /// Each ref that sees the table: its name, its kind and its current snapshot.
    pub refs: Vec<(String, String, Option<i64>)>,
}

/// A malformed row: the catalog broke its own format.
#[derive(Debug)]
pub struct Malformed(pub String);

fn field_type(fields: &[Value], field: &Value) -> Value {
    let id = &field["field_id"];
    let child = |name: &str| {
        fields
            .iter()
            .find(|f| &f["parent_field_id"] == id && f["name"] == name)
            .expect("a list or a map has its own fields (TYP-2)")
    };
    match field["type"].as_str() {
        Some("struct") => struct_of(fields, id),
        Some("list") => {
            let element = child("element");
            json!({"type": "list", "element-id": element["field_id"], "element-required": element["required"],
                   "element": field_type(fields, element)})
        }
        Some("map") => {
            let (key, value) = (child("key"), child("value"));
            json!({"type": "map", "key-id": key["field_id"], "key": field_type(fields, key),
                   "value-id": value["field_id"], "value-required": value["required"],
                   "value": field_type(fields, value)})
        }
        _ => field["type"].clone(),
    }
}

fn struct_of(fields: &[Value], parent: &Value) -> Value {
    let mut children: Vec<&Value> = fields.iter().filter(|f| &f["parent_field_id"] == parent).collect();
    children.sort_by_key(|f| f["position"].as_i64());
    let rendered: Vec<Value> = children
        .into_iter()
        .map(|f| {
            let mut one = json!({"id": f["field_id"], "name": f["name"], "required": f["required"],
                                 "type": field_type(fields, f)});
            for (key, from) in [
                ("doc", "doc"),
                ("initial-default", "initial_default"),
                ("write-default", "write_default"),
            ] {
                if !f[from].is_null() {
                    one[key] = f[from].clone();
                }
            }
            one
        })
        .collect();
    json!({"type": "struct", "fields": rendered})
}

/// A schema of TYP-6, from the fields of `describe`, with its id.
pub fn schema(id: i64, fields: &Value) -> Value {
    let empty = Vec::new();
    let mut rendered = struct_of(fields.as_array().unwrap_or(&empty), &Value::Null);
    rendered["schema-id"] = json!(id);
    rendered
}

/// The ids of a table's definition on a ref, as a load serves them (ICE-1) and as REST-12 holds a commit to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefinitionIds {
    pub current_schema_id: i64,
    pub last_column_id: i32,
    pub last_partition_id: i32,
    pub default_spec_id: i32,
    pub default_sort_order_id: i32,
}

/// The ids of `definition`, what `describe` answers, whose current snapshot has the schema `current_schema`.
pub fn definition_ids(definition: &Value, current_schema: Option<i64>) -> DefinitionIds {
    DefinitionIds {
        current_schema_id: current_schema.unwrap_or(0),
        last_column_id: int(&definition["last_field_id"]),
        last_partition_id: int(&definition["last_partition_field_id"]),
        default_spec_id: int(&definition["spec_id"]),
        default_sort_order_id: int(&definition["sort_order_id"]),
    }
}

fn int(value: &Value) -> i32 {
    value.as_i64().unwrap_or(0) as i32
}

/// The fields of a partition spec, from the catalog's shape.
pub fn partition_fields(fields: &[Value]) -> Vec<PartitionField> {
    fields
        .iter()
        .map(|f| PartitionField {
            source_id: int(&f["source_ids"][0]),
            field_id: int(&f["field_id"]),
            name: f["name"].as_str().unwrap_or_default().to_string(),
            transform: f["transform"].as_str().unwrap_or_default().to_string(),
        })
        .collect()
}

fn specs(definition: &Value) -> Vec<PartitionSpec> {
    let empty = serde_json::Map::new();
    let mut specs: Vec<PartitionSpec> = definition["specs"]
        .as_object()
        .unwrap_or(&empty)
        .iter()
        .map(|(id, fields)| PartitionSpec {
            spec_id: id.parse().unwrap_or(0),
            fields: partition_fields(fields.as_array().map_or(&[], Vec::as_slice)),
        })
        .collect();
    specs.sort_by_key(|s| s.spec_id);
    specs
}

fn sort_orders(definition: &Value) -> Vec<SortOrder> {
    let empty = serde_json::Map::new();
    let mut orders = vec![SortOrder {
        order_id: 0,
        fields: Vec::new(),
    }];
    orders.extend(
        definition["sort_orders"]
            .as_object()
            .unwrap_or(&empty)
            .iter()
            .map(|(id, fields)| SortOrder {
                order_id: id.parse().unwrap_or(0),
                fields: fields
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|f| SortField {
                        transform: f["transform"].as_str().unwrap_or_default().to_string(),
                        source_id: int(&f["source_ids"][0]),
                        direction: f["direction"].as_str().unwrap_or_default().to_string(),
                        null_order: f["null_order"].as_str().unwrap_or_default().to_string(),
                    })
                    .collect(),
            }),
    );
    orders.sort_by_key(|o| o.order_id);
    orders
}

/// Whether a merge only added: a reader takes a snapshot's added files for all it did, and applies no deletes to them.
fn only_added(summary: &Value) -> bool {
    let none = |key: &str| summary[key].as_i64() == Some(0);
    none("deleted-data-files") && none("changed-vectors")
}

/// Whether a snapshot's files are its parent's and those it added, as that schema has them: its manifests are then those
/// of its parent and what it added (ICE-2).
pub fn adds_only(snapshot: &TableSnapshot) -> bool {
    only_added(&snapshot.summary) && snapshot.schema_seq != snapshot.snapshot_id
}

/// A snapshot's `operation`, as Iceberg names what it does: it has no merge, and what is not its own is an append.
fn operation(summary: &Value) -> &'static str {
    match summary["operation"].as_str() {
        Some("merge") if only_added(summary) => "append",
        Some("merge" | "rebase" | "overwrite") => "overwrite",
        Some("replace") => "replace",
        Some("delete") => "delete",
        _ => "append",
    }
}

fn summary(of: &TableSnapshot) -> BTreeMap<String, String> {
    let mut said: BTreeMap<String, String> = of
        .summary
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| *key != "operation")
        .map(|(key, value)| {
            (
                key.clone(),
                value.as_str().map_or_else(|| value.to_string(), str::to_string),
            )
        })
        .collect();
    said.insert("operation".into(), operation(&of.summary).into());
    said
}

/// A table's property, if it is a positive number: a table any client wrote is still served.
fn positive<T: FromStr + PartialOrd + Default>(definition: &Value, key: &str) -> Option<T> {
    let value = definition["properties"][key].as_str()?.parse().ok()?;
    (value > T::default()).then_some(value)
}

/// Which snapshots a client sees as not expired (REST-5), by Iceberg's own properties and defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expiry {
    pub keep: i32,
    pub max_age_ms: i64,
}

pub fn expiry(definition: &Value) -> Expiry {
    Expiry {
        keep: positive(definition, "history.expire.min-snapshots-to-keep").unwrap_or(1),
        max_age_ms: positive(definition, "history.expire.max-snapshot-age-ms").unwrap_or(5 * 24 * 60 * 60 * 1000),
    }
}

/// How many metadata files a table keeps before its latest (REST-6).
pub fn previous_versions(definition: &Value) -> usize {
    positive(definition, "write.metadata.previous-versions-max").unwrap_or(100)
}

/// The size a manifest stops rolling up at, as a client's merge stops there.
pub fn manifest_target_size(definition: &Value) -> i64 {
    positive(definition, "commit.manifest.target-size-bytes").unwrap_or(8 * 1024 * 1024)
}

/// The property that says when a table's Iceberg metadata is materialized (REST-6, REST-9). A table property, so it is
/// set on a ref, a branch inherits it, and it is served to a client as any other.
pub use metalake::ICEBERG_METADATA;

/// When a table's Iceberg metadata is materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Materialize {
    /// When a client loads the table, what is new since the last load (REST-6). Every table, unless marked.
    #[default]
    OnLoad,
    /// After each commit, by the service, so that a load finds it there.
    OnCommit,
}

impl Materialize {
    /// The value the property names, if it names one.
    pub fn named(value: &str) -> Option<Self> {
        match value {
            "on-load" => Some(Self::OnLoad),
            "on-commit" => Some(Self::OnCommit),
            _ => None,
        }
    }

    /// The value of a table with `definition`: on load when it is absent, and when it names nothing, as another engine
    /// may have set it, which the protocol does not check.
    pub fn of(definition: &Value) -> Self {
        let value = definition["properties"][ICEBERG_METADATA].as_str();
        value.and_then(Self::named).unwrap_or_default()
    }
}

/// The write modes every table is served with: a client that wrote position deletes would be refused (REST-4).
pub const COPY_ON_WRITE: [&str; 3] = ["write.delete.mode", "write.update.mode", "write.merge.mode"];

/// The metadata of a table that is staged: a definition in the shape of `describe`, with no snapshot and no ref.
pub fn staged(table_id: String, definition: Value) -> TableMetadata {
    let fields = definition["fields"].clone();
    let rows = Rows {
        table_id,
        definition,
        schemas: BTreeMap::from([(0, fields)]),
        snapshots: Vec::new(),
        refs: Vec::new(),
    };
    metadata(&rows, |_| String::new())
}

/// Whether a snapshot is the one a table was created with and nothing else: CMT-1 writes it, and Iceberg has no such thing,
/// so a client takes it as data already written. Trino skips its first files on seeing one.
pub fn is_empty_creation(snapshot: &TableSnapshot) -> bool {
    let added = snapshot.summary["added-data-files"].as_i64().unwrap_or(0);
    snapshot.parent_snapshot_id.is_none() && added == 0
}

/// The history of the table's current snapshot, oldest first: `main`'s, whose snapshots each follow the one before, so
/// that history is the ancestry of its current snapshot among those served (ICE-1). An engine reads a table as of a time
/// by it, at the last entry not after that time, which is the snapshot MetaLake reads at that time (TT-3). The snapshots
/// not served are expired, and their entries go with them, as Iceberg has it.
fn snapshot_log(rows: &Rows, current: Option<i64>, unwritten: Option<i64>) -> Vec<Value> {
    let served: HashMap<i64, &TableSnapshot> = rows
        .snapshots
        .iter()
        .filter(|s| Some(s.snapshot_id) != unwritten)
        .map(|s| (s.snapshot_id, s))
        .collect();
    let mut log = Vec::new();
    let mut at = current.and_then(|id| served.get(&id));
    while let Some(snapshot) = at.filter(|_| log.len() < served.len()) {
        log.push(json!({"snapshot-id": snapshot.snapshot_id, "timestamp-ms": snapshot.timestamp_ms.unwrap_or(0)}));
        at = snapshot.parent_snapshot_id.and_then(|parent| served.get(&parent));
    }
    log.reverse();
    log
}

/// The table's metadata, each snapshot naming the manifest list `manifest_list` gives it.
pub fn metadata(rows: &Rows, manifest_list: impl Fn(i64) -> String) -> TableMetadata {
    let d = &rows.definition;
    let head = rows
        .snapshots
        .iter()
        .find(|s| Some(s.snapshot_id) == d["snapshot_id"].as_i64());
    let current_schema = head.map(|s| s.schema_seq);
    // A table nothing was written to has no current snapshot, as in Iceberg (ICE-1).
    let unwritten = head.filter(|s| is_empty_creation(s)).map(|s| s.snapshot_id);
    let current = head.map(|s| s.snapshot_id).filter(|id| Some(*id) != unwritten);
    let properties = d["properties"].as_object().into_iter().flatten();
    let ids = definition_ids(d, current_schema);
    TableMetadata {
        format_version: 2,
        table_uuid: rows.table_id.clone(),
        location: d["location"].as_str().unwrap_or_default().to_string(),
        last_sequence_number: rows.snapshots.iter().map(|s| s.snapshot_id).max().unwrap_or(0),
        last_updated_ms: rows.snapshots.iter().filter_map(|s| s.timestamp_ms).max().unwrap_or(0),
        last_column_id: ids.last_column_id,
        schemas: rows.schemas.iter().map(|(id, fields)| schema(*id, fields)).collect(),
        current_schema_id: ids.current_schema_id,
        partition_specs: specs(d),
        default_spec_id: ids.default_spec_id,
        last_partition_id: ids.last_partition_id,
        sort_orders: sort_orders(d),
        default_sort_order_id: ids.default_sort_order_id,
        properties: properties
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .chain(COPY_ON_WRITE.map(|key| (key.to_string(), "copy-on-write".to_string())))
            .collect(),
        current_snapshot_id: current,
        snapshots: rows
            .snapshots
            .iter()
            .filter(|s| Some(s.snapshot_id) != unwritten)
            .map(|s| Snapshot {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                sequence_number: s.snapshot_id,
                timestamp_ms: s.timestamp_ms.unwrap_or(0),
                schema_id: s.schema_seq,
                summary: summary(s),
                manifest_list: manifest_list(s.snapshot_id),
            })
            .collect(),
        refs: rows
            .refs
            .iter()
            .filter_map(|(name, kind, id)| {
                let snapshot_id = (*id).filter(|id| Some(*id) != unwritten)?;
                Some((
                    name.clone(),
                    SnapshotRef {
                        snapshot_id,
                        kind: kind.clone(),
                    },
                ))
            })
            .collect(),
        snapshot_log: snapshot_log(rows, current, unwritten),
        // A metadata file names no other: it can always be generated again, whatever files lie beside it (REST-6).
        metadata_log: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(id: i64, parent: Option<i64>, added: i64) -> TableSnapshot {
        TableSnapshot {
            snapshot_id: id,
            parent_snapshot_id: parent,
            ref_name: "main".into(),
            schema_seq: id,
            timestamp_ms: None,
            summary: json!({"operation": "update", "added-data-files": added}),
        }
    }

    fn with_head(snapshots: Vec<TableSnapshot>, head: i64) -> Rows {
        Rows {
            table_id: "t".into(),
            definition: json!({"snapshot_id": head}),
            schemas: BTreeMap::from([(1, json!([]))]),
            snapshots,
            refs: vec![("main".to_string(), "branch".to_string(), Some(head))],
        }
    }

    #[test]
    fn ice1_merge_operation_states_what_it_did() {
        let merge = |summary: Value| operation(&summary);
        let keys = |deleted: i64, vectors: i64| json!({"operation": "merge", "deleted-data-files": deleted, "changed-vectors": vectors});
        assert_eq!(merge(keys(0, 0)), "append");
        assert_eq!(merge(keys(1, 0)), "overwrite");
        assert_eq!(merge(keys(0, 1)), "overwrite");
        assert_eq!(
            merge(json!({"operation": "merge", "deleted-data-files": 0})),
            "overwrite"
        );
        assert_eq!(
            merge(json!({"operation": "rebase", "deleted-data-files": 0, "changed-vectors": 0})),
            "overwrite"
        );
    }

    #[test]
    fn ice2_adds_only_needs_every_fact() {
        let of = |id: i64, schema_seq: i64, summary: Value| TableSnapshot {
            snapshot_id: id,
            parent_snapshot_id: Some(1),
            ref_name: "main".into(),
            schema_seq,
            timestamp_ms: None,
            summary,
        };
        let keys = |deleted: i64, vectors: i64| json!({"deleted-data-files": deleted, "changed-vectors": vectors});
        assert!(adds_only(&of(5, 1, keys(0, 0))));
        assert!(!adds_only(&of(5, 1, keys(1, 0))), "it removed a file");
        assert!(!adds_only(&of(5, 1, keys(0, 1))), "it set a vector");
        assert!(
            !adds_only(&of(5, 5, keys(0, 0))),
            "bounds are served in the current type (ICE-3)"
        );
        assert!(
            !adds_only(&of(5, 1, json!({"operation": "rebase"}))),
            "what it did is not stated"
        );
    }

    #[test]
    fn rest5_expiry_read_from_properties() {
        let of = |key: &str, value: &str| json!({"properties": {key: value}});
        assert_eq!(
            expiry(&json!({"properties": {}})),
            Expiry {
                keep: 1,
                max_age_ms: 432_000_000
            }
        );
        assert_eq!(expiry(&of("history.expire.min-snapshots-to-keep", "7")).keep, 7);
        assert_eq!(
            expiry(&of("history.expire.max-snapshot-age-ms", "60000")).max_age_ms,
            60_000
        );
        assert_eq!(expiry(&of("history.expire.min-snapshots-to-keep", "0")).keep, 1);
        assert_eq!(
            expiry(&of("history.expire.max-snapshot-age-ms", "soon")).max_age_ms,
            432_000_000
        );
        assert_eq!(previous_versions(&of("write.metadata.previous-versions-max", "3")), 3);
        assert_eq!(previous_versions(&json!({"properties": {}})), 100);
        assert_eq!(
            manifest_target_size(&of("commit.manifest.target-size-bytes", "1024")),
            1024
        );
    }

    #[test]
    fn ice1_unwritten_table_has_no_snapshot() {
        let served = metadata(&with_head(vec![snapshot(1, None, 0)], 1), |_| String::new());
        assert_eq!(served.current_snapshot_id, None);
        assert!(served.snapshots.is_empty() && served.refs.is_empty());
        assert_eq!(served.current_schema_id, 1, "its schema is still served");
    }

    #[test]
    fn ice1_written_table_keeps_its_snapshots() {
        let written = metadata(&with_head(vec![snapshot(1, None, 2)], 1), |_| String::new());
        assert_eq!(written.current_snapshot_id, Some(1));
        let chain = vec![snapshot(1, None, 0), snapshot(2, Some(1), 1)];
        let served = metadata(&with_head(chain, 2), |_| String::new());
        assert_eq!(served.current_snapshot_id, Some(2));
        assert_eq!(served.snapshots.len(), 2);
    }

    #[test]
    fn ice1_snapshot_log_follows_main_ancestry() {
        let at = |id: i64, parent: Option<i64>, ms: i64| TableSnapshot {
            timestamp_ms: Some(ms),
            ..snapshot(id, parent, 1)
        };
        let log = |rows: &Rows| metadata(rows, |_| String::new()).snapshot_log;
        // 2 was expired, so 3 heads what is served; 6 is a branch's, off 4.
        let chain = vec![
            at(3, Some(2), 300),
            at(4, Some(3), 400),
            at(5, Some(4), 500),
            at(6, Some(4), 600),
        ];
        assert_eq!(
            log(&with_head(chain, 5)),
            vec![
                json!({"snapshot-id": 3, "timestamp-ms": 300}),
                json!({"snapshot-id": 4, "timestamp-ms": 400}),
                json!({"snapshot-id": 5, "timestamp-ms": 500}),
            ]
        );
        let creation = TableSnapshot {
            timestamp_ms: Some(100),
            ..snapshot(1, None, 0)
        };
        let created = vec![creation, at(2, Some(1), 200)];
        assert_eq!(
            log(&with_head(created.clone(), 2)),
            vec![
                json!({"snapshot-id": 1, "timestamp-ms": 100}),
                json!({"snapshot-id": 2, "timestamp-ms": 200})
            ],
            "once written to, its creation is served as any snapshot"
        );
        assert!(
            log(&with_head(created, 1)).is_empty(),
            "an unwritten table has no history"
        );
    }

    #[test]
    fn rest7_staged_metadata() {
        let definition = json!({"location": "memory:///t", "last_field_id": 1, "last_partition_field_id": 999,
                                "fields": [{"field_id": 1, "parent_field_id": null, "position": 1, "name": "k",
                                            "type": "long", "required": true}],
                                "spec_id": 0, "specs": {"0": []}, "sort_order_id": 0, "properties": {}});
        let served = staged("t".into(), definition);
        assert_eq!(served.current_snapshot_id, None);
        assert!(served.snapshots.is_empty() && served.refs.is_empty());
        assert_eq!((served.current_schema_id, served.last_column_id), (0, 1));
        assert_eq!(served.schemas[0]["fields"][0]["name"], "k");
        assert_eq!(served.properties["write.merge.mode"], "copy-on-write");
    }

    #[test]
    fn rest4_copy_on_write_override() {
        let rows = Rows {
            table_id: "t".into(),
            definition: json!({"properties": {"write.delete.mode": "merge-on-read", "a": "b"}}),
            schemas: BTreeMap::new(),
            snapshots: Vec::new(),
            refs: Vec::new(),
        };
        let served = metadata(&rows, |_| String::new());
        assert_eq!(served.properties["write.delete.mode"], "copy-on-write");
        assert_eq!(served.properties["write.merge.mode"], "copy-on-write");
        assert_eq!(served.properties["a"], "b");
    }

    #[test]
    fn rest9_materialize_default_on_load() {
        let of = |properties: Value| Materialize::of(&json!({ "properties": properties }));
        assert_eq!(of(json!({})), Materialize::OnLoad, "absent");
        assert_eq!(of(json!({ICEBERG_METADATA: "on-load"})), Materialize::OnLoad);
        assert_eq!(of(json!({ICEBERG_METADATA: "on-commit"})), Materialize::OnCommit);
        assert_eq!(
            of(json!({ICEBERG_METADATA: "On-Commit"})),
            Materialize::OnLoad,
            "a value it does not name"
        );
        assert_eq!(Materialize::of(&json!({})), Materialize::OnLoad, "no properties");

        // served as any other property, to a client that ignores what it does not know
        let rows = Rows {
            table_id: "t".into(),
            definition: json!({"properties": {ICEBERG_METADATA: "on-commit"}}),
            schemas: BTreeMap::new(),
            snapshots: Vec::new(),
            refs: Vec::new(),
        };
        assert_eq!(
            metadata(&rows, |_| String::new()).properties[ICEBERG_METADATA],
            "on-commit"
        );
    }
}
