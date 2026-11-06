//! The rows of a table as `TableMetadata` (ICE-1). The reference's `iceberg_metadata` is the oracle this is tested against.

use std::collections::{BTreeMap, HashMap, HashSet};

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

/// A snapshot's `operation`, as Iceberg names what it does: it has no merge, and what is not its own is an append.
fn operation(operation: Option<&str>) -> &'static str {
    match operation {
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
    said.insert("operation".into(), operation(of.summary["operation"].as_str()).into());
    said
}

/// How many ancestors of each ref's current snapshot are served, so that a client that rebases finds its base (REST-5).
const ANCESTORS: usize = 10;

/// The snapshots a load serves: each ref's current one and its last ten ancestors that remain, whatever a client asks.
pub fn served(snapshots: Vec<TableSnapshot>, refs: &[(String, String, Option<i64>)]) -> Vec<TableSnapshot> {
    let parents: HashMap<i64, Option<i64>> = snapshots
        .iter()
        .map(|s| (s.snapshot_id, s.parent_snapshot_id))
        .collect();
    let mut kept = HashSet::new();
    for (.., current) in refs {
        let mut at = *current;
        for _ in 0..=ANCESTORS {
            let Some(id) = at.filter(|id| parents.contains_key(id)) else {
                break;
            };
            kept.insert(id);
            at = parents[&id];
        }
    }
    let mut served = snapshots;
    served.retain(|s| kept.contains(&s.snapshot_id));
    served
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

/// The table's metadata, each snapshot naming the manifest list `manifest_list` gives it.
pub fn metadata(rows: &Rows, manifest_list: impl Fn(i64) -> String) -> TableMetadata {
    let d = &rows.definition;
    let current = d["snapshot_id"].as_i64();
    let current_schema = rows
        .snapshots
        .iter()
        .find(|s| Some(s.snapshot_id) == current)
        .map(|s| s.schema_seq);
    let properties = d["properties"].as_object().into_iter().flatten();
    TableMetadata {
        format_version: 2,
        table_uuid: rows.table_id.clone(),
        location: d["location"].as_str().unwrap_or_default().to_string(),
        last_sequence_number: rows.snapshots.iter().map(|s| s.snapshot_id).max().unwrap_or(0),
        last_updated_ms: rows.snapshots.iter().filter_map(|s| s.timestamp_ms).max().unwrap_or(0),
        last_column_id: int(&d["last_field_id"]),
        schemas: rows.schemas.iter().map(|(id, fields)| schema(*id, fields)).collect(),
        current_schema_id: current_schema.unwrap_or(0),
        partition_specs: specs(d),
        default_spec_id: int(&d["spec_id"]),
        last_partition_id: int(&d["last_partition_field_id"]),
        sort_orders: sort_orders(d),
        default_sort_order_id: int(&d["sort_order_id"]),
        properties: properties
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .chain(COPY_ON_WRITE.map(|key| (key.to_string(), "copy-on-write".to_string())))
            .collect(),
        current_snapshot_id: current,
        snapshots: rows
            .snapshots
            .iter()
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
                let snapshot_id = (*id)?;
                Some((
                    name.clone(),
                    SnapshotRef {
                        snapshot_id,
                        kind: kind.clone(),
                    },
                ))
            })
            .collect(),
        snapshot_log: Vec::new(),
        metadata_log: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(ids: std::ops::RangeInclusive<i64>) -> Vec<TableSnapshot> {
        ids.map(|id| TableSnapshot {
            snapshot_id: id,
            parent_snapshot_id: (id > 1).then_some(id - 1),
            ref_name: "main".into(),
            schema_seq: 1,
            summary: Value::Null,
            timestamp_ms: None,
        })
        .collect()
    }

    fn ids(served: Vec<TableSnapshot>) -> Vec<i64> {
        served.iter().map(|s| s.snapshot_id).collect()
    }

    #[test]
    fn rest5_ancestor_limit() {
        let refs = [("main".to_string(), "branch".to_string(), Some(20))];
        assert_eq!(ids(served(chain(1..=20), &refs)), (10..=20).collect::<Vec<_>>());
    }

    #[test]
    fn rest5_ancestors_per_ref() {
        let refs = [
            ("main".to_string(), "branch".to_string(), Some(20)),
            ("old".to_string(), "tag".to_string(), Some(5)),
            ("empty".to_string(), "branch".to_string(), None),
        ];
        let kept = ids(served(chain(3..=20), &refs));
        assert_eq!(kept[..3], [3, 4, 5]);
        assert_eq!(kept[3], 10);
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
}
