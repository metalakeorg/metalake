//! A table at a read: an immutable value, built from the protocol's `describe` (TBL-1, VIS-9, §6, §7).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::schema::arrow_schema;

/// A field of a table's definition (§6): `ty` is a type of the specification.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub field_id: i32,
    pub parent_field_id: Option<i32>,
    pub position: i32,
    pub name: String,
    pub ty: String,
    pub required: bool,
    pub doc: Option<String>,
    pub initial_default: Option<Value>,
    pub write_default: Option<Value>,
}

/// A table as one read sees it: on one ref, at one sequence. Every read of the same value sees the same table.
///
/// Serialized as the definition `describe` answered, with its ref and sequence, and read back through the same
/// derivation: a table rebuilt in another process is the one a read there would see.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "Described", try_from = "Described")]
pub struct Table {
    id: Uuid,
    ref_id: i32,
    seq: i64,
    snapshot_id: Option<i64>,
    namespace: Vec<String>,
    name: String,
    location: String,
    fields: Vec<Field>,
    schema: SchemaRef,
    spec_id: Option<i32>,
    specs: BTreeMap<i32, Value>,
    sort_order_id: Option<i32>,
    sort_orders: BTreeMap<i32, Value>,
    cluster_key: Vec<i32>,
    properties: BTreeMap<String, String>,
    /// What `describe` answered, which everything above derives from.
    definition: Value,
}

/// A table as it travels: the definition as JSON text, so that a format which does not describe itself carries it.
#[derive(Serialize, Deserialize)]
struct Described {
    ref_id: i32,
    seq: i64,
    definition: String,
}

impl From<Table> for Described {
    fn from(table: Table) -> Self {
        Self {
            ref_id: table.ref_id,
            seq: table.seq,
            definition: table.definition.to_string(),
        }
    }
}

impl TryFrom<Described> for Table {
    type Error = Error;

    fn try_from(described: Described) -> Result<Self> {
        let definition: Value = serde_json::from_str(&described.definition)
            .map_err(|e| Error::Schema(format!("a table's definition is not JSON: {e}")))?;
        Self::from_definition(described.ref_id, described.seq, &definition)
    }
}

impl Table {
    /// The table `describe` answered for a read of `ref_id` at `seq`.
    pub(crate) fn from_definition(ref_id: i32, seq: i64, d: &Value) -> Result<Self> {
        let malformed = |what: &str| Error::Schema(format!("describe answered a definition without {what}: {d}"));
        let fields: Vec<Field> = d["fields"]
            .as_array()
            .ok_or_else(|| malformed("fields"))?
            .iter()
            .map(|f| {
                Ok(Field {
                    field_id: int(&f["field_id"]).ok_or_else(|| malformed("a field id"))?,
                    parent_field_id: int(&f["parent_field_id"]),
                    position: int(&f["position"]).ok_or_else(|| malformed("a position"))?,
                    name: f["name"].as_str().ok_or_else(|| malformed("a name"))?.to_string(),
                    ty: f["type"].as_str().ok_or_else(|| malformed("a type"))?.to_string(),
                    required: f["required"].as_bool().unwrap_or(false),
                    doc: f["doc"].as_str().map(str::to_string),
                    initial_default: Some(f["initial_default"].clone()).filter(|v| !v.is_null()),
                    write_default: Some(f["write_default"].clone()).filter(|v| !v.is_null()),
                })
            })
            .collect::<Result<_>>()?;
        let by_id = |v: &Value| -> BTreeMap<i32, Value> {
            v.as_object()
                .into_iter()
                .flatten()
                .filter_map(|(k, v)| Some((k.parse().ok()?, v.clone())))
                .collect()
        };
        Ok(Self {
            id: d["table_id"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| malformed("an id"))?,
            ref_id,
            seq,
            snapshot_id: d["snapshot_id"].as_i64(),
            namespace: d["namespace"]
                .as_array()
                .ok_or_else(|| malformed("a namespace"))?
                .iter()
                .map(|n| n.as_str().map(str::to_string).ok_or_else(|| malformed("a namespace")))
                .collect::<Result<_>>()?,
            name: d["name"].as_str().ok_or_else(|| malformed("a name"))?.to_string(),
            location: d["location"]
                .as_str()
                .ok_or_else(|| malformed("a location"))?
                .to_string(),
            schema: Arc::new(arrow_schema(&fields)?),
            fields,
            spec_id: int(&d["spec_id"]),
            specs: by_id(&d["specs"]),
            sort_order_id: int(&d["sort_order_id"]),
            sort_orders: by_id(&d["sort_orders"]),
            cluster_key: d["cluster_key"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(int)
                .collect(),
            properties: d["properties"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect(),
            definition: d.clone(),
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    /// The ref the table was read on.
    pub fn ref_id(&self) -> i32 {
        self.ref_id
    }

    /// The sequence the table was read at.
    pub fn seq(&self) -> i64 {
        self.seq
    }

    /// The table's snapshot at the read, none before its first commit of data.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn namespace(&self) -> &[String] {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Where its files live.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Its fields, as the specification types them.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Its fields as Arrow, each with its id in `PARQUET:field_id`.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The default partition spec, and every spec the table has had, by id.
    pub fn spec_id(&self) -> Option<i32> {
        self.spec_id
    }

    pub fn specs(&self) -> &BTreeMap<i32, Value> {
        &self.specs
    }

    /// The default sort order, and every sort order the table has had, by id.
    pub fn sort_order_id(&self) -> Option<i32> {
        self.sort_order_id
    }

    pub fn sort_orders(&self) -> &BTreeMap<i32, Value> {
        &self.sort_orders
    }

    /// The fields the table's files are clustered by, the first at statistics position 1 (STA-3).
    pub fn cluster_key(&self) -> &[i32] {
        &self.cluster_key
    }

    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    /// The Arrow schema, owned.
    pub fn arrow_schema(&self) -> Schema {
        self.schema.as_ref().clone()
    }
}

fn int(v: &Value) -> Option<i32> {
    v.as_i64().and_then(|n| i32::try_from(n).ok())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn definition() -> Value {
        json!({
            "table_id": "6f1c2a3e-1b2c-4d5e-8f90-a1b2c3d4e5f6",
            "snapshot_id": 7,
            "namespace": ["db"],
            "name": "events",
            "location": "gs://lake/wh/db/events",
            "fields": [
                {"field_id": 1, "position": 0, "name": "id", "type": "long", "required": true},
                {"field_id": 2, "position": 1, "name": "renamed", "type": "string", "doc": "was name"}
            ],
            "spec_id": 0,
            "specs": {"0": {"fields": []}},
            "sort_order_id": 0,
            "sort_orders": {"0": {"fields": []}},
            "cluster_key": [1],
            "properties": {"inline.max-bytes": "0"}
        })
    }

    fn assert_same(a: &Table, b: &Table) {
        assert_eq!(
            (a.id(), a.ref_id(), a.seq(), a.snapshot_id()),
            (b.id(), b.ref_id(), b.seq(), b.snapshot_id())
        );
        assert_eq!(
            (a.namespace(), a.name(), a.location()),
            (b.namespace(), b.name(), b.location())
        );
        assert_eq!(a.fields(), b.fields());
        assert_eq!(a.schema(), b.schema());
        assert_eq!(
            (a.specs(), a.sort_orders(), a.cluster_key()),
            (b.specs(), b.sort_orders(), b.cluster_key())
        );
        assert_eq!(a.properties(), b.properties());
    }

    #[test]
    fn serde_round_trip_postcard() {
        let table = Table::from_definition(3, 42, &definition()).unwrap();
        let bytes = postcard::to_stdvec(&table).unwrap();
        assert_same(&table, &postcard::from_bytes(&bytes).unwrap());
    }

    #[test]
    fn serde_round_trip_json() {
        let table = Table::from_definition(3, 42, &definition()).unwrap();
        let text = serde_json::to_string(&table).unwrap();
        assert_same(&table, &serde_json::from_str(&text).unwrap());
    }

    #[test]
    fn deserialize_malformed_definition_refused() {
        let described = Described {
            ref_id: 3,
            seq: 42,
            definition: json!({"name": "events"}).to_string(),
        };
        assert!(matches!(Table::try_from(described), Err(Error::Schema(_))));
    }
}
