//! A table at a read: an immutable value, built from the protocol's `describe` (TBL-1, VIS-9, §6, §7).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
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
#[derive(Debug, Clone)]
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
