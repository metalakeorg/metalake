//! Translates a table's Iceberg definition to the protocol's (REST-7): the schema tree to field rows, and the spec and sort
//! order to their catalog shape. `project::metadata::schema` is the inverse, and is what the tests hold this to.

use std::collections::BTreeMap;

use metalake::Table;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::update::{Creation, check_property};
use super::{ApplyError, Refused};
use crate::state::State;
use crate::wire::commit::CreateTableRequest;

/// Where a partition field's ids start when a table has none: Iceberg's convention.
const NO_PARTITION_FIELD: i64 = 999;

fn invalid(message: impl Into<String>) -> Refused {
    Refused::Invalid(message.into())
}

fn number(value: &Value, what: &str) -> Result<i64, Refused> {
    value.as_i64().ok_or_else(|| invalid(format!("a schema has no {what}")))
}

/// One field and its descendants, as rows, which is what a list's element and a map's key and value are too.
fn push_field(
    rows: &mut Vec<Value>,
    parent: Option<i64>,
    position: usize,
    (id, name, required): (i64, &str, bool),
    field_type: &Value,
) -> Result<(), Refused> {
    let kind = match field_type {
        Value::String(name) => name.as_str(),
        Value::Object(_) => field_type["type"].as_str().unwrap_or_default(),
        _ => "",
    };
    if kind.is_empty() {
        return Err(invalid(format!("field {name} has no type")));
    }
    rows.push(
        json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name,
                     "type": kind, "required": required}),
    );
    match kind {
        "struct" => push_fields(rows, Some(id), &field_type["fields"]),
        "list" => {
            let element = number(&field_type["element-id"], "element-id")?;
            let required = field_type["element-required"].as_bool().unwrap_or(false);
            push_field(
                rows,
                Some(id),
                1,
                (element, "element", required),
                &field_type["element"],
            )
        }
        "map" => {
            let key = number(&field_type["key-id"], "key-id")?;
            let value = number(&field_type["value-id"], "value-id")?;
            let required = field_type["value-required"].as_bool().unwrap_or(false);
            push_field(rows, Some(id), 1, (key, "key", true), &field_type["key"])?;
            push_field(rows, Some(id), 2, (value, "value", required), &field_type["value"])
        }
        _ => Ok(()),
    }
}

fn push_fields(rows: &mut Vec<Value>, parent: Option<i64>, fields: &Value) -> Result<(), Refused> {
    let fields = fields.as_array().ok_or_else(|| invalid("a struct has no fields"))?;
    for (at, field) in fields.iter().enumerate() {
        if !field["initial-default"].is_null() || !field["write-default"].is_null() {
            return Err(invalid("default values are not served: tables are format 2"));
        }
        let name = field["name"].as_str().ok_or_else(|| invalid("a field has no name"))?;
        let required = field["required"].as_bool().unwrap_or(false);
        let id = number(&field["id"], "id")?;
        push_field(rows, parent, at + 1, (id, name, required), &field["type"])?;
        if !field["doc"].is_null() {
            let row = rows.iter_mut().find(|row| row["field_id"] == id);
            row.expect("just pushed")["doc"] = field["doc"].clone();
        }
    }
    Ok(())
}

/// A schema as field rows, with a table's ids as the client assigned them (CMT-9).
pub fn field_rows(schema: &Value) -> Result<Vec<Value>, Refused> {
    let identified = schema["identifier-field-ids"]
        .as_array()
        .is_some_and(|ids| !ids.is_empty());
    if identified {
        return Err(invalid("identifier fields are not served"));
    }
    let mut rows = Vec::new();
    push_fields(&mut rows, None, &schema["fields"])?;
    Ok(rows)
}

fn partition_fields(spec: &Value) -> Result<Vec<Value>, Refused> {
    if spec["spec-id"].as_i64().unwrap_or(0) != 0 {
        return Err(invalid("a new table's partition spec is spec 0"));
    }
    let fields = spec["fields"].as_array().map_or(&[][..], Vec::as_slice);
    Ok(fields
        .iter()
        .map(|f| {
            json!({"source_ids": [f["source-id"]], "field_id": f["field-id"], "name": f["name"],
                   "transform": f["transform"]})
        })
        .collect())
}

fn sort_fields(order: &Value) -> Vec<Value> {
    let fields = order["fields"].as_array().map_or(&[][..], Vec::as_slice);
    fields
        .iter()
        .map(|f| {
            json!({"source_ids": [f["source-id"]], "transform": f["transform"], "direction": f["direction"],
                   "null_order": f["null-order"]})
        })
        .collect()
}

/// Property changes in the protocol's shape, a removal being a null. `format-version` is the server's to serve, so it is
/// checked and not stored.
pub(super) fn properties(given: &BTreeMap<String, Option<String>>) -> Result<Map<String, Value>, Refused> {
    let mut kept = Map::new();
    for (key, value) in given {
        let Some(value) = value else {
            kept.insert(key.clone(), Value::Null);
            continue;
        };
        if key == "format-version" {
            if value != "2" {
                return Err(invalid("tables are served in format 2"));
            }
            continue;
        }
        check_property(key, value)?;
        kept.insert(key.clone(), json!(value));
    }
    Ok(kept)
}

/// What the protocol commits to create the table `name` of `namespace`.
pub fn definition(
    namespace: &[&str],
    name: &str,
    creation: &Creation,
    given: &BTreeMap<String, Option<String>>,
) -> Result<Value, Refused> {
    let mut definition = json!({
        "create": {"namespace": namespace, "name": name, "location": creation.location},
        "fields": field_rows(&creation.schema)?,
    });
    let spec = partition_fields(&creation.spec)?;
    if !spec.is_empty() {
        definition["partition_spec"] = Value::Array(spec);
    }
    let order = creation.sort_order.as_ref().map_or_else(Vec::new, sort_fields);
    if !order.is_empty() {
        definition["sort_order"] = Value::Array(order);
    }
    let mut kept = properties(given)?;
    kept.retain(|_, value| !value.is_null());
    if !kept.is_empty() {
        definition["properties"] = Value::Object(kept);
    }
    Ok(definition)
}

/// A definition about to be committed, in the shape of `describe` after its commit.
pub fn described(definition: &Value) -> Value {
    let fields = definition["fields"].as_array().map_or(&[][..], Vec::as_slice);
    let last = |rows: &[Value], key: &str| rows.iter().filter_map(|r| r[key].as_i64()).max();
    let spec = definition["partition_spec"].as_array().map_or(&[][..], Vec::as_slice);
    let mut sort_orders = Map::new();
    if !definition["sort_order"].is_null() {
        sort_orders.insert("1".to_string(), definition["sort_order"].clone());
    }
    json!({
        "location": definition["create"]["location"],
        "fields": fields,
        "last_field_id": last(fields, "field_id").unwrap_or(0),
        "last_partition_field_id": last(spec, "field_id").unwrap_or(NO_PARTITION_FIELD),
        "spec_id": 0,
        "specs": {"0": spec},
        "sort_order_id": i32::from(!sort_orders.is_empty()),
        "sort_orders": sort_orders,
        "properties": definition["properties"].as_object().cloned().unwrap_or_default(),
    })
}

/// What a `createTable` asks.
#[derive(Debug)]
pub struct Requested {
    pub table_id: Uuid,
    pub definition: Value,
    pub stage: bool,
}

/// A request as a definition, with a new id, and a location unless the client gave one.
pub fn requested(state: &State, namespace: &[&str], request: CreateTableRequest) -> Result<Requested, ApplyError> {
    let table_id = Uuid::new_v4();
    let location = match request.location {
        Some(location) => location,
        None => state.catalog().default_location(namespace, &request.name, table_id)?,
    };
    if state.storage()?.path(&location).is_err() {
        return Err(invalid(format!("{location} is out of the warehouse's store")).into());
    }
    let creation = Creation {
        table_id: table_id.to_string(),
        schema: request.schema,
        spec: request
            .partition_spec
            .unwrap_or_else(|| json!({"spec-id": 0, "fields": []})),
        sort_order: request.write_order,
        location,
    };
    let given = request.properties.into_iter().map(|(k, v)| (k, Some(v))).collect();
    let definition = definition(namespace, &request.name, &creation, &given)?;
    Ok(Requested {
        table_id,
        definition,
        stage: request.stage_create,
    })
}

/// Commits a `createTable` that is not staged.
pub async fn create(state: &State, namespace: &[&str], name: &str, new: Requested) -> Result<Table, ApplyError> {
    let main = state.main();
    let transaction = main
        .transaction()
        .define(new.table_id, new.definition)
        .author("iceberg");
    transaction.commit().await?;
    Ok(main.table(namespace, name).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::metadata::schema;

    fn nested() -> Value {
        json!({"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "k", "required": true, "type": "long"},
            {"id": 2, "name": "s", "required": false, "doc": "a struct", "type": {"type": "struct", "fields": [
                {"id": 3, "name": "x", "required": true, "type": "decimal(10,2)"},
                {"id": 4, "name": "y", "required": false, "type": "fixed[16]"}]}},
            {"id": 5, "name": "tags", "required": false, "type": {"type": "list", "element-id": 6,
                "element-required": true, "element": "string"}},
            {"id": 7, "name": "m", "required": false, "type": {"type": "map", "key-id": 8, "key": "string",
                "value-id": 9, "value-required": false, "value": {"type": "list", "element-id": 10,
                    "element-required": false, "element": "timestamptz"}}}]})
    }

    fn refused(answer: Result<impl std::fmt::Debug, Refused>, says: &str) {
        match answer {
            Err(Refused::Invalid(message)) => assert!(message.contains(says), "{message}"),
            other => panic!("{other:?}"),
        }
    }

    fn creation(schema: Value, spec: Value, sort_order: Option<Value>) -> Creation {
        Creation {
            table_id: "t".into(),
            schema,
            spec,
            sort_order,
            location: "memory:///t".into(),
        }
    }

    #[test]
    fn typ6_schema_round_trip() {
        let rows = field_rows(&nested()).unwrap();
        assert_eq!(schema(0, &Value::Array(rows)), nested());
    }

    #[test]
    fn map_key_required() {
        let rows = field_rows(&nested()).unwrap();
        let key = rows.iter().find(|r| r["name"] == "key").unwrap();
        assert_eq!((&key["required"], &key["parent_field_id"]), (&json!(true), &json!(7)));
    }

    #[test]
    fn identifier_fields_refused() {
        let mut identified = nested();
        identified["identifier-field-ids"] = json!([1]);
        refused(field_rows(&identified), "identifier");
        identified["identifier-field-ids"] = json!([]);
        assert!(field_rows(&identified).is_ok());
    }

    #[test]
    fn field_defaults_refused() {
        let with_default = json!({"fields": [{"id": 1, "name": "k", "required": false, "type": "int",
                                              "write-default": 3}]});
        refused(field_rows(&with_default), "format 2");
    }

    #[test]
    fn rest7_definition() {
        let spec = json!({"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "k_b",
                                                    "transform": "bucket[4]"}]});
        let order = json!({"order-id": 1, "fields": [{"source-id": 1, "transform": "identity", "direction": "asc",
                                                      "null-order": "nulls-first"}]});
        let given = BTreeMap::from([
            ("a".to_string(), Some("1".to_string())),
            ("format-version".to_string(), Some("2".to_string())),
        ]);
        let definition = definition(&["db"], "t", &creation(nested(), spec, Some(order)), &given).unwrap();
        assert_eq!(definition["create"]["namespace"], json!(["db"]));
        assert_eq!(definition["partition_spec"][0]["source_ids"], json!([1]));
        assert_eq!(definition["sort_order"][0]["null_order"], "nulls-first");
        assert_eq!(definition["properties"], json!({"a": "1"}));
        let described = described(&definition);
        assert_eq!(described["last_field_id"], 10);
        assert_eq!(described["last_partition_field_id"], 1000);
        assert_eq!(described["sort_order_id"], 1);
    }

    #[test]
    fn unpartitioned_unsorted_definition() {
        let spec = json!({"spec-id": 0, "fields": []});
        let definition = definition(&[], "t", &creation(nested(), spec, None), &BTreeMap::new()).unwrap();
        assert!(definition.get("partition_spec").is_none() && definition.get("sort_order").is_none());
        let described = described(&definition);
        assert_eq!(described["last_partition_field_id"], NO_PARTITION_FIELD);
        assert_eq!(described["sort_order_id"], 0);
    }

    #[test]
    fn nonzero_spec_refused() {
        let spec = json!({"spec-id": 3, "fields": []});
        refused(
            definition(&[], "t", &creation(nested(), spec, None), &BTreeMap::new()),
            "spec 0",
        );
    }

    #[test]
    fn creation_properties_validated() {
        let spec = json!({"spec-id": 0, "fields": []});
        let new = creation(nested(), spec, None);
        let given = |key: &str, value: &str| BTreeMap::from([(key.to_string(), Some(value.to_string()))]);
        refused(definition(&[], "t", &new, &given("format-version", "3")), "format 2");
        refused(
            definition(&[], "t", &new, &given("write.merge.mode", "merge-on-read")),
            "write.merge.mode",
        );
        assert!(definition(&[], "t", &new, &given("write.merge.mode", "copy-on-write")).is_ok());
    }
}
