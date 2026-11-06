//! The Avro schemas of Iceberg's manifests (the spec's "Manifests" and "Manifest Lists"), built in code: `apache-avro` does
//! not read a `logicalType` of an array from JSON, and Iceberg's maps need it. Each field states its id, as the spec does.

use std::collections::BTreeMap;

use apache_avro::Schema;
use apache_avro::schema::{ArraySchema, Name, RecordField, RecordSchema, UnionSchema};
use serde_json::json;

fn field(name: &str, id: i64, schema: Schema) -> RecordField {
    let custom_attributes = BTreeMap::from([("field-id".to_string(), json!(id))]);
    RecordField::builder()
        .name(name)
        .schema(schema)
        .custom_attributes(custom_attributes)
        .build()
}

fn optional(name: &str, id: i64, schema: Schema) -> RecordField {
    let union = UnionSchema::new(vec![Schema::Null, schema]).expect("null and one other type");
    let mut field = field(name, id, Schema::Union(union));
    field.default = Some(serde_json::Value::Null);
    field
}

fn record(name: &str, fields: Vec<RecordField>) -> Schema {
    let name = Name::new(name).expect("a plain name");
    Schema::Record(RecordSchema::builder().name(name).fields(fields).build())
}

/// Iceberg's map of a column to a value: an array of pairs, which Avro's own map cannot be, as its keys are strings.
fn map(name: &str, id: i64, key_id: i64, value_id: i64, value: Schema) -> RecordField {
    let pair = record(
        &format!("k{key_id}_v{value_id}"),
        vec![field("key", key_id, Schema::Int), field("value", value_id, value)],
    );
    let attributes = BTreeMap::from([("logicalType".to_string(), json!("map"))]);
    optional(
        name,
        id,
        Schema::Array(ArraySchema {
            items: Box::new(pair),
            attributes,
        }),
    )
}

/// A manifest's entry, whose partition is the record of the spec it is written for.
pub fn manifest_entry(partition: Vec<RecordField>) -> Schema {
    let data_file = record(
        "r2",
        vec![
            field("content", 134, Schema::Int),
            field("file_path", 100, Schema::String),
            field("file_format", 101, Schema::String),
            field("partition", 102, record("r102", partition)),
            field("record_count", 103, Schema::Long),
            field("file_size_in_bytes", 104, Schema::Long),
            map("value_counts", 109, 119, 120, Schema::Long),
            map("null_value_counts", 110, 121, 122, Schema::Long),
            map("nan_value_counts", 137, 138, 139, Schema::Long),
            map("lower_bounds", 125, 126, 127, Schema::Bytes),
            map("upper_bounds", 128, 129, 130, Schema::Bytes),
            optional("sort_order_id", 140, Schema::Int),
            optional("referenced_data_file", 143, Schema::String),
        ],
    );
    record(
        "manifest_entry",
        vec![
            field("status", 0, Schema::Int),
            optional("snapshot_id", 1, Schema::Long),
            optional("sequence_number", 3, Schema::Long),
            optional("file_sequence_number", 4, Schema::Long),
            field("data_file", 2, data_file),
        ],
    )
}

/// An entry of a manifest list.
pub fn manifest_file() -> Schema {
    record(
        "manifest_file",
        vec![
            field("manifest_path", 500, Schema::String),
            field("manifest_length", 501, Schema::Long),
            field("partition_spec_id", 502, Schema::Int),
            field("content", 517, Schema::Int),
            field("sequence_number", 515, Schema::Long),
            field("min_sequence_number", 516, Schema::Long),
            field("added_snapshot_id", 503, Schema::Long),
            field("added_files_count", 504, Schema::Int),
            field("existing_files_count", 505, Schema::Int),
            field("deleted_files_count", 506, Schema::Int),
            field("added_rows_count", 512, Schema::Long),
            field("existing_rows_count", 513, Schema::Long),
            field("deleted_rows_count", 514, Schema::Long),
        ],
    )
}
