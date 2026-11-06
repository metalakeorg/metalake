//! A snapshot's files as a manifest, and its manifests as a manifest list (ICE-2, ICE-3).

use std::collections::HashMap;

use apache_avro::types::Value;
use apache_avro::{Codec, Schema, Writer, ZstandardSettings};
use metalake::ListedFile;
use serde_json::{Value as Json, json};

use super::avro;
use super::bounds::to_iceberg;

const ADDED: i32 = 1;
const EXISTING: i32 = 0;

/// Bytes a client can read, or why not.
pub type Written = Result<Vec<u8>, apache_avro::Error>;

fn some(value: Value) -> Value {
    Value::Union(1, Box::new(value))
}

fn none() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

fn pair(key: i32, value: Value) -> Value {
    Value::Record(vec![("key".into(), Value::Int(key)), ("value".into(), value)])
}

fn counts(ids: &[i32], counts: &[Option<i64>]) -> Value {
    let pairs = ids
        .iter()
        .zip(counts)
        .filter_map(|(id, n)| Some(pair(*id, Value::Long((*n)?))));
    some(Value::Array(pairs.collect()))
}

fn bounds(file: &ListedFile, of: &[Option<Vec<u8>>], types: &HashMap<i32, String>) -> Value {
    let pairs = file.stat_field_ids.iter().zip(of).filter_map(|(id, bound)| {
        let served = to_iceberg(types.get(id)?, bound.as_deref()?)?;
        Some(pair(*id, Value::Bytes(served)))
    });
    some(Value::Array(pairs.collect()))
}

/// The path a file is served at: its own, or where its inline bytes are materialized (ICE-5).
pub fn served_path(location: &str, file: &ListedFile) -> String {
    match &file.path {
        Some(path) => path.clone(),
        None => format!(
            "{}/metalake-inline/{}.parquet",
            location.trim_end_matches('/'),
            file.file_id
        ),
    }
}

fn entry(file: &ListedFile, location: &str, snapshot: i64, types: &HashMap<i32, String>) -> Value {
    let status = if file.begin_seq == snapshot { ADDED } else { EXISTING };
    let value_counts = match &file.value_count {
        Some(values) => counts(&file.stat_field_ids, values),
        None => none(),
    };
    let sort_order = file.sort_order_id.map_or_else(none, |id| some(Value::Int(id)));
    let data_file = vec![
        ("content".to_string(), Value::Int(0)),
        ("file_path".into(), Value::String(served_path(location, file))),
        ("file_format".into(), Value::String("PARQUET".into())),
        ("partition".into(), Value::Record(Vec::new())),
        ("record_count".into(), Value::Long(file.record_count)),
        ("file_size_in_bytes".into(), Value::Long(file.file_size)),
        ("value_counts".into(), value_counts),
        (
            "null_value_counts".into(),
            counts(&file.stat_field_ids, &file.null_count),
        ),
        ("nan_value_counts".into(), counts(&file.stat_field_ids, &file.nan_count)),
        ("lower_bounds".into(), bounds(file, &file.lower, types)),
        ("upper_bounds".into(), bounds(file, &file.upper, types)),
        ("sort_order_id".into(), sort_order),
        ("referenced_data_file".into(), none()),
    ];
    Value::Record(vec![
        ("status".into(), Value::Int(status)),
        ("snapshot_id".into(), some(Value::Long(snapshot))),
        ("sequence_number".into(), some(Value::Long(file.begin_seq))),
        ("file_sequence_number".into(), some(Value::Long(file.begin_seq))),
        ("data_file".into(), Value::Record(data_file)),
    ])
}

fn writer(schema: &Schema) -> Result<Writer<'_, Vec<u8>>, apache_avro::Error> {
    Writer::with_codec(schema, Vec::new(), Codec::Zstandard(ZstandardSettings::default()))
}

/// What a manifest's file says of the table it belongs to.
#[derive(Debug)]
pub struct Header<'a> {
    pub schema: &'a Json,
    pub schema_id: i64,
    pub spec_id: i32,
}

/// The manifest of `files`, as snapshot `snapshot` lists them. Only an unpartitioned table, for now.
pub fn manifest(
    files: &[ListedFile],
    location: &str,
    snapshot: i64,
    types: &HashMap<i32, String>,
    header: &Header<'_>,
) -> Written {
    let schema = avro::manifest_entry(Vec::new());
    let mut writer = writer(&schema)?;
    writer.add_user_metadata("schema".into(), header.schema.to_string())?;
    writer.add_user_metadata("schema-id".into(), header.schema_id.to_string())?;
    writer.add_user_metadata("partition-spec".into(), json!([]).to_string())?;
    writer.add_user_metadata("partition-spec-id".into(), header.spec_id.to_string())?;
    writer.add_user_metadata("format-version".into(), "2")?;
    writer.add_user_metadata("content".into(), "data")?;
    for file in files {
        writer.append_value(entry(file, location, snapshot, types))?;
    }
    writer.into_inner()
}

/// A manifest as a manifest list names it.
#[derive(Debug)]
pub struct Listed {
    pub path: String,
    pub length: i64,
    pub spec_id: i32,
    pub added_files: i32,
    pub existing_files: i32,
    pub added_rows: i64,
    pub existing_rows: i64,
    pub min_sequence: i64,
}

/// The manifest list of `snapshot`, whose parent, if any, `parent` is.
pub fn manifest_list(snapshot: i64, parent: Option<i64>, manifests: &[Listed]) -> Written {
    let schema = avro::manifest_file();
    let mut writer = writer(&schema)?;
    writer.add_user_metadata("snapshot-id".into(), snapshot.to_string())?;
    let parent = parent.map_or_else(|| "null".to_string(), |id| id.to_string());
    writer.add_user_metadata("parent-snapshot-id".into(), parent)?;
    writer.add_user_metadata("sequence-number".into(), snapshot.to_string())?;
    writer.add_user_metadata("format-version".into(), "2")?;
    for m in manifests {
        writer.append_value(Value::Record(vec![
            ("manifest_path".into(), Value::String(m.path.clone())),
            ("manifest_length".into(), Value::Long(m.length)),
            ("partition_spec_id".into(), Value::Int(m.spec_id)),
            ("content".into(), Value::Int(0)),
            ("sequence_number".into(), Value::Long(snapshot)),
            ("min_sequence_number".into(), Value::Long(m.min_sequence)),
            ("added_snapshot_id".into(), Value::Long(snapshot)),
            ("added_files_count".into(), Value::Int(m.added_files)),
            ("existing_files_count".into(), Value::Int(m.existing_files)),
            ("deleted_files_count".into(), Value::Int(0)),
            ("added_rows_count".into(), Value::Long(m.added_rows)),
            ("existing_rows_count".into(), Value::Long(m.existing_rows)),
            ("deleted_rows_count".into(), Value::Long(0)),
        ]))?;
    }
    writer.into_inner()
}
