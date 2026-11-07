//! A snapshot's files as manifests, one per partition spec and kind, and its manifests as a manifest list (ICE-2, ICE-3,
//! ICE-4). Unknown to these: where a file comes from, and when it is written.

use std::collections::{BTreeMap, HashMap};

use apache_avro::schema::RecordField;
use apache_avro::types::Value;
use apache_avro::{Codec, DeflateSettings, Reader, Schema, Writer};
use bytes::Bytes;
use metalake::partition::{Scalar, Transform};
use metalake::{Error, ListedFile, Result};
use serde_json::{Value as Json, json};

use super::avro;
use super::bounds::to_iceberg;
use super::metadata::partition_fields;

const ADDED: i32 = 1;
const EXISTING: i32 = 0;
const DATA: i32 = 0;
const POSITION_DELETES: i32 = 1;

/// What a manifest's file says of the table it belongs to, and what serving a statistic or a partition takes.
#[derive(Debug)]
pub struct Context<'a> {
    pub location: &'a str,
    pub snapshot: i64,
    /// The type of each field at the snapshot.
    pub types: &'a HashMap<i32, String>,
    pub schema: &'a Json,
    pub schema_id: i64,
    pub spec_id: i32,
    /// The spec's fields, in the catalog's shape.
    pub spec: &'a [Json],
}

/// A vector, as the position delete file it is served as (ICE-4).
#[derive(Debug)]
pub struct Delete<'a> {
    pub file: &'a ListedFile,
    pub path: String,
    pub size: i64,
}

/// A manifest as a manifest list names it.
#[derive(Debug)]
pub struct Listed {
    pub path: String,
    pub length: i64,
    pub spec_id: i32,
    pub deletes: bool,
    pub added_files: i32,
    pub existing_files: i32,
    pub added_rows: i64,
    pub existing_rows: i64,
    pub min_sequence: i64,
    /// The snapshot that added it, which a reader compares with the one it reads (ICE-2).
    pub added_snapshot: i64,
}

/// Iceberg's default for manifests, which every reader has: DuckDB reads no other.
fn codec() -> Codec {
    Codec::Deflate(DeflateSettings::default())
}

fn avro_failed(error: apache_avro::Error) -> Error {
    Error::Corrupt(format!("a manifest could not be written: {error}"))
}

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

/// The type a partition field's values have in a manifest: the source's for `identity` and `truncate`, Iceberg's `date`
/// for `day`, as clients read it, and `int` for the others (PAR-2).
pub(crate) fn result_type<'a>(field: &Json, types: &'a HashMap<i32, String>) -> Result<&'a str> {
    let transform = field["transform"].as_str().unwrap_or_default();
    if transform == "identity" || transform.starts_with("truncate") {
        let source = field["source_ids"][0].as_i64().unwrap_or(0) as i32;
        return match types.get(&source) {
            Some(ty) => Ok(ty),
            None => Err(Error::Corrupt(format!("partition field over unknown field {source}"))),
        };
    }
    Ok(if transform == "day" { "date" } else { "int" })
}

fn partition_schema(ty: &str) -> Result<Schema> {
    Ok(match ty {
        "boolean" => Schema::Boolean,
        "int" => Schema::Int,
        "long" => Schema::Long,
        "float" => Schema::Float,
        "double" => Schema::Double,
        "string" => Schema::String,
        "date" => Schema::Date,
        other => {
            return Err(Error::Unsupported(format!(
                "a partition of type {other} is not served yet"
            )));
        }
    })
}

fn partition_value(ty: &str, value: &Json) -> Option<Value> {
    Some(match ty {
        "boolean" => Value::Boolean(value.as_bool()?),
        "int" => Value::Int(value.as_i64()? as i32),
        "long" => Value::Long(value.as_i64()?),
        "float" => Value::Float(value.as_f64()? as f32),
        "double" => Value::Double(value.as_f64()?),
        "string" => Value::String(value.as_str()?.to_string()),
        "date" => match value.as_str() {
            Some(text) => Value::Date(metalake::partition::days(text)? as i32),
            None => Value::Date(value.as_i64()? as i32),
        },
        _ => return None,
    })
}

/// Converts one partition value of a client's manifest to the catalog's JSON (PAR-2). `None` when the type is not served.
pub(crate) fn partition_json(ty: &str, field: &Json, value: &Value) -> Option<Json> {
    let value = match value {
        Value::Union(_, inner) => inner.as_ref(),
        other => other,
    };
    Some(match (ty, value) {
        (_, Value::Null) => Json::Null,
        ("boolean", Value::Boolean(v)) => json!(v),
        ("int", Value::Int(v)) => json!(v),
        ("long", Value::Long(v)) => json!(v),
        ("long", Value::Int(v)) => json!(v),
        ("float", Value::Float(v)) => json!(v),
        ("double", Value::Double(v)) => json!(v),
        ("string", Value::String(v)) => json!(v),
        ("date", Value::Date(days) | Value::Int(days)) if field["transform"] == "identity" => {
            let identity = Transform::Identity.apply("date", Scalar::Long(i64::from(*days)));
            identity.ok()?
        }
        ("date", Value::Date(days) | Value::Int(days)) => json!(days),
        _ => return None,
    })
}

fn partition(file: &ListedFile, context: &Context<'_>) -> Result<Value> {
    let mut values = Vec::new();
    for (at, field) in context.spec.iter().enumerate() {
        let name = field["name"].as_str().unwrap_or_default().to_string();
        let held = &file.partition[at];
        let value = match held {
            Json::Null => none(),
            held => {
                let ty = result_type(field, context.types)?;
                let bad = || Error::Corrupt(format!("partition value {held} of file {}", file.file_id));
                some(partition_value(ty, held).ok_or_else(bad)?)
            }
        };
        values.push((name, value));
    }
    Ok(Value::Record(values))
}

fn entry(added: bool, snapshot: i64, sequence: i64, data_file: Vec<(String, Value)>) -> Value {
    Value::Record(vec![
        ("status".into(), Value::Int(if added { ADDED } else { EXISTING })),
        ("snapshot_id".into(), some(Value::Long(snapshot))),
        ("sequence_number".into(), some(Value::Long(sequence))),
        ("file_sequence_number".into(), some(Value::Long(sequence))),
        ("data_file".into(), Value::Record(data_file)),
    ])
}

fn data_entry(file: &ListedFile, context: &Context<'_>) -> Result<Value> {
    let value_counts = match &file.value_count {
        Some(values) => counts(&file.stat_field_ids, values),
        None => none(),
    };
    let ids = &file.stat_field_ids;
    let data_file = vec![
        ("content".to_string(), Value::Int(DATA)),
        ("file_path".into(), Value::String(served_path(context.location, file))),
        ("file_format".into(), Value::String("PARQUET".into())),
        ("partition".into(), partition(file, context)?),
        ("record_count".into(), Value::Long(file.record_count)),
        ("file_size_in_bytes".into(), Value::Long(file.file_size)),
        ("value_counts".into(), value_counts),
        ("null_value_counts".into(), counts(ids, &file.null_count)),
        ("nan_value_counts".into(), counts(ids, &file.nan_count)),
        ("lower_bounds".into(), bounds(file, &file.lower, context.types)),
        ("upper_bounds".into(), bounds(file, &file.upper, context.types)),
        (
            "sort_order_id".into(),
            file.sort_order_id.map_or_else(none, |id| some(Value::Int(id))),
        ),
        ("referenced_data_file".into(), none()),
    ];
    Ok(entry(
        file.begin_seq == context.snapshot,
        file.begin_seq,
        file.begin_seq,
        data_file,
    ))
}

fn delete_entry(delete: &Delete<'_>, sequence: i64, cardinality: i64, context: &Context<'_>) -> Result<Value> {
    let referenced = served_path(context.location, delete.file);
    let data_file = vec![
        ("content".to_string(), Value::Int(POSITION_DELETES)),
        ("file_path".into(), Value::String(delete.path.clone())),
        ("file_format".into(), Value::String("PARQUET".into())),
        ("partition".into(), partition(delete.file, context)?),
        ("record_count".into(), Value::Long(cardinality)),
        ("file_size_in_bytes".into(), Value::Long(delete.size)),
        ("value_counts".into(), none()),
        ("null_value_counts".into(), none()),
        ("nan_value_counts".into(), none()),
        ("lower_bounds".into(), none()),
        ("upper_bounds".into(), none()),
        ("sort_order_id".into(), none()),
        ("referenced_data_file".into(), some(Value::String(referenced))),
    ];
    Ok(entry(sequence == context.snapshot, sequence, sequence, data_file))
}

fn writer<'a>(schema: &'a Schema, context: &Context<'_>, content: &str) -> Result<Writer<'a, Vec<u8>>> {
    let mut writer = Writer::with_codec(schema, Vec::new(), codec()).map_err(avro_failed)?;
    let spec = json!(partition_fields(context.spec)).to_string();
    for (key, value) in [
        ("schema", context.schema.to_string()),
        ("schema-id", context.schema_id.to_string()),
        ("partition-spec", spec),
        ("partition-spec-id", context.spec_id.to_string()),
        ("format-version", "2".to_string()),
        ("content", content.to_string()),
    ] {
        writer.add_user_metadata(key.into(), value).map_err(avro_failed)?;
    }
    Ok(writer)
}

fn entry_schema(context: &Context<'_>) -> Result<Schema> {
    let mut fields: Vec<RecordField> = Vec::new();
    for field in context.spec {
        let ty = partition_schema(result_type(field, context.types)?)?;
        let (name, id) = (
            field["name"].as_str().unwrap_or_default(),
            field["field_id"].as_i64().unwrap_or(0),
        );
        fields.push(avro::optional(name, id, ty));
    }
    Ok(avro::manifest_entry(fields))
}

/// The data manifest of `files`, as the snapshot of `context` lists them.
pub fn data_manifest(files: &[&ListedFile], context: &Context<'_>) -> Result<Vec<u8>> {
    let schema = entry_schema(context)?;
    let mut writer = writer(&schema, context, "data")?;
    for file in files {
        writer.append_value(data_entry(file, context)?).map_err(avro_failed)?;
    }
    writer.into_inner().map_err(avro_failed)
}

/// The delete manifest of `deletes`, as the snapshot of `context` lists them.
pub fn delete_manifest(deletes: &[Delete<'_>], context: &Context<'_>) -> Result<Vec<u8>> {
    let schema = entry_schema(context)?;
    let mut writer = writer(&schema, context, "deletes")?;
    for delete in deletes {
        let vector = delete.file.vector.as_ref();
        let (sequence, cardinality) = vector.map_or((0, 0), |v| (v.begin_seq, v.cardinality));
        let entry = delete_entry(delete, sequence, cardinality, context)?;
        writer.append_value(entry).map_err(avro_failed)?;
    }
    writer.into_inner().map_err(avro_failed)
}

/// How many manifests of one spec and level a list holds before they become one, so that it stays short for who plans a
/// read. It is below Iceberg's `commit.manifest.min-count-to-merge`, so that a client's commit rewrites none of them.
pub const ROLL_UP: usize = 50;

/// A row of a parent's manifest list, which a snapshot lists again as it is (ICE-2).
#[derive(Debug, Clone, PartialEq)]
pub struct Inherited {
    pub row: Value,
    pub path: String,
    pub length: i64,
    pub spec_id: i32,
    pub deletes: bool,
    pub files: i32,
    pub rows: i64,
    pub min_sequence: i64,
    pub added_snapshot: i64,
}

impl Inherited {
    fn of(row: Value) -> Result<Self> {
        let corrupt = |name: &str| Error::Corrupt(format!("a manifest list has no {name}"));
        let Value::Record(fields) = &row else {
            return Err(corrupt("record"));
        };
        let value = |name: &str| fields.iter().find(|(key, _)| key == name).map(|(_, value)| value);
        let number = |name: &str| match value(name) {
            Some(Value::Int(n)) => Ok(i64::from(*n)),
            Some(Value::Long(n)) => Ok(*n),
            _ => Err(corrupt(name)),
        };
        let Some(Value::String(path)) = value("manifest_path") else {
            return Err(corrupt("manifest_path"));
        };
        Ok(Self {
            path: path.clone(),
            length: number("manifest_length")?,
            spec_id: number("partition_spec_id")? as i32,
            deletes: number("content")? == i64::from(POSITION_DELETES),
            files: (number("added_files_count")? + number("existing_files_count")?) as i32,
            rows: number("added_rows_count")? + number("existing_rows_count")?,
            min_sequence: number("min_sequence_number")?,
            added_snapshot: number("added_snapshot_id")?,
            row: row.clone(),
        })
    }

    /// How many times its files were rolled up: none for what one snapshot added, and no level at all for what a snapshot
    /// wrote of all it saw, which has no bound worth keeping.
    fn level(&self) -> Option<u32> {
        let name = self.path.rsplit('/').next().unwrap_or_default();
        if name.starts_with("a-") {
            return Some(0);
        }
        name.strip_prefix('r')?.split('-').next()?.parse().ok()
    }

    /// Its level, if it rolls up: not one of deletes, and not one that reached the size a manifest stops at.
    fn rolls_up_at(&self, target: i64) -> Option<u32> {
        self.level().filter(|_| !self.deletes && self.length < target)
    }
}

/// The rows of a manifest list this server wrote, to be listed again as they are (ICE-2).
pub fn listed_manifests(bytes: &[u8]) -> Result<Vec<Inherited>> {
    let reader = Reader::new(bytes).map_err(avro_failed)?;
    reader.map(|row| Inherited::of(row.map_err(avro_failed)?)).collect()
}

/// The spec and level of manifests that are due to become one, if any.
pub fn roll_up_due(manifests: &[Inherited], target: i64) -> Option<(i32, u32)> {
    let mut counts = BTreeMap::new();
    for manifest in manifests {
        if let Some(level) = manifest.rolls_up_at(target) {
            *counts.entry((manifest.spec_id, level)).or_insert(0) += 1;
        }
    }
    let due = counts.into_iter().find(|(_, count)| *count >= ROLL_UP);
    due.map(|(at, _)| at)
}

/// Takes from `manifests` those of a spec and a level.
pub fn taken(manifests: &mut Vec<Inherited>, spec_id: i32, level: u32, target: i64) -> Vec<Inherited> {
    let (parts, rest) = std::mem::take(manifests)
        .into_iter()
        .partition(|m| m.spec_id == spec_id && m.rolls_up_at(target) == Some(level));
    *manifests = rest;
    parts
}

/// One data manifest of what the manifests `parts` hold, as files `snapshot` found there. Its files are not touched, and each
/// entry keeps the snapshot and the sequence that added it. A list's manifests share a schema, as one that changes it writes
/// them all.
pub fn rolled_manifest(parts: &[Bytes], context: &Context<'_>) -> Result<Vec<u8>> {
    let schema = entry_schema(context)?;
    let mut writer = writer(&schema, context, "data")?;
    for part in parts {
        for entry in Reader::new(&part[..]).map_err(avro_failed)? {
            let Value::Record(mut fields) = entry.map_err(avro_failed)? else {
                return Err(Error::Corrupt("a manifest entry is not a record".into()));
            };
            for (name, value) in &mut fields {
                if name == "status" {
                    *value = Value::Int(EXISTING);
                }
            }
            writer.append_value(Value::Record(fields)).map_err(avro_failed)?;
        }
    }
    writer.into_inner().map_err(avro_failed)
}

/// How a list shows the manifest of the files of `parts`, of `spec_id`, which `snapshot` rolled up.
pub fn rolled_up(parts: &[Inherited], path: String, length: usize, spec_id: i32, snapshot: i64) -> Result<Inherited> {
    let listed = Listed {
        path,
        length: length as i64,
        spec_id,
        deletes: false,
        added_files: 0,
        existing_files: parts.iter().map(|m| m.files).sum(),
        added_rows: 0,
        existing_rows: parts.iter().map(|m| m.rows).sum(),
        min_sequence: parts.iter().map(|m| m.min_sequence).min().unwrap_or(0),
        added_snapshot: parts.iter().map(|m| m.added_snapshot).max().unwrap_or(0),
    };
    Inherited::of(list_row(&listed, snapshot))
}

fn list_row(m: &Listed, snapshot: i64) -> Value {
    Value::Record(vec![
        ("manifest_path".into(), Value::String(m.path.clone())),
        ("manifest_length".into(), Value::Long(m.length)),
        ("partition_spec_id".into(), Value::Int(m.spec_id)),
        (
            "content".into(),
            Value::Int(if m.deletes { POSITION_DELETES } else { DATA }),
        ),
        ("sequence_number".into(), Value::Long(snapshot)),
        ("min_sequence_number".into(), Value::Long(m.min_sequence)),
        ("added_snapshot_id".into(), Value::Long(m.added_snapshot)),
        ("added_files_count".into(), Value::Int(m.added_files)),
        ("existing_files_count".into(), Value::Int(m.existing_files)),
        ("deleted_files_count".into(), Value::Int(0)),
        ("added_rows_count".into(), Value::Long(m.added_rows)),
        ("existing_rows_count".into(), Value::Long(m.existing_rows)),
        ("deleted_rows_count".into(), Value::Long(0)),
    ])
}

/// The manifest list of `snapshot`, whose parent, if any, is `parent`: its own manifests, and those it carries.
pub fn manifest_list(
    snapshot: i64,
    parent: Option<i64>,
    manifests: &[Listed],
    carried: Vec<Inherited>,
) -> Result<Vec<u8>> {
    let schema = avro::manifest_file();
    let mut writer = Writer::with_codec(&schema, Vec::new(), codec()).map_err(avro_failed)?;
    let parent = parent.map_or_else(|| "null".to_string(), |id| id.to_string());
    for (key, value) in [
        ("snapshot-id", snapshot.to_string()),
        ("parent-snapshot-id", parent),
        ("sequence-number", snapshot.to_string()),
        ("format-version", "2".to_string()),
    ] {
        writer.add_user_metadata(key.into(), value).map_err(avro_failed)?;
    }
    for m in manifests {
        writer.append_value(list_row(m, snapshot)).map_err(avro_failed)?;
    }
    for m in carried {
        writer.append_value(m.row).map_err(avro_failed)?;
    }
    writer.into_inner().map_err(avro_failed)
}
