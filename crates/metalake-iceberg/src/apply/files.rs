//! The files an Iceberg commit proposes (ICE-6): reads the client's manifest lists and manifests, and converts each new
//! data file to the protocol's `FileAdd`. Reads only; nothing here commits.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use apache_avro::Reader;
use apache_avro::types::Value;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use metalake::{FileAdd, Storage};
use serde_json::Value as Json;
use uuid::Uuid;

use super::{ApplyError, Refused};
use crate::project::bounds::from_iceberg;
use crate::project::manifest::{partition_json, result_type};
use crate::wire::commit::ProposedSnapshot;

const EXISTING: i32 = 0;
const ADDED: i32 = 1;
const DELETED: i32 = 2;
const DATA: i32 = 0;
/// Requests in flight per commit.
const CONCURRENCY: usize = 16;

/// One column's statistics, as a manifest carries them.
#[derive(Debug, Clone, Default, PartialEq)]
struct ColumnStatistics {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    null_count: Option<i64>,
    nan_count: Option<i64>,
    value_count: Option<i64>,
}

/// A data file as a client's manifest describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub spec_id: i32,
    partition: Value,
    pub record_count: i64,
    pub file_size: i64,
    sort_order_id: Option<i32>,
    statistics: BTreeMap<i32, ColumnStatistics>,
}

/// What a commit's snapshots add and remove, once those that cancel out are gone.
#[derive(Debug, Default, PartialEq)]
pub struct ProposedFiles {
    pub added: Vec<ManifestFile>,
    pub removed: Vec<String>,
}

fn invalid(message: impl Into<String>) -> Refused {
    Refused::Invalid(message.into())
}

fn unwrapped(value: &Value) -> &Value {
    match value {
        Value::Union(_, inner) => inner,
        other => other,
    }
}

fn field<'a>(record: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    let found = record.iter().find(|(key, _)| key == name)?;
    Some(unwrapped(&found.1))
}

fn long(value: &Value) -> Option<i64> {
    match value {
        Value::Long(v) => Some(*v),
        Value::Int(v) => Some(i64::from(*v)),
        _ => None,
    }
}

fn required_long(record: &[(String, Value)], name: &str) -> Result<i64, Refused> {
    field(record, name)
        .and_then(long)
        .ok_or_else(|| invalid(format!("a manifest has no {name}")))
}

fn required_string<'a>(record: &'a [(String, Value)], name: &str) -> Result<&'a str, Refused> {
    match field(record, name) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(invalid(format!("a manifest has no {name}"))),
    }
}

/// The records of an Avro file.
fn records(bytes: &[u8]) -> Result<Vec<Vec<(String, Value)>>, Refused> {
    let unreadable = |e: apache_avro::Error| invalid(format!("an unreadable manifest: {e}"));
    let reader = Reader::new(bytes).map_err(unreadable)?;
    let mut found = Vec::new();
    for value in reader {
        match value.map_err(unreadable)? {
            Value::Record(record) => found.push(record),
            _ => return Err(invalid("a manifest entry is not a record")),
        }
    }
    Ok(found)
}

/// The manifests that `snapshot_id` wrote, with their spec: the others are the table's, as served.
fn manifests_of(bytes: &[u8], snapshot_id: i64) -> Result<Vec<(String, i32)>, Refused> {
    let mut manifests = Vec::new();
    for record in records(bytes)? {
        if required_long(&record, "added_snapshot_id")? != snapshot_id {
            continue;
        }
        let spec_id = required_long(&record, "partition_spec_id")? as i32;
        manifests.push((required_string(&record, "manifest_path")?.to_string(), spec_id));
    }
    Ok(manifests)
}

/// Applies `apply` to the value of each key in a map of a data file, which Avro holds as `key` and `value` records.
fn each_pair(data_file: &[(String, Value)], name: &str, mut apply: impl FnMut(i32, &Value)) {
    let Some(Value::Array(pairs)) = field(data_file, name) else {
        return;
    };
    for pair in pairs {
        if let Value::Record(pair) = pair
            && let (Some(key), Some(value)) = (field(pair, "key").and_then(long), field(pair, "value"))
        {
            apply(key as i32, value);
        }
    }
}

fn statistics(data_file: &[(String, Value)]) -> BTreeMap<i32, ColumnStatistics> {
    let mut columns: BTreeMap<i32, ColumnStatistics> = BTreeMap::new();
    let bytes = |value: &Value| match value {
        Value::Bytes(bytes) => Some(bytes.clone()),
        _ => None,
    };
    each_pair(data_file, "lower_bounds", |key, value| {
        columns.entry(key).or_default().lower = bytes(value);
    });
    each_pair(data_file, "upper_bounds", |key, value| {
        columns.entry(key).or_default().upper = bytes(value);
    });
    each_pair(data_file, "null_value_counts", |key, value| {
        columns.entry(key).or_default().null_count = long(value);
    });
    each_pair(data_file, "nan_value_counts", |key, value| {
        columns.entry(key).or_default().nan_count = long(value);
    });
    each_pair(data_file, "value_counts", |key, value| {
        columns.entry(key).or_default().value_count = long(value);
    });
    columns
}

/// The entries of a manifest, as a status and a file.
fn entries(bytes: &[u8], spec_id: i32) -> Result<Vec<(i32, ManifestFile)>, Refused> {
    let mut found = Vec::new();
    for record in records(bytes)? {
        let status = required_long(&record, "status")? as i32;
        let Some(Value::Record(data_file)) = field(&record, "data_file") else {
            return Err(invalid("a manifest entry has no data_file"));
        };
        if required_long(data_file, "content")? != i64::from(DATA) {
            if status == ADDED {
                return Err(invalid("delete files are not served: tables are copy-on-write"));
            }
            // A vector goes with its data file, so a client's removal of its delete file says nothing more.
            continue;
        }
        let path = required_string(data_file, "file_path")?;
        if status != EXISTING && !required_string(data_file, "file_format")?.eq_ignore_ascii_case("parquet") {
            return Err(invalid(format!("{path} is not Parquet")));
        }
        let file = ManifestFile {
            path: path.to_string(),
            spec_id,
            partition: field(data_file, "partition").cloned().unwrap_or(Value::Null),
            record_count: required_long(data_file, "record_count")?,
            file_size: required_long(data_file, "file_size_in_bytes")?,
            sort_order_id: field(data_file, "sort_order_id").and_then(long).map(|id| id as i32),
            statistics: statistics(data_file),
        };
        found.push((status, file));
    }
    Ok(found)
}

/// Folds the entries of a commit's snapshots, in order: a file added and then deleted within the commit never existed.
fn folded(entries: impl IntoIterator<Item = (i32, ManifestFile)>) -> ProposedFiles {
    let mut files = ProposedFiles::default();
    for (status, file) in entries {
        match status {
            ADDED => files.added.push(file),
            DELETED => match files.added.iter().position(|added| added.path == file.path) {
                Some(at) => {
                    files.added.remove(at);
                }
                None => files.removed.push(file.path),
            },
            _ => {}
        }
    }
    files
}

/// An object a client said it wrote. One that is not there is the request's fault, and will not be there on a retry.
async fn client_object(storage: &Storage, url: &str) -> Result<Bytes, ApplyError> {
    storage.get(url).await.map_err(|e| {
        if e.is_missing_object() {
            invalid(format!("{url} is not there")).into()
        } else {
            e.into()
        }
    })
}

/// What the snapshots of a commit add and remove.
pub async fn proposed(storage: &Storage, snapshots: &[ProposedSnapshot]) -> Result<ProposedFiles, ApplyError> {
    let mut all = Vec::new();
    for snapshot in snapshots {
        let list = client_object(storage, &snapshot.manifest_list).await?;
        let manifests = manifests_of(&list, snapshot.snapshot_id)?;
        let read = manifests.into_iter().map(|(path, spec_id)| {
            let storage = storage.clone();
            async move {
                let bytes = client_object(&storage, &path).await?;
                Ok::<_, ApplyError>(entries(&bytes, spec_id)?)
            }
        });
        let read: Vec<_> = stream::iter(read).buffered(CONCURRENCY).try_collect().await?;
        all.extend(read.into_iter().flatten());
    }
    Ok(folded(all))
}

/// The partition of a file as the catalog holds it, one value per field of its spec.
fn partition(file: &ManifestFile, spec: &[Json], types: &HashMap<i32, String>) -> Result<Json, Refused> {
    let Value::Record(values) = unwrapped(&file.partition) else {
        return Err(invalid(format!("{} has no partition record", file.path)));
    };
    let mut held = Vec::with_capacity(spec.len());
    for spec_field in spec {
        let name = spec_field["name"].as_str().unwrap_or_default();
        let value = values.iter().find(|(key, _)| key == name);
        let value = value.ok_or_else(|| invalid(format!("{} has no partition {name}", file.path)))?;
        let unknown = |_| invalid(format!("the partition {name} is over an unknown field"));
        let ty = result_type(spec_field, types).map_err(unknown)?;
        let converted = partition_json(ty, spec_field, &value.1);
        held.push(converted.ok_or_else(|| invalid(format!("a partition of type {ty} is not served yet")))?);
    }
    Ok(Json::Array(held))
}

/// A proposed file as the protocol adds it. Statistics of a field the table does not have are dropped. A manifest does
/// not carry the footer's size, so it is not known (STA-1): the commit reads no file, and the footer is kept after it.
pub fn file_add(
    table_id: Uuid,
    file: &ManifestFile,
    spec: &[Json],
    types: &HashMap<i32, String>,
) -> Result<FileAdd, Refused> {
    let known: BTreeSet<i32> = file.statistics.keys().copied().collect();
    let columns: Vec<(i32, &String)> = known
        .into_iter()
        .filter_map(|id| types.get(&id).map(|ty| (id, ty)))
        .collect();
    let bound = |pick: fn(&ColumnStatistics) -> &Option<Vec<u8>>| -> Vec<Option<Vec<u8>>> {
        let convert = |(id, ty): &(i32, &String)| pick(&file.statistics[id]).as_ref().and_then(|b| from_iceberg(ty, b));
        columns.iter().map(convert).collect()
    };
    let count = |pick: fn(&ColumnStatistics) -> Option<i64>| -> Vec<Option<i64>> {
        columns.iter().map(|(id, _)| pick(&file.statistics[id])).collect()
    };
    Ok(FileAdd {
        table_id,
        file_id: None,
        path: Some(file.path.clone()),
        spec_id: file.spec_id,
        partition: partition(file, spec, types)?,
        sort_order_id: file.sort_order_id,
        record_count: file.record_count,
        file_size: file.file_size,
        footer_size: 0,
        first_row_id: None,
        stat_field_ids: columns.iter().map(|(id, _)| *id).collect(),
        lower: bound(|c| &c.lower),
        upper: bound(|c| &c.upper),
        null_count: count(|c| c.null_count),
        nan_count: count(|c| c.nan_count),
        value_count: Some(count(|c| c.value_count)),
        content: None,
        footer: None,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::project::avro;

    fn record(fields: Vec<(&str, Value)>) -> Value {
        Value::Record(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    fn some(value: Value) -> Value {
        Value::Union(1, Box::new(value))
    }

    fn none() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn pairs(values: Vec<(i32, Value)>) -> Value {
        let pair = |(key, value)| record(vec![("key", Value::Int(key)), ("value", value)]);
        some(Value::Array(values.into_iter().map(pair).collect()))
    }

    fn data_file(path: &str, content: i32, format: &str) -> Value {
        record(vec![
            ("content", Value::Int(content)),
            ("file_path", Value::String(path.into())),
            ("file_format", Value::String(format.into())),
            ("partition", record(vec![])),
            ("record_count", Value::Long(3)),
            ("file_size_in_bytes", Value::Long(100)),
            ("value_counts", pairs(vec![(1, Value::Long(3))])),
            (
                "null_value_counts",
                pairs(vec![(1, Value::Long(0)), (9, Value::Long(1))]),
            ),
            ("nan_value_counts", none()),
            (
                "lower_bounds",
                pairs(vec![(1, Value::Bytes(7_i64.to_le_bytes().to_vec()))]),
            ),
            (
                "upper_bounds",
                pairs(vec![(1, Value::Bytes(9_i64.to_le_bytes().to_vec()))]),
            ),
            ("sort_order_id", none()),
            ("referenced_data_file", none()),
        ])
    }

    fn entry(status: i32, file: Value) -> Value {
        record(vec![
            ("status", Value::Int(status)),
            ("snapshot_id", some(Value::Long(77))),
            ("sequence_number", none()),
            ("file_sequence_number", none()),
            ("data_file", file),
        ])
    }

    fn avro_file(schema: &apache_avro::Schema, values: Vec<Value>) -> Vec<u8> {
        let mut writer = apache_avro::Writer::new(schema, Vec::new()).unwrap();
        for value in values {
            writer.append_value(value).unwrap();
        }
        writer.into_inner().unwrap()
    }

    fn manifest(values: Vec<Value>) -> Vec<u8> {
        avro_file(&avro::manifest_entry(Vec::new()), values)
    }

    fn listed(path: &str, snapshot: i64, content: i32) -> Value {
        record(vec![
            ("manifest_path", Value::String(path.into())),
            ("manifest_length", Value::Long(1)),
            ("partition_spec_id", Value::Int(0)),
            ("content", Value::Int(content)),
            ("sequence_number", Value::Long(1)),
            ("min_sequence_number", Value::Long(1)),
            ("added_snapshot_id", Value::Long(snapshot)),
            ("added_files_count", Value::Int(1)),
            ("existing_files_count", Value::Int(0)),
            ("deleted_files_count", Value::Int(0)),
            ("added_rows_count", Value::Long(1)),
            ("existing_rows_count", Value::Long(0)),
            ("deleted_rows_count", Value::Long(0)),
        ])
    }

    fn list(values: Vec<Value>) -> Vec<u8> {
        avro_file(&avro::manifest_file(), values)
    }

    fn snapshot(id: i64, manifest_list: &str) -> ProposedSnapshot {
        ProposedSnapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            manifest_list: manifest_list.to_string(),
            summary: BTreeMap::new(),
        }
    }

    fn refused<T: std::fmt::Debug>(answer: Result<T, Refused>, says: &str) {
        match answer {
            Err(Refused::Invalid(message)) => assert!(message.contains(says), "{message}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn ice6_manifests_of_snapshot_only() {
        let bytes = list(vec![listed("old.avro", 5, 0), listed("new.avro", 77, 0)]);
        assert_eq!(manifests_of(&bytes, 77).unwrap(), [("new.avro".to_string(), 0)]);
    }

    #[test]
    fn rest4_added_delete_file_refused() {
        refused(
            entries(&manifest(vec![entry(ADDED, data_file("d", 1, "PARQUET"))]), 0),
            "copy-on-write",
        );
    }

    #[test]
    fn rest4_removed_delete_file_ignored() {
        let bytes = manifest(vec![entry(DELETED, data_file("d", 1, "PARQUET"))]);
        assert_eq!(entries(&bytes, 0).unwrap(), Vec::new());
    }
    #[test]
    fn ice6_data_file_format_validated() {
        refused(
            entries(&manifest(vec![entry(ADDED, data_file("a.orc", 0, "ORC"))]), 0),
            "not Parquet",
        );
    }

    #[test]
    fn ice6_manifest_entries() {
        let bytes = manifest(vec![
            entry(ADDED, data_file("a", 0, "PARQUET")),
            entry(EXISTING, data_file("b", 0, "PARQUET")),
            entry(DELETED, data_file("c", 0, "parquet")),
        ]);
        let found = entries(&bytes, 4).unwrap();
        let statuses: Vec<(i32, &str)> = found.iter().map(|(s, f)| (*s, f.path.as_str())).collect();
        assert_eq!(statuses, [(ADDED, "a"), (EXISTING, "b"), (DELETED, "c")]);
        let (_, added) = &found[0];
        assert_eq!((added.spec_id, added.record_count, added.file_size), (4, 3, 100));
        assert_eq!(added.statistics[&1].value_count, Some(3));
        assert_eq!(added.statistics[&1].lower, Some(7_i64.to_le_bytes().to_vec()));
        assert_eq!(added.statistics[&9].null_count, Some(1));
    }

    #[test]
    fn unreadable_manifest_refused() {
        refused(entries(b"not avro", 0), "unreadable");
    }

    #[test]
    fn ice6_commit_folds_snapshots() {
        let file = |path: &str| ManifestFile {
            path: path.to_string(),
            spec_id: 0,
            partition: Value::Null,
            record_count: 1,
            file_size: 1,
            sort_order_id: None,
            statistics: BTreeMap::new(),
        };
        let files = folded([
            (ADDED, file("a")),
            (ADDED, file("b")),
            (DELETED, file("a")),
            (DELETED, file("old")),
            (EXISTING, file("kept")),
        ]);
        let added: Vec<&str> = files.added.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(added, ["b"]);
        assert_eq!(files.removed, ["old"]);
    }

    #[tokio::test]
    async fn ice6_proposed_reads_each_snapshot() {
        let storage = Storage::in_memory();
        let put = |name: &str, bytes: Vec<u8>| {
            let (storage, url) = (storage.clone(), format!("memory:///t/{name}"));
            async move { storage.put(&url, Bytes::from(bytes)).await.unwrap() }
        };
        put(
            "m1",
            manifest(vec![entry(ADDED, data_file("memory:///t/a", 0, "PARQUET"))]),
        )
        .await;
        put(
            "m2",
            manifest(vec![entry(DELETED, data_file("memory:///t/a", 0, "PARQUET"))]),
        )
        .await;
        put(
            "m3",
            manifest(vec![entry(ADDED, data_file("memory:///t/b", 0, "PARQUET"))]),
        )
        .await;
        put("l1", list(vec![listed("memory:///t/m1", 7, 0)])).await;
        put(
            "l2",
            list(vec![
                listed("memory:///t/m1", 7, 0),
                listed("memory:///t/m2", 8, 0),
                listed("memory:///t/m3", 8, 0),
            ]),
        )
        .await;
        let snapshots = [snapshot(7, "memory:///t/l1"), snapshot(8, "memory:///t/l2")];
        let files = proposed(&storage, &snapshots).await.unwrap();
        let gone = proposed(&storage, &[snapshot(9, "memory:///t/none")]).await;
        assert!(
            matches!(gone, Err(ApplyError::Refused(Refused::Invalid(_)))),
            "{gone:?}"
        );
        let added: Vec<&str> = files.added.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(added, ["memory:///t/b"]);
        assert_eq!(files.removed, Vec::<String>::new());
    }

    #[test]
    fn ice6_file_add_converts_statistics() {
        let table_id = Uuid::nil();
        let (_, file) = entries(
            &manifest(vec![entry(ADDED, data_file("memory:///t/a", 0, "PARQUET"))]),
            0,
        )
        .unwrap()
        .remove(0);
        let types = HashMap::from([(1, "long".to_string())]);
        let add = file_add(table_id, &file, &[], &types).unwrap();
        assert_eq!(add.stat_field_ids, [1], "field 9 is not the table's");
        assert_eq!(add.lower, [Some(metalake::bounds::long(7))]);
        assert_eq!(add.upper, [Some(metalake::bounds::long(9))]);
        assert_eq!(add.null_count, [Some(0)]);
        assert_eq!(add.value_count, Some(vec![Some(3)]));
        assert_eq!(add.nan_count, [None]);
        assert_eq!(
            (add.footer_size, add.partition.clone()),
            (0, json!([])),
            "a size not known"
        );
        assert_eq!(add.path.as_deref(), Some("memory:///t/a"));
    }

    #[test]
    fn par2_partition_values() {
        let file = |partition: Value| ManifestFile {
            path: "a".into(),
            spec_id: 1,
            partition,
            record_count: 1,
            file_size: 1,
            sort_order_id: None,
            statistics: BTreeMap::new(),
        };
        let types = HashMap::from([
            (1, "long".to_string()),
            (2, "date".to_string()),
            (3, "string".to_string()),
        ]);
        let spec = [
            json!({"name": "k_bucket", "transform": "bucket[4]", "source_ids": [1]}),
            json!({"name": "d_day", "transform": "day", "source_ids": [2]}),
            json!({"name": "d", "transform": "identity", "source_ids": [2]}),
            json!({"name": "s", "transform": "identity", "source_ids": [3]}),
        ];
        let values = record(vec![
            ("k_bucket", some(Value::Int(3))),
            ("d_day", some(Value::Date(19_782))),
            ("d", some(Value::Date(19_782))),
            ("s", none()),
        ]);
        assert_eq!(
            partition(&file(values), &spec, &types).unwrap(),
            json!([3, 19_782, "2024-02-29", null])
        );
        let missing = record(vec![("k_bucket", some(Value::Int(3)))]);
        refused(partition(&file(missing), &spec, &types), "no partition d_day");

        let uuid = HashMap::from([(1, "uuid".to_string())]);
        let spec = [json!({"name": "u", "transform": "identity", "source_ids": [1]})];
        let values = record(vec![("u", some(Value::Bytes(vec![0; 16])))]);
        refused(partition(&file(values), &spec, &uuid), "not served yet");
    }
}
