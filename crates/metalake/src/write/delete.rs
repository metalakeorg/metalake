//! Deletes: the vectors a commit sets on the files it deletes rows of (DV-1 to DV-4). Each file's new vector is the
//! union of what it deletes now and the vector it has: positions are of immutable files, so deletes commute, and a
//! commit that finds the vector changed under it (DV-4) takes the union again and retries. A vector within the
//! table's inline limit travels in the commit; the larger ones of a table go in one Puffin file, its intent
//! registered before it is uploaded (CMT-10).

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use futures_util::TryStreamExt;
use metalake_pg::records::{PlannedFile, VectorSet};
use serde_json::json;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::scan::{self, TaskVector};
use crate::storage::Storage;
use crate::table::Table;
use crate::vectors::{Deletes, DeletionVector};

/// The size of an inline vector at most, unless the table's property `inline.max-bytes` says otherwise.
const INLINE_MAX_BYTES: usize = 65_536;
/// How long the intent of a Puffin file lives before an uncommitted one is an orphan (RET-7).
const INTENT_TTL: Duration = Duration::from_secs(3600);
/// The field id Iceberg gives a row's position, which a deletion vector's blob names (Puffin).
const ROW_POSITION_FIELD: i32 = 2_147_483_545;

/// The rows a transaction deletes from one table.
#[derive(Debug, Clone)]
pub(crate) struct TableDeletes {
    pub table: Table,
    pub storage: Storage,
    pub deletes: Deletes,
}

impl TableDeletes {
    /// The vectors that set this table's deletes over the vectors its files have at `at` (the table's read point,
    /// or the latest when retrying); files whose vector would not change are left out.
    pub async fn vectors(&self, catalog: &Catalog, at: Option<i64>) -> Result<Vec<VectorSet>> {
        let t = &self.table;
        let wanted = self.deletes.files();
        let planned: BTreeMap<i64, PlannedFile> = {
            let session = catalog.session().await?;
            catalog
                .protocol()
                .plan(&session, t.ref_id(), t.id(), at, None)
                .await?
                .try_filter(|f| std::future::ready(wanted.contains_key(&f.file_id)))
                .map_ok(|f| (f.file_id, f))
                .try_collect()
                .await?
        };
        let mut changed = Vec::new();
        for (file_id, deletes) in wanted {
            let file = planned
                .get(file_id)
                .ok_or_else(|| Error::NotFound(format!("file {file_id} of {}", t.name())))?;
            if deletes.max().is_some_and(|p| p >= file.record_count as u64) {
                return Err(Error::Invalid(format!(
                    "file {file_id} has {} rows, and position {} is not one",
                    file.record_count,
                    deletes.max().unwrap_or_default()
                )));
            }
            let current = file.vector.clone().map(TaskVector::from);
            let now = scan::vector(*file_id, current.as_ref(), &self.storage)
                .await?
                .unwrap_or_default();
            let merged = now.union(deletes);
            if merged.cardinality() == now.cardinality() {
                continue;
            }
            changed.push((*file_id, current.map(|v| v.begin_seq), merged));
        }
        stored(catalog, t, &self.storage, &planned, changed, true).await
    }
}

/// The sets of `vectors`, each a file, the vector it replaces and the new one: inline within the table's limit, the
/// larger in one Puffin file, written unless `upload` is false, for a merge tried without effect.
pub(crate) async fn stored(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    planned: &BTreeMap<i64, PlannedFile>,
    vectors: Vec<(i64, Option<i64>, DeletionVector)>,
    upload: bool,
) -> Result<Vec<VectorSet>> {
    let inline_max = t
        .properties()
        .get("inline.max-bytes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(INLINE_MAX_BYTES);
    let mut sets = Vec::new();
    let mut large: Vec<(VectorSet, Vec<u8>)> = Vec::new();
    for (file_id, base_seq, vector) in vectors {
        let blob = vector.to_blob();
        let set = VectorSet {
            table_id: t.id(),
            file_id,
            base_seq,
            path: None,
            content_offset: None,
            content_size: blob.len() as i64,
            cardinality: vector.cardinality() as i64,
            inline: None,
        };
        if blob.len() <= inline_max {
            sets.push(VectorSet {
                inline: Some(blob),
                ..set
            });
        } else {
            large.push((set, blob));
        }
    }
    if !large.is_empty() {
        sets.extend(puffin(catalog, t, storage, planned, large, upload).await?);
    }
    Ok(sets)
}

/// Writes the large vectors to one Puffin file and answers them at their offsets in it.
async fn puffin(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    planned: &BTreeMap<i64, PlannedFile>,
    large: Vec<(VectorSet, Vec<u8>)>,
    upload: bool,
) -> Result<Vec<VectorSet>> {
    let path = format!(
        "{}/data/{}-deletes.puffin",
        t.location().trim_end_matches('/'),
        Uuid::new_v4().simple()
    );
    let mut bytes = b"PFA1".to_vec();
    let mut blobs = Vec::new();
    let mut sets = Vec::new();
    for (set, blob) in large {
        let offset = bytes.len() as i64;
        bytes.extend_from_slice(&blob);
        let mut properties = json!({"cardinality": set.cardinality.to_string()});
        if let Some(data) = planned.get(&set.file_id).and_then(|f| f.path.as_ref()) {
            properties["referenced-data-file"] = json!(data);
        }
        blobs.push(json!({
            "type": "deletion-vector-v1", "fields": [ROW_POSITION_FIELD], "snapshot-id": -1,
            "sequence-number": -1, "offset": offset, "length": blob.len(), "properties": properties,
        }));
        sets.push(VectorSet {
            path: Some(path.clone()),
            content_offset: Some(offset),
            ..set
        });
    }
    if !upload {
        return Ok(sets);
    }
    let footer = serde_json::to_vec(&json!({ "blobs": blobs })).expect("JSON in memory");
    bytes.extend_from_slice(b"PFA1");
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(b"PFA1");

    let session = catalog.session().await?;
    catalog
        .protocol()
        .register_intents(&session, t.ref_id(), t.id(), std::slice::from_ref(&path), INTENT_TTL)
        .await?;
    storage.put(&path, Bytes::from(bytes)).await?;
    Ok(sets)
}

/// Whether a refused commit is one a delete's union fixes: its vector changed under it (DV-4).
pub(crate) fn rebases(e: &metalake_pg::Error) -> bool {
    e.rule() == Some("DV-4") && e.kind() == metalake_pg::ErrorKind::Conflict
}
