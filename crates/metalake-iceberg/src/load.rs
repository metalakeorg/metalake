//! What a `loadTable` takes: gathering a table's rows, and generating its files the first time a client asks (REST-6). A
//! snapshot's files never change, so what is there is there for good, and a repeat writes the same bytes.

use std::collections::btree_map::Entry;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};

use apache_avro::types::Value as AvroValue;
use bytes::Bytes;
use futures_util::TryStreamExt;
use metalake::{Error, ListedFile, Ref, Result, Scan, Storage, Table, TableSnapshot, listed_vector};

use crate::project::deletes::position_deletes;
use crate::project::manifest::{
    Context, Delete, Listed, data_manifest, delete_manifest, listed_manifests, manifest_list, served_path,
};
use crate::project::metadata::{Rows, SERVED_PER_REF, adds_only, metadata, schema};
use crate::state::State;
use crate::wire::metadata::{LoadTableResult, TableMetadata};

/// Where what is generated for a table lies, away from data (REST-6).
fn generated(location: &str) -> String {
    format!("{}/metalake-manifests", location.trim_end_matches('/'))
}

fn list_path(location: &str, snapshot: i64) -> String {
    format!("{}/snap-{snapshot}.avro", generated(location))
}

fn delete_path(location: &str, file: &ListedFile, sequence: i64) -> String {
    format!(
        "{}/metalake-deletes/{}-{sequence}.parquet",
        location.trim_end_matches('/'),
        file.file_id
    )
}

/// The rows of `table`, on `main`, and only the snapshots served (REST-5).
async fn rows(state: &State, table: &Table) -> Result<Rows> {
    let main = state.main();
    let definition = main.definition(table, None).await?;
    let mut refs = Vec::new();
    let live = state.catalog().describe_refs().await?;
    for described in &live {
        let on = state.named(&described.name).await?;
        let seen = on.definition(table, None).await.ok();
        let current = seen.and_then(|d| d["snapshot_id"].as_i64());
        refs.push((described.name.clone(), described.kind.clone(), current));
    }
    let heads: Vec<i64> = refs.iter().filter_map(|(.., current)| *current).collect();
    let snapshots = state.catalog().ancestors(table, &heads, SERVED_PER_REF).await?;
    let mut schemas = BTreeMap::new();
    for snapshot in &snapshots {
        if let Entry::Vacant(schema) = schemas.entry(snapshot.schema_seq) {
            let on = state.named(&snapshot.ref_name).await?;
            let at = on.definition(table, Some(snapshot.snapshot_id)).await?;
            schema.insert(at["fields"].clone());
        }
    }
    Ok(Rows {
        table_id: table.id().to_string(),
        definition,
        schemas,
        snapshots,
        refs,
    })
}

/// A manifest as its list shows it, from each entry's being added, its rows and its sequence.
fn listed(path: String, length: usize, spec_id: i32, deletes: bool, entries: &[(bool, i64, i64)]) -> Listed {
    let (added, existing): (Vec<_>, Vec<_>) = entries.iter().partition(|(added, ..)| *added);
    Listed {
        path,
        length: length as i64,
        spec_id,
        deletes,
        added_files: added.len() as i32,
        existing_files: existing.len() as i32,
        added_rows: added.iter().map(|(_, rows, _)| rows).sum(),
        existing_rows: existing.iter().map(|(_, rows, _)| rows).sum(),
        min_sequence: entries.iter().map(|(.., sequence)| *sequence).min().unwrap_or(0),
        added_snapshot: entries.iter().map(|(.., sequence)| *sequence).max().unwrap_or(0),
    }
}

/// Writes what a snapshot's manifests name and nothing holds: inline files (ICE-5) and position delete files (ICE-4).
async fn materialize<'f>(files: &'f [ListedFile], location: &str, storage: &Storage) -> Result<Vec<Delete<'f>>> {
    let mut deletes = Vec::new();
    for file in files {
        if file.path.is_none() {
            let held = || Error::Corrupt(format!("file {} has no bytes", file.file_id));
            let bytes = file.content.clone().ok_or_else(held)?;
            storage.put(&served_path(location, file), bytes.into()).await?;
        }
        if let (Some(at), Some(vector)) = (&file.vector, listed_vector(file, storage).await?) {
            let path = delete_path(location, file, at.begin_seq);
            let bytes = position_deletes(&served_path(location, file), vector.positions())?;
            let size = bytes.len() as i64;
            storage.put(&path, bytes.into()).await?;
            deletes.push(Delete { file, path, size });
        }
    }
    Ok(deletes)
}

/// The manifests a snapshot takes from its parent, when it only added to it and the parent's list is there.
async fn inherited(storage: &Storage, table: &Table, snapshot: &TableSnapshot) -> Result<Option<Vec<AvroValue>>> {
    let Some(parent) = snapshot.parent_snapshot_id.filter(|_| adds_only(snapshot)) else {
        return Ok(None);
    };
    let path = list_path(table.location(), parent);
    if !storage.exists(&path).await? {
        return Ok(None);
    }
    Ok(Some(listed_manifests(&storage.get(&path).await?)?))
}

/// A snapshot's manifests and manifest list: what it added, and its parent's manifests when it only added to them, which
/// costs what it added, or else all it sees.
async fn generate(on: &Ref, state: &State, storage: &Storage, table: &Table, snapshot: &TableSnapshot) -> Result<()> {
    let (location, at) = (table.location(), snapshot.snapshot_id);
    let namespace: Vec<&str> = table.namespace().iter().map(String::as_str).collect();
    let read = on.at(at).table(&namespace, table.name()).await?;
    let carried = inherited(storage, table, snapshot).await?;
    let files: Vec<ListedFile> = match carried {
        Some(_) => on.added_files(table, at).await?,
        None => Scan::new(state.catalog(), &read).list().await?.try_collect().await?,
    };
    let deletes = materialize(&files, location, storage).await?;

    let definition = on.definition(table, Some(at)).await?;
    let fields = definition["fields"].as_array().cloned().unwrap_or_default();
    let types: HashMap<i32, String> = fields
        .iter()
        .filter_map(|f| Some((f["field_id"].as_i64()? as i32, f["type"].as_str()?.to_string())))
        .collect();
    let rendered = schema(snapshot.schema_seq, &definition["fields"]);
    let mut spec_ids: Vec<i32> = files.iter().map(|f| f.spec_id).collect();
    spec_ids.sort_unstable();
    spec_ids.dedup();

    let mut manifests = Vec::new();
    for spec_id in spec_ids {
        let empty = Vec::new();
        let spec = definition["specs"][spec_id.to_string()].as_array().unwrap_or(&empty);
        let context = Context {
            location,
            snapshot: at,
            types: &types,
            schema: &rendered,
            schema_id: snapshot.schema_seq,
            spec_id,
            spec,
        };
        // What the snapshot added apart from the rest, so that a reader finds it by its manifest (ICE-2).
        for (added, name) in [(true, "a"), (false, "m")] {
            let of_spec = files.iter().filter(|f| f.spec_id == spec_id);
            let here: Vec<&ListedFile> = of_spec.filter(|f| (f.begin_seq == at) == added).collect();
            if here.is_empty() {
                continue;
            }
            let bytes = data_manifest(&here, &context)?;
            let path = format!("{}/{name}-{at}-{spec_id}.avro", generated(location));
            let shown: Vec<_> = here.iter().map(|f| (added, f.record_count, f.begin_seq)).collect();
            manifests.push(listed(path.clone(), bytes.len(), spec_id, false, &shown));
            storage.put(&path, bytes.into()).await?;
        }

        let here: Vec<Delete<'_>> = deletes
            .iter()
            .filter(|d| d.file.spec_id == spec_id)
            .map(|d| Delete {
                file: d.file,
                path: d.path.clone(),
                size: d.size,
            })
            .collect();
        if !here.is_empty() {
            let bytes = delete_manifest(&here, &context)?;
            let path = format!("{}/d-{at}-{spec_id}.avro", generated(location));
            let vectors = here.iter().filter_map(|d| d.file.vector.as_ref());
            let shown: Vec<_> = vectors
                .map(|v| (v.begin_seq == at, v.cardinality, v.begin_seq))
                .collect();
            manifests.push(listed(path.clone(), bytes.len(), spec_id, true, &shown));
            storage.put(&path, bytes.into()).await?;
        }
    }
    let carried = carried.unwrap_or_default();
    let list = manifest_list(at, snapshot.parent_snapshot_id, &manifests, carried)?;
    storage.put(&list_path(location, at), list.into()).await
}

/// The metadata a load serves, as the file that holds it and where it lies. A ref can change it with no new sequence, so
/// its name carries its content.
fn rendered(location: &str, served: &TableMetadata) -> Result<(String, Bytes)> {
    let body = serde_json::to_vec(served).map_err(|e| Error::Corrupt(format!("metadata of {location}: {e}")))?;
    let mut hasher = DefaultHasher::new();
    body.hash(&mut hasher);
    let (sequence, content) = (served.last_sequence_number, hasher.finish());
    let path = format!("{}/v{sequence}-{content:016x}.metadata.json", generated(location));
    Ok((path, Bytes::from(body)))
}

/// Whether a snapshot's list is there, asking once.
async fn listed_once<Found>(id: i64, asked: &mut HashMap<i64, bool>, exists: impl Fn(i64) -> Found) -> Result<bool>
where
    Found: Future<Output = Result<bool>>,
{
    if let Some(there) = asked.get(&id) {
        return Ok(*there);
    }
    let there = exists(id).await?;
    asked.insert(id, there);
    Ok(there)
}

/// The snapshots served whose lists are not there, the earliest first. A list is written after its parent's and stays, so
/// the walk from each head ends at the first list it finds. The chain's oldest is asked too: a window that widened reaches
/// past every list there is, and then each one is asked.
async fn unlisted<'a, Found>(
    snapshots: &'a [TableSnapshot],
    heads: &[i64],
    exists: impl Fn(i64) -> Found,
) -> Result<Vec<&'a TableSnapshot>>
where
    Found: Future<Output = Result<bool>>,
{
    let by_id: HashMap<i64, &TableSnapshot> = snapshots.iter().map(|s| (s.snapshot_id, s)).collect();
    let mut asked = HashMap::new();
    let mut missing = BTreeMap::new();
    for head in heads {
        let mut chain = Vec::new();
        let mut at = by_id.get(head).copied();
        while let Some(snapshot) = at {
            chain.push(snapshot);
            at = snapshot.parent_snapshot_id.and_then(|id| by_id.get(&id).copied());
        }
        let Some(oldest) = chain.last() else {
            continue;
        };
        let every_one = !listed_once(oldest.snapshot_id, &mut asked, &exists).await?;
        for snapshot in chain {
            let there = listed_once(snapshot.snapshot_id, &mut asked, &exists).await?;
            if there && !every_one {
                break;
            }
            if !there {
                missing.insert(snapshot.snapshot_id, snapshot);
            }
        }
    }
    Ok(missing.into_values().collect())
}

/// `loadTable`. Its metadata file is written last, so finding it says that every file it names is there, and a load of
/// what has not changed asks nothing else (REST-6). An engine reads that file itself for its metadata tables, as Trino does.
pub(crate) async fn load(state: &State, table: &Table) -> Result<LoadTableResult> {
    let rows = rows(state, table).await?;
    let storage = state.storage()?;
    let location = table.location();
    let served = metadata(&rows, |at| list_path(location, at));
    let (metadata_location, body) = rendered(location, &served)?;
    if !storage.exists(&metadata_location).await? {
        let heads: Vec<i64> = rows.refs.iter().filter_map(|(.., current)| *current).collect();
        let exists = |id| {
            let (storage, path) = (&storage, list_path(location, id));
            async move { storage.exists(&path).await }
        };
        for snapshot in unlisted(&rows.snapshots, &heads, exists).await? {
            let on = state.named(&snapshot.ref_name).await?;
            generate(&on, state, &storage, table, snapshot).await?;
        }
        storage.put(&metadata_location, body).await?;
    }
    Ok(LoadTableResult {
        metadata_location: Some(metadata_location),
        metadata: served,
        config: BTreeMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;

    use serde_json::json;

    use super::*;

    fn snapshot(id: i64, parent: Option<i64>) -> TableSnapshot {
        TableSnapshot {
            snapshot_id: id,
            parent_snapshot_id: parent,
            ref_name: "main".into(),
            schema_seq: 1,
            summary: json!({}),
            timestamp_ms: None,
        }
    }

    fn chain(ids: std::ops::RangeInclusive<i64>) -> Vec<TableSnapshot> {
        let first = *ids.start();
        ids.map(|id| snapshot(id, (id > first).then_some(id - 1))).collect()
    }

    /// The snapshots `unlisted` finds, given the lists there are, and how many it asked for.
    async fn found(snapshots: &[TableSnapshot], heads: &[i64], lists: &[i64]) -> (Vec<i64>, usize) {
        let there: HashSet<i64> = lists.iter().copied().collect();
        let (requests, there) = (Cell::new(0), RefCell::new(there));
        let exists = |id: i64| {
            requests.set(requests.get() + 1);
            let found = there.borrow().contains(&id);
            async move { Ok(found) }
        };
        let missing = unlisted(snapshots, heads, exists).await.expect("no storage to fail");
        (missing.iter().map(|s| s.snapshot_id).collect(), requests.get())
    }

    #[tokio::test]
    async fn rest6_lists_there_cost_the_same_at_any_length() {
        let lists: Vec<i64> = (1..=1000).collect();
        let (short, long) = (chain(991..=1000), chain(1..=1000));
        let at_ten = found(&short, &[1000], &lists).await;
        let at_a_thousand = found(&long, &[1000], &lists).await;
        assert_eq!(at_ten, (vec![], 2));
        assert_eq!(at_a_thousand, at_ten);
    }

    #[tokio::test]
    async fn rest6_only_new_snapshots_generated() {
        let lists: Vec<i64> = (1..=8).collect();
        let (missing, requests) = found(&chain(1..=10), &[10], &lists).await;
        assert_eq!(missing, [9, 10]);
        assert_eq!(requests, 4, "the oldest, the two new ones and the first there");
    }

    #[tokio::test]
    async fn rest6_widened_window_finds_older_lists() {
        let lists: Vec<i64> = (91..=100).collect();
        let (missing, _) = found(&chain(1..=100), &[100], &lists).await;
        assert_eq!(missing, (1..=90).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn rest6_heads_share_their_chain() {
        let mut snapshots = chain(1..=5);
        snapshots.push(snapshot(6, Some(3)));
        let lists = [1, 2, 3, 4, 5];
        let (missing, requests) = found(&snapshots, &[5, 6, 7], &lists).await;
        assert_eq!(missing, [6]);
        assert_eq!(
            requests, 4,
            "a list is asked for once, and a head with no snapshot for none"
        );
    }
}
