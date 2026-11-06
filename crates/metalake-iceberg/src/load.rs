//! What a `loadTable` takes: gathering a table's rows, and generating its files the first time a client asks (REST-6). A
//! snapshot's files never change, so what is there is there for good, and a repeat writes the same bytes.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use futures_util::TryStreamExt;
use metalake::{Error, ListedFile, Ref, Result, Scan, Storage, Table, TableSnapshot, listed_vector};

use crate::project::deletes::position_deletes;
use crate::project::manifest::{Context, Delete, Listed, data_manifest, delete_manifest, manifest_list, served_path};
use crate::project::metadata::{Rows, metadata, schema, served};
use crate::state::State;
use crate::wire::metadata::LoadTableResult;

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
    let snapshots = state.catalog().table_snapshots(table).await?;
    let snapshots = served(snapshots, &refs);
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

/// A snapshot's manifests and manifest list, unless they are there.
async fn generate(on: &Ref, state: &State, storage: &Storage, table: &Table, snapshot: &TableSnapshot) -> Result<()> {
    let (location, at) = (table.location(), snapshot.snapshot_id);
    if storage.exists(&list_path(location, at)).await? {
        return Ok(());
    }
    let namespace: Vec<&str> = table.namespace().iter().map(String::as_str).collect();
    let read = on.at(at).table(&namespace, table.name()).await?;
    let files: Vec<ListedFile> = Scan::new(state.catalog(), &read).list().await?.try_collect().await?;
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
        let here: Vec<&ListedFile> = files.iter().filter(|f| f.spec_id == spec_id).collect();
        let bytes = data_manifest(&here, &context)?;
        let path = format!("{}/m-{at}-{spec_id}.avro", generated(location));
        let shown: Vec<_> = here
            .iter()
            .map(|f| (f.begin_seq == at, f.record_count, f.begin_seq))
            .collect();
        manifests.push(listed(path.clone(), bytes.len(), spec_id, false, &shown));
        storage.put(&path, bytes.into()).await?;

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
    let list = manifest_list(at, snapshot.parent_snapshot_id, &manifests)?;
    storage.put(&list_path(location, at), list.into()).await
}

/// `loadTable`.
pub(crate) async fn load(state: &State, table: &Table) -> Result<LoadTableResult> {
    let rows = rows(state, table).await?;
    let storage = state.storage()?;
    for snapshot in &rows.snapshots {
        let on = state.named(&snapshot.ref_name).await?;
        generate(&on, state, &storage, table, snapshot).await?;
    }
    let location = table.location();
    let served = metadata(&rows, |at| list_path(location, at));
    Ok(LoadTableResult {
        metadata_location: format!("{}/v{}.metadata.json", generated(location), served.last_sequence_number),
        metadata: served,
        config: BTreeMap::new(),
    })
}
