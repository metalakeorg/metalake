//! What a `loadTable` takes: gathering a table's rows, and generating its files the first time a client asks (REST-6). A
//! snapshot's files never change, so what is there is there for good, and a repeat writes the same bytes.

use std::collections::{BTreeMap, HashMap};

use futures_util::TryStreamExt;
use metalake::{Error, ListedFile, Ref, Result, Scan, Storage, Table};

use crate::project::manifest::{Header, Listed, manifest, manifest_list, served_path};
use crate::project::metadata::{Rows, metadata, schema};
use crate::state::State;
use crate::wire::metadata::LoadTableResult;

/// Where what is generated for a table lies, away from data (REST-6).
fn generated(location: &str) -> String {
    format!("{}/metalake-manifests", location.trim_end_matches('/'))
}

fn list_path(location: &str, snapshot: i64) -> String {
    format!("{}/snap-{snapshot}.avro", generated(location))
}

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Corrupt(format!("generating a manifest: {error}"))
}

/// The rows of `table`, on `main`.
async fn rows(state: &State, table: &Table) -> Result<Rows> {
    let main = state.main();
    let definition = main.definition(table, None).await?;
    let live = state.catalog().describe_refs().await?;
    let mut snapshots = state.catalog().table_snapshots(table).await?;
    snapshots.retain(|s| live.iter().any(|r| r.name == s.ref_name));
    let mut schemas = BTreeMap::new();
    for snapshot in &snapshots {
        if !schemas.contains_key(&snapshot.schema_seq) {
            let on = state.named(&snapshot.ref_name).await?;
            let at = on.definition(table, Some(snapshot.snapshot_id)).await?;
            schemas.insert(snapshot.schema_seq, at["fields"].clone());
        }
    }
    let mut refs = Vec::new();
    for described in live {
        let on = state.named(&described.name).await?;
        let seen = on.definition(table, None).await.ok();
        let current = seen.and_then(|d| d["snapshot_id"].as_i64());
        refs.push((described.name, described.kind, current));
    }
    Ok(Rows {
        table_id: table.id().to_string(),
        definition,
        schemas,
        snapshots,
        refs,
    })
}

/// A snapshot's manifest and manifest list, unless they are there.
async fn generate(on: &Ref, state: &State, storage: &Storage, table: &Table, rows: &Rows, at: i64) -> Result<()> {
    let location = table.location();
    if storage.exists(&list_path(location, at)).await? {
        return Ok(());
    }
    let snapshot = rows
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == at)
        .expect("a snapshot of the rows");
    let namespace: Vec<&str> = table.namespace().iter().map(String::as_str).collect();
    let read = on.at(at).table(&namespace, table.name()).await?;
    let files: Vec<ListedFile> = Scan::new(state.catalog(), &read).list().await?.try_collect().await?;
    let unsupported = files.iter().find(|f| f.vector.is_some() || f.spec_id != 0);
    if unsupported.is_some() || rows.definition["specs"]["0"].as_array().is_some_and(|f| !f.is_empty()) {
        return Err(Error::Unsupported("vectors and partitions are not served yet".into()));
    }
    for file in files.iter().filter(|f| f.path.is_none()) {
        let bytes = file
            .content
            .clone()
            .ok_or_else(|| Error::Corrupt(format!("file {} has no bytes", file.file_id)))?;
        storage.put(&served_path(location, file), bytes.into()).await?;
    }
    let at_fields = on.definition(table, Some(at)).await?;
    let fields = at_fields["fields"].as_array().cloned().unwrap_or_default();
    let types: HashMap<i32, String> = fields
        .iter()
        .filter_map(|f| Some((f["field_id"].as_i64()? as i32, f["type"].as_str()?.to_string())))
        .collect();
    let rendered = schema(snapshot.schema_seq, &at_fields["fields"]);
    let header = Header {
        schema: &rendered,
        schema_id: snapshot.schema_seq,
        spec_id: 0,
    };
    let bytes = manifest(&files, location, at, &types, &header).map_err(failed)?;
    let path = format!("{}/m-{at}.avro", generated(location));
    let length = bytes.len() as i64;
    storage.put(&path, bytes.into()).await?;
    let (added, existing): (Vec<_>, Vec<_>) = files.iter().partition(|f| f.begin_seq == at);
    let listed = Listed {
        path,
        length,
        spec_id: 0,
        added_files: added.len() as i32,
        existing_files: existing.len() as i32,
        added_rows: added.iter().map(|f| f.record_count).sum(),
        existing_rows: existing.iter().map(|f| f.record_count).sum(),
        min_sequence: files.iter().map(|f| f.begin_seq).min().unwrap_or(at),
    };
    let list = manifest_list(at, snapshot.parent_snapshot_id, &[listed]).map_err(failed)?;
    storage.put(&list_path(location, at), list.into()).await
}

/// `loadTable`.
pub(crate) async fn load(state: &State, table: &Table) -> Result<LoadTableResult> {
    let rows = rows(state, table).await?;
    let storage = state.storage()?;
    for snapshot in &rows.snapshots {
        let on = state.named(&snapshot.ref_name).await?;
        generate(&on, state, &storage, table, &rows, snapshot.snapshot_id).await?;
    }
    let location = table.location();
    let served = metadata(&rows, |at| list_path(location, at));
    Ok(LoadTableResult {
        metadata_location: format!("{}/v{}.metadata.json", generated(location), served.last_sequence_number),
        metadata: served,
        config: BTreeMap::new(),
    })
}
