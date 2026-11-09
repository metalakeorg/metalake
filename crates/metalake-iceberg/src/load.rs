//! What a `loadTable` takes: gathering a table's rows, and generating its files the first time a client asks (REST-6). A
//! snapshot's files never change, so what is there is there for good, and a repeat writes the same bytes.

use std::collections::btree_map::Entry;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use futures_util::future::try_join_all;
use futures_util::{StreamExt, TryStreamExt, stream};
use metalake::{Error, ListedFile, Ref, Result, Scan, Storage, Table, TableSnapshot, listed_vector};

use crate::project::deletes::position_deletes;
use crate::project::manifest::{
    Context, Delete, Inherited, Listed, data_manifest, delete_manifest, listed_manifests, manifest_list, roll_up_due,
    rolled_manifest, rolled_up, served_path, taken,
};
use crate::project::metadata::{
    Expiry, Rows, adds_only, expiry, manifest_target_size, metadata, previous_versions, schema,
};
use crate::state::State;
use crate::wire::metadata::{LoadTableResult, TableMetadata};

/// Objects written at once when a snapshot is generated.
const PUTS: usize = 16;

/// Where what is generated for a table lies, away from data (REST-6).
fn generated(location: &str) -> String {
    format!("{}/metalake-manifests", location.trim_end_matches('/'))
}

/// Where a table's metadata files lie, apart, as a list of them should not grow with its manifests.
fn metadata_files(location: &str) -> String {
    format!("{}/metalake-metadata", location.trim_end_matches('/'))
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

/// The ref of `name` among those a load read once, in the catalog's order: a load names each of them, and finding one
/// in the catalog reads them all.
fn known<'a>(refs: &'a [Ref], name: &str) -> Result<&'a Ref> {
    refs.iter()
        .find(|r| r.name() == name)
        .ok_or_else(|| Error::NotFound(format!("ref {name}")))
}

/// The rows of `table`, on `main`, and only the snapshots served (REST-5).
async fn rows(state: &State, table: &Table) -> Result<Rows> {
    let main = state.main();
    let definition = main.definition(table, None).await?;
    let mut refs = Vec::new();
    let live = state.catalog().refs().await?;
    for on in &live {
        let seen = on.definition(table, None).await.ok();
        let current = seen.and_then(|d| d["snapshot_id"].as_i64());
        refs.push((on.name().to_string(), on.kind().to_string(), current));
    }
    let heads: Vec<i64> = refs.iter().filter_map(|(.., current)| *current).collect();
    let Expiry { keep, max_age_ms } = expiry(&definition);
    let snapshots = state.catalog().ancestors(table, &heads, keep, Some(max_age_ms)).await?;
    let mut schemas = BTreeMap::new();
    for snapshot in &snapshots {
        if let Entry::Vacant(schema) = schemas.entry(snapshot.schema_seq) {
            let on = known(&live, &snapshot.ref_name)?;
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

/// Writes what a snapshot's manifests name and nothing holds: inline files (ICE-5) and position delete files (ICE-4),
/// each at a path of its file and vector alone. Those its parent saw are there already when `since`, the parent, has its
/// list, as a list is written after its objects: only what is new since is written, so an overwrite pays for what it
/// changed. A vector's delete file is encoded all the same, as the manifest states its size.
async fn materialize<'f>(
    files: &'f [ListedFile],
    location: &str,
    storage: &Storage,
    since: Option<i64>,
) -> Result<Vec<Delete<'f>>> {
    let new = |seq: i64| since.is_none_or(|parent| seq > parent);
    let mut deletes = Vec::new();
    let mut puts: Vec<(String, Bytes)> = Vec::new();
    for file in files {
        if file.path.is_none() && new(file.begin_seq) {
            let held = || Error::Corrupt(format!("file {} has no bytes", file.file_id));
            let bytes = file.content.clone().ok_or_else(held)?;
            puts.push((served_path(location, file), bytes.into()));
        }
        if let (Some(at), Some(vector)) = (&file.vector, listed_vector(file, storage).await?) {
            let path = delete_path(location, file, at.begin_seq);
            let bytes = position_deletes(&served_path(location, file), vector.positions())?;
            let size = bytes.len() as i64;
            if new(at.begin_seq) {
                puts.push((path.clone(), bytes.into()));
            }
            deletes.push(Delete { file, path, size });
        }
    }
    stream::iter(puts)
        .map(|(path, bytes)| async move { storage.put(&path, bytes).await })
        .buffer_unordered(PUTS)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(deletes)
}

/// What a snapshot is generated on: its parent, if the parent's list is there, and with it every object the parent
/// names; and the parent's manifests, when the snapshot only added to them.
struct Base {
    since: Option<i64>,
    carried: Option<Vec<Inherited>>,
}

async fn base(storage: &Storage, table: &Table, snapshot: &TableSnapshot) -> Result<Base> {
    let none = Base {
        since: None,
        carried: None,
    };
    let Some(parent) = snapshot.parent_snapshot_id else {
        return Ok(none);
    };
    let path = list_path(table.location(), parent);
    if !storage.exists(&path).await? {
        return Ok(none);
    }
    let carried = match adds_only(snapshot) {
        true => Some(listed_manifests(&storage.get(&path).await?)?),
        false => None,
    };
    Ok(Base {
        since: Some(parent),
        carried,
    })
}

/// Rolls up what `manifests` holds of one spec and level, level by level, so that a list stays short: a client that plans a
/// read opens all of it. It reads what it rolls up and nothing else of the table. What a snapshot added is not among them:
/// a reader finds it by its own manifest.
async fn roll_up<'c>(
    storage: &Storage,
    location: &str,
    snapshot: i64,
    manifests: &mut Vec<Inherited>,
    target: i64,
    context_of: impl Fn(i32) -> Context<'c>,
) -> Result<()> {
    while let Some((spec_id, level)) = roll_up_due(manifests, target) {
        let parts = taken(manifests, spec_id, level, target);
        let bytes = try_join_all(parts.iter().map(|m| storage.get(&m.path))).await?;
        let written = rolled_manifest(&bytes, &context_of(spec_id))?;
        let lo = parts.iter().map(|m| m.min_sequence).min().unwrap_or(0);
        let hi = parts.iter().map(|m| m.added_snapshot).max().unwrap_or(0);
        let path = format!("{}/r{}-{lo}-{hi}-{spec_id}.avro", generated(location), level + 1);
        manifests.push(rolled_up(&parts, path.clone(), written.len(), spec_id, snapshot)?);
        storage.put(&path, written.into()).await?;
    }
    Ok(())
}

/// A snapshot's manifests and manifest list: what it added, and its parent's manifests when it only added to them, which
/// costs what it added, or else all it sees. It reads no definition: its fields are those of its schema, which the load
/// read once a schema, and its specs and manifest size those of the definition served, which holds every spec the table
/// has had (REST-6).
async fn generate(
    on: &Ref,
    state: &State,
    storage: &Storage,
    table: &Table,
    rows: &Rows,
    snapshot: &TableSnapshot,
) -> Result<()> {
    let (location, at) = (table.location(), snapshot.snapshot_id);
    let base = base(storage, table, snapshot).await?;
    let files: Vec<ListedFile> = match base.carried {
        Some(_) => on.added_files(table, at).await?,
        None => {
            let namespace: Vec<&str> = table.namespace().iter().map(String::as_str).collect();
            let read = on.at(at).table(&namespace, table.name()).await?;
            Scan::new(state.catalog(), &read).list().await?.try_collect().await?
        }
    };
    let deletes = materialize(&files, location, storage, base.since).await?;

    let definition = &rows.definition;
    let unread = || Error::Corrupt(format!("schema {} of snapshot {at} not read", snapshot.schema_seq));
    let fields = rows.schemas.get(&snapshot.schema_seq).ok_or_else(unread)?;
    let types: HashMap<i32, String> = fields
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .filter_map(|f| Some((f["field_id"].as_i64()? as i32, f["type"].as_str()?.to_string())))
        .collect();
    let rendered = schema(snapshot.schema_seq, fields);
    let mut spec_ids: Vec<i32> = files.iter().map(|f| f.spec_id).collect();
    spec_ids.sort_unstable();
    spec_ids.dedup();

    let empty = Vec::new();
    let context_of = |spec_id: i32| Context {
        location,
        snapshot: at,
        types: &types,
        schema: &rendered,
        schema_id: snapshot.schema_seq,
        spec_id,
        spec: definition["specs"][spec_id.to_string()].as_array().unwrap_or(&empty),
    };
    let mut manifests = Vec::new();
    for spec_id in spec_ids {
        let context = context_of(spec_id);
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
    let mut carried = base.carried.unwrap_or_default();
    let target = manifest_target_size(definition);
    roll_up(storage, location, at, &mut carried, target, context_of).await?;
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
    let path = format!("{}/v{sequence}-{content:016x}.metadata.json", metadata_files(location));
    Ok((path, Bytes::from(body)))
}

/// What a table's metadata files `found`, each with its age, lose to keep `keep` of them, the latest by sequence and then
/// by age, as a ref can change a table with no new sequence.
fn stale_versions(found: Vec<(String, i64)>, keep: usize) -> Vec<String> {
    let sequence = |path: &str| -> i64 {
        let name = path.rsplit('/').next().unwrap_or_default();
        let digits = name.strip_prefix('v').and_then(|rest| rest.split('-').next());
        digits.and_then(|digits| digits.parse().ok()).unwrap_or(0)
    };
    let mut versions = found;
    versions.sort_by_key(|(path, age)| (std::cmp::Reverse(sequence(path)), *age));
    versions.into_iter().skip(keep).map(|(path, _)| path).collect()
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
        let live = state.catalog().refs().await?;
        let exists = |id| {
            let (storage, path) = (&storage, list_path(location, id));
            async move { storage.exists(&path).await }
        };
        for snapshot in unlisted(&rows.snapshots, &heads, exists).await? {
            let on = known(&live, &snapshot.ref_name)?;
            generate(on, state, &storage, table, &rows, snapshot).await?;
        }
        storage.put(&metadata_location, body).await?;
        let found = storage.list(&metadata_files(location)).await?;
        for stale in stale_versions(found, previous_versions(&rows.definition) + 1) {
            storage.delete(&stale).await?;
        }
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

    use apache_avro::types::Value as AvroValue;
    use serde_json::json;

    use super::*;
    use crate::project::manifest::ROLL_UP;

    const TARGET: i64 = 1024;

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

    fn manifest(name: &str, spec_id: i32, deletes: bool) -> Inherited {
        Inherited {
            row: AvroValue::Null,
            path: format!("gs://t/metalake-manifests/{name}"),
            length: 1,
            spec_id,
            deletes,
            files: 1,
            rows: 1,
            min_sequence: 1,
            added_snapshot: 1,
        }
    }

    fn many(prefix: &str, count: usize, spec_id: i32) -> Vec<Inherited> {
        (0..count)
            .map(|i| manifest(&format!("{prefix}{i}-{spec_id}.avro"), spec_id, false))
            .collect()
    }

    #[test]
    fn ice2_roll_up_due_by_spec_and_level() {
        assert_eq!(roll_up_due(&many("a-", ROLL_UP - 1, 0), TARGET), None);
        assert_eq!(roll_up_due(&many("a-", ROLL_UP, 0), TARGET), Some((0, 0)));
        assert_eq!(roll_up_due(&many("a-", ROLL_UP, 3), TARGET), Some((3, 0)));
        assert_eq!(roll_up_due(&many("r2-", ROLL_UP, 0), TARGET), Some((0, 2)));
        let mut split = many("a-", ROLL_UP - 1, 0);
        split.extend(many("a-", 1, 1));
        split.extend(many("r1-", ROLL_UP - 1, 0));
        assert_eq!(roll_up_due(&split, TARGET), None, "a spec and a level at a time");
    }

    #[test]
    fn ice2_roll_up_leaves_what_has_no_level() {
        let whole = many("m-", ROLL_UP, 0);
        assert_eq!(roll_up_due(&whole, TARGET), None, "all a snapshot saw");
        let deletes: Vec<Inherited> = (0..ROLL_UP)
            .map(|i| manifest(&format!("a-{i}-0.avro"), 0, true))
            .collect();
        assert_eq!(roll_up_due(&deletes, TARGET), None, "deletes stay apart");
        assert_eq!(
            roll_up_due(&many("r1-", ROLL_UP, 0), 1),
            None,
            "they reached the target"
        );
    }

    #[test]
    fn ice2_taken_leaves_the_rest() {
        let mut manifests = many("a-", 2, 0);
        manifests.extend(many("r1-", 1, 0));
        manifests.extend(many("a-", 1, 1));
        let parts = taken(&mut manifests, 0, 0, TARGET);
        assert_eq!((parts.len(), manifests.len()), (2, 2));
    }

    #[test]
    fn rest6_metadata_files_keep_the_latest() {
        let file = |name: &str, age| (format!("gs://t/metalake-metadata/{name}.metadata.json"), age);
        let found = vec![file("v9-a", 5), file("v10-b", 1), file("v10-c", 9), file("v2-d", 50)];
        let stale = stale_versions(found, 2);
        assert_eq!(stale.len(), 2);
        assert!(
            stale[0].contains("v9-a") && stale[1].contains("v2-d"),
            "by number, not by name: {stale:?}"
        );
        assert_eq!(stale_versions(vec![file("v1-a", 1)], 2), Vec::<String>::new());
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
