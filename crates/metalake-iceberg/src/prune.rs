//! Pruning what is generated (REST-6, RET-7b): the lists, manifests, delete files and inline files of a table's
//! projection that no snapshot it keeps reaches any more. A projection is derived, so only reading it says what is live:
//! the lists of the snapshots served, the manifests they name, and the files those name, as Iceberg's `ExpireSnapshots`
//! reaches them from the snapshots it retains. What is not reached is deleted once older than the catalog's grace
//! (RET-9), as Iceberg's `RemoveOrphanFiles` waits `older_than`, so that a load writing meanwhile, or a reader of what was
//! served, loses nothing. Metadata files are pruned by the load that writes them (`write.metadata.previous-versions-max`).

use std::collections::HashSet;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt, stream};
use metalake::{Result, Storage};

use crate::load::{generated, list_path};
use crate::project::manifest::{listed_manifests, manifest_files};

/// How often, at most, a process prunes one table's projection: a load of a table committed to every second would
/// otherwise list it as often.
pub(crate) const PRUNE_EVERY: Duration = Duration::from_secs(3600);
/// Objects read at once while finding what is live.
const GETS: usize = 16;
/// The directories of a table's projection that are pruned: what its lists and manifests name.
const PRUNED: [&str; 3] = ["metalake-manifests", "metalake-deletes", "metalake-inline"];

/// Where an object lies in a table's projection, as `metalake-manifests/snap-1.avro`: the same however its URL is
/// spelled, by a load or by a listing. None for an object outside it, as a data file a manifest names.
fn key(url: &str) -> Option<&str> {
    let at = url.rfind("/metalake-")? + 1;
    let key = &url[at..];
    PRUNED
        .iter()
        .any(|dir| key.starts_with(&format!("{dir}/")))
        .then_some(key)
}

/// The objects of the projection that the snapshots `kept` reach: their lists, the manifests those name, and the delete
/// and inline files those name. A snapshot kept whose list is not there yet reaches nothing: no load wrote it.
async fn reachable(storage: &Storage, location: &str, kept: &[i64]) -> Result<HashSet<String>> {
    let lists: Vec<String> = kept.iter().map(|id| list_path(location, *id)).collect();
    let lists = read_all(storage, lists).await?;
    let mut live: HashSet<String> = HashSet::new();
    let mut manifests: HashSet<String> = HashSet::new();
    for (path, bytes) in lists {
        live.extend(key(&path).map(str::to_string));
        for manifest in listed_manifests(&bytes)? {
            manifests.insert(manifest.path);
        }
    }
    for (path, bytes) in read_all(storage, manifests.into_iter().collect()).await? {
        live.extend(key(&path).map(str::to_string));
        for file in manifest_files(&bytes)? {
            live.extend(key(&file).map(str::to_string));
        }
    }
    Ok(live)
}

/// The objects at `paths` that are there, with their bytes, [`GETS`] at once.
async fn read_all(storage: &Storage, paths: Vec<String>) -> Result<Vec<(String, bytes::Bytes)>> {
    let read: Vec<Option<(String, bytes::Bytes)>> = stream::iter(paths)
        .map(|path| {
            let storage = storage.clone();
            async move {
                match storage.get(&path).await {
                    Ok(bytes) => Ok(Some((path, bytes))),
                    Err(e) if e.is_missing_object() => Ok(None),
                    Err(e) => Err(e),
                }
            }
        })
        .buffer_unordered(GETS)
        .try_collect()
        .await?;
    Ok(read.into_iter().flatten().collect())
}

/// Of the objects `found`, each a URL and its age in seconds, those to delete: not `live`, and older than `grace`.
fn doomed(found: Vec<(String, i64)>, live: &HashSet<String>, grace: Duration) -> Vec<String> {
    found
        .into_iter()
        .filter(|(url, age)| {
            let old = u64::try_from(*age).is_ok_and(|age| age >= grace.as_secs());
            old && key(url).is_some_and(|key| !live.contains(key))
        })
        .map(|(url, _)| url)
        .collect()
}

/// Deletes what the projection of the table at `location` holds that the snapshots `kept` do not reach and that is
/// older than `grace`, and answers how many objects.
pub(crate) async fn prune(storage: &Storage, location: &str, kept: &[i64], grace: Duration) -> Result<usize> {
    let live = reachable(storage, location, kept).await?;
    let root = generated(location);
    let base = root.trim_end_matches("/metalake-manifests");
    let mut deleted = 0;
    for dir in PRUNED {
        let found = storage.list(&format!("{base}/{dir}")).await?;
        for url in doomed(found, &live, grace) {
            storage.delete(&url).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest6_prune_key_ignores_spelling() {
        let load = "file:///C:/lake/db/t-1/metalake-manifests/snap-7.avro";
        let listed = "file:///C:/lake/db/t-1/metalake-manifests/snap-7.avro";
        assert_eq!(key(load), Some("metalake-manifests/snap-7.avro"));
        assert_eq!(key(load), key(listed));
        assert_eq!(
            key("gs://lake/t/metalake-deletes/5-9.parquet"),
            Some("metalake-deletes/5-9.parquet")
        );
        assert_eq!(
            key("gs://lake/t/data/f.parquet"),
            None,
            "a data file is not the projection's"
        );
        assert_eq!(
            key("gs://lake/t/metalake-metadata/v1-a.metadata.json"),
            None,
            "a load prunes those"
        );
    }

    #[test]
    fn rest6_prune_doomed_unreached_past_grace() {
        let live: HashSet<String> = ["metalake-manifests/snap-2.avro", "metalake-manifests/a-1-0.avro"]
            .map(str::to_string)
            .into();
        let at = |name: &str| format!("gs://lake/t/{name}");
        let found = vec![
            (at("metalake-manifests/snap-1.avro"), 100),
            (at("metalake-manifests/snap-2.avro"), 100),
            (at("metalake-manifests/a-1-0.avro"), 100),
            (at("metalake-manifests/a-0-0.avro"), 100),
            (at("metalake-manifests/a-3-0.avro"), 5),
            (at("metalake-deletes/4-1.parquet"), 100),
        ];
        let grace = Duration::from_secs(60);
        assert_eq!(
            doomed(found, &live, grace),
            [
                at("metalake-manifests/snap-1.avro"),
                at("metalake-manifests/a-0-0.avro"),
                at("metalake-deletes/4-1.parquet")
            ],
            "what nothing reaches and is past its grace; a-3, unreached but recent, stays"
        );
    }
}
