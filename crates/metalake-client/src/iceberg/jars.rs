//! The jars an engine is given as files (Flink's `pipeline.jars`), fetched once from Maven Central into this user's
//! MetaLake directory, as `metalake-local` fetches Postgres. Each is held to the SHA-512 pinned here for its version,
//! so that a jar is the one CI tested, whatever the mirror serves; one with no pinned hash is not fetched.

use std::fs;
use std::path::{Path, PathBuf};

use object_store::{ObjectStore, ObjectStoreExt};
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectPath;
use sha2::{Digest, Sha512};

use super::profile::Artifact;
use crate::{Error, Result, server};

const MAVEN: &str = "https://repo1.maven.org/maven2";

/// The SHA-512 of each jar a profile names, as Maven Central publishes it beside the jar.
const PINNED: &[(&str, &str, &str)] = &[
    (
        "iceberg-flink-runtime-1.20",
        "1.11.0",
        "f39f4d984fd429f12dbe5eb2df7358d33602054d17a0c2d03e2cc98e4c95cf2de949d2f31da3d2c4c926c6755ca878621fb0e02c2fb5edf7526c362f5a0f7182",
    ),
    (
        "iceberg-aws-bundle",
        "1.11.0",
        "bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0",
    ),
    (
        "iceberg-gcp-bundle",
        "1.11.0",
        "c38fb7c6ac3d04145b62ec79114af0bc6ccf0f6ab7a219cbf5286efd0dd63c33a0157a2662dc062c505defe203855b730b8ce30f0f7ddf0d60d79767ba27f5e3",
    ),
    (
        "iceberg-azure-bundle",
        "1.11.0",
        "4d14e6a631e4f1abb58b185847437873569e47b11a3bfe3697724591c5011bf8f330a0ed4eedbdfa5c7d44e03c20d0fb7a8e5a495ffcd0b4b826480ef30ffdc6",
    ),
];

/// The directory of the jars this user's engines were given.
pub(crate) fn directory() -> Result<PathBuf> {
    let servers = server::directory()?;
    Ok(servers.parent().unwrap_or(&servers).join("jars"))
}

/// Where `artifact`'s jar is, once fetched.
pub(crate) fn path(directory: &Path, artifact: &Artifact) -> PathBuf {
    directory.join(artifact.file_name())
}

/// The jars of `artifacts`, each fetched unless it already is.
pub(crate) async fn fetch(artifacts: &[Artifact]) -> Result<Vec<PathBuf>> {
    let maven = HttpBuilder::new()
        .with_url(MAVEN)
        .build()
        .map_err(|e| Error::failed(format!("Maven Central: {e}")))?;
    let directory = directory()?;
    let mut fetched = Vec::new();
    for artifact in artifacts {
        let sha512 = pinned(artifact)?;
        fetched.push(fetch_one(&maven, &directory, artifact, sha512).await?);
    }
    Ok(fetched)
}

/// The SHA-512 pinned for `artifact`.
fn pinned(artifact: &Artifact) -> Result<&'static str> {
    PINNED
        .iter()
        .find(|(name, version, _)| *name == artifact.name && *version == artifact.version)
        .map(|(_, _, sha512)| *sha512)
        .ok_or_else(|| Error::failed(format!("no hash is pinned for {}", artifact.coordinate())))
}

/// One jar, from `store`, into `directory`: written whole under another name and renamed once its hash is the one
/// expected, so that a jar there is always one that was checked.
async fn fetch_one(store: &dyn ObjectStore, directory: &Path, artifact: &Artifact, sha512: &str) -> Result<PathBuf> {
    let target = path(directory, artifact);
    if target.exists() {
        return Ok(target);
    }
    let failed = |e: object_store::Error| Error::failed(format!("fetching {}: {e}", artifact.url()));
    let bytes = store
        .get(&ObjectPath::from(artifact.path()))
        .await
        .map_err(failed)?
        .bytes()
        .await
        .map_err(failed)?;
    let got = hex(&Sha512::digest(&bytes));
    if got != sha512 {
        return Err(Error::failed(format!(
            "{} is not the jar pinned: its SHA-512 is {got}, not {sha512}",
            artifact.url()
        )));
    }
    fs::create_dir_all(directory)?;
    let partial = target.with_extension(format!("jar.{}", uuid::Uuid::new_v4().simple()));
    fs::write(&partial, &bytes)?;
    if let Err(e) = fs::rename(&partial, &target) {
        let _ = fs::remove_file(&partial);
        // Another caller renamed the same jar first.
        if !target.exists() {
            return Err(e.into());
        }
    }
    Ok(target)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    use super::*;

    fn runtime() -> Artifact {
        Artifact::iceberg("iceberg-flink-runtime-1.20")
    }

    async fn store_with(artifact: &Artifact, bytes: &'static [u8]) -> InMemory {
        let store = InMemory::new();
        let path = ObjectPath::from(artifact.path());
        store.put(&path, PutPayload::from_static(bytes)).await.unwrap();
        store
    }

    fn temporary() -> PathBuf {
        std::env::temp_dir().join(format!("metalake-jars-{}", uuid::Uuid::new_v4().simple()))
    }

    #[tokio::test]
    async fn fetch_checks_hash() {
        let artifact = runtime();
        let store = store_with(&artifact, b"a jar").await;
        let directory = temporary();
        let right = hex(&Sha512::digest(b"a jar"));

        let wrong = fetch_one(&store, &directory, &artifact, &hex(&Sha512::digest(b"another")))
            .await
            .unwrap_err()
            .to_string();
        assert!(wrong.contains("is not the jar pinned"), "{wrong}");
        assert!(
            !path(&directory, &artifact).exists(),
            "nothing kept that was not checked"
        );

        let fetched = fetch_one(&store, &directory, &artifact, &right).await.unwrap();
        assert_eq!(fetched, path(&directory, &artifact));
        assert_eq!(fs::read(&fetched).unwrap(), b"a jar");
        let left: Vec<_> = fs::read_dir(&directory).unwrap().collect();
        assert_eq!(left.len(), 1, "no partial file left");
        fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn fetch_keeps_fetched_jar() {
        let artifact = runtime();
        let directory = temporary();
        fs::create_dir_all(&directory).unwrap();
        fs::write(path(&directory, &artifact), b"fetched before").unwrap();
        // The store has none: a jar there is not fetched again.
        let fetched = fetch_one(&InMemory::new(), &directory, &artifact, "unused")
            .await
            .unwrap();
        assert_eq!(fs::read(&fetched).unwrap(), b"fetched before");
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn pinned_hashes() {
        assert!(pinned(&runtime()).unwrap().len() == 128);
        for store in [
            super::super::Store::S3,
            super::super::Store::Gcs,
            super::super::Store::Azure,
        ] {
            assert!(pinned(&store.bundle().unwrap()).is_ok(), "{store:?}");
        }
        assert!(pinned(&Artifact::iceberg("iceberg-hive-runtime")).is_err());
    }

    /// The real jar, from Maven Central: run by hand, `--ignored`.
    #[tokio::test]
    #[ignore]
    async fn fetch_flink_runtime_from_maven() {
        let fetched = fetch(&[runtime()]).await.unwrap();
        assert!(fs::metadata(&fetched[0]).unwrap().len() > 1_000_000);
    }
}
