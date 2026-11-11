//! Credentials vended for a table's files (REST-11), what every store shares: which store a table's files lie on, by its
//! location; the prefix a credential is for; the endpoint a client renews it at; and a credential kept while it has
//! time left, so that loads of a table ask the store's token service once. How a credential is minted is each store's
//! [`Minter`]: a store with none vends nothing, as a local table does.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod azure;
pub mod s3;
mod sigv4;

use futures_util::future::BoxFuture;

use crate::wire::credentials::StorageCredential;

/// What a credential lets its holder do with the files under its prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Access {
    /// A `reader`'s (REST-10).
    Read,
    /// A `writer`'s.
    ReadWrite,
}

/// A credential a store's token service minted: what a client sets for it, and when it expires, in milliseconds since
/// the epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Minted {
    pub config: BTreeMap<String, String>,
    pub expires_at_ms: i64,
}

/// How one store mints a credential narrowed to a prefix and an access (REST-11).
pub trait Minter: Send + Sync {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>>;
}

/// How long before it expires a kept credential is minted again: a client given one renews it no sooner.
const MARGIN_MS: i64 = 5 * 60 * 1000;

/// The minters of the stores a server vends for, by the scheme of a table's location, and what they minted.
#[derive(Clone, Default)]
pub struct Vendor {
    minters: HashMap<String, Arc<dyn Minter>>,
    kept: Arc<Mutex<HashMap<(String, Access), Minted>>>,
}

impl std::fmt::Debug for Vendor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut schemes: Vec<&String> = self.minters.keys().collect();
        schemes.sort();
        f.debug_struct("Vendor").field("schemes", &schemes).finish()
    }
}

impl Vendor {
    /// Vends for locations of `scheme` through `minter`.
    pub fn with(mut self, scheme: &str, minter: Arc<dyn Minter>) -> Self {
        self.minters.insert(scheme.to_string(), minter);
        self
    }

    fn minter(&self, location: &str) -> Option<&Arc<dyn Minter>> {
        self.minters.get(scheme(location)?)
    }

    /// Whether a table at `location` is given credentials: its files lie on a store this server mints for.
    pub fn serves(&self, location: &str) -> bool {
        self.minter(location).is_some()
    }

    /// The credentials for a table at `location`, with `access`, each saying `refresh` as where a client renews it; none
    /// for a store this server does not mint for.
    pub async fn vend(&self, location: &str, access: Access, refresh: &str) -> Result<Vec<StorageCredential>, String> {
        let Some(minter) = self.minter(location) else {
            return Ok(Vec::new());
        };
        let prefix = prefix(location);
        let key = (prefix.clone(), access);
        let kept = self.kept.lock().expect("kept credentials").get(&key).cloned();
        let minted = match kept.filter(|m| m.expires_at_ms - MARGIN_MS > now_ms()) {
            Some(minted) => minted,
            None => {
                let minted = minter.mint(&prefix, access).await?;
                self.kept.lock().expect("kept credentials").insert(key, minted.clone());
                minted
            }
        };
        let mut config = minted.config;
        config.insert(refresh_key(location).to_string(), refresh.to_string());
        Ok(vec![StorageCredential { prefix, config }])
    }
}

fn scheme(location: &str) -> Option<&str> {
    location.split_once("://").map(|(scheme, _)| scheme)
}

/// The prefix a credential for a table at `location` is for: its location as a directory, so that a table's credential
/// does not open another whose name starts with its own.
pub fn prefix(location: &str) -> String {
    format!("{}/", location.trim_end_matches('/'))
}

/// The key under which Iceberg's client for the store reads where to renew a credential.
fn refresh_key(location: &str) -> &'static str {
    match scheme(location) {
        Some("gs") => "gcs.oauth2.refresh-credentials-endpoint",
        Some("abfs" | "abfss" | "wasb" | "wasbs") => "adls.refresh-credentials-endpoint",
        _ => "client.refresh-credentials-endpoint",
    }
}

fn now_ms() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

/// `loadCredentials`'s path for a table, relative to the catalog's URI, as Iceberg's clients call it to renew. A
/// namespace's levels are joined by the unit separator, as in every route.
pub fn refresh_path(namespace: &str, table: &str) -> String {
    format!(
        "v1/namespaces/{}/tables/{}/credentials",
        encoded(namespace),
        encoded(table)
    )
}

/// A path segment, with every byte but the unreserved ones percent-encoded.
fn encoded(segment: &str) -> String {
    segment
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            other => format!("%{other:02X}"),
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A minter that says what it was asked, and counts how often; its credentials expire `lasting` ms from now.
    pub(crate) struct Fake {
        pub(crate) minted: AtomicUsize,
        pub(crate) lasting: i64,
    }

    impl Minter for Fake {
        fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
            Box::pin(async move {
                self.minted.fetch_add(1, Ordering::SeqCst);
                let config = BTreeMap::from([
                    ("fake.prefix".to_string(), prefix.to_string()),
                    ("fake.access".to_string(), format!("{access:?}")),
                ]);
                Ok(Minted {
                    config,
                    expires_at_ms: now_ms() + self.lasting,
                })
            })
        }
    }

    fn fake_vendor(lasting: i64) -> (Vendor, Arc<Fake>) {
        let fake = Arc::new(Fake {
            minted: AtomicUsize::new(0),
            lasting,
        });
        (Vendor::default().with("s3", fake.clone()), fake)
    }

    #[tokio::test]
    async fn vend_by_store() {
        let (vendor, _) = fake_vendor(3_600_000);
        assert!(vendor.serves("s3://bucket/lake/db/t"));
        assert!(!vendor.serves("file:///tmp/lake/db/t"), "a local table vends none");
        let none = vendor
            .vend("file:///tmp/lake/db/t", Access::ReadWrite, "r")
            .await
            .unwrap();
        assert!(none.is_empty());

        let vended = vendor
            .vend("s3://bucket/lake/db/t", Access::Read, "v1/x")
            .await
            .unwrap();
        assert_eq!(vended.len(), 1);
        assert_eq!(
            vended[0].prefix, "s3://bucket/lake/db/t/",
            "a directory, not a name's start"
        );
        assert_eq!(vended[0].config["fake.access"], "Read");
        assert_eq!(vended[0].config["client.refresh-credentials-endpoint"], "v1/x");
    }

    #[tokio::test]
    async fn vend_kept_while_lasting() {
        let (vendor, fake) = fake_vendor(3_600_000);
        for _ in 0..3 {
            vendor.vend("s3://b/t", Access::ReadWrite, "r").await.unwrap();
        }
        assert_eq!(fake.minted.load(Ordering::SeqCst), 1, "kept while it lasts");
        vendor.vend("s3://b/t", Access::Read, "r").await.unwrap();
        vendor.vend("s3://b/other", Access::ReadWrite, "r").await.unwrap();
        assert_eq!(fake.minted.load(Ordering::SeqCst), 3, "one per prefix and access");

        let (vendor, fake) = fake_vendor(MARGIN_MS / 2);
        vendor.vend("s3://b/t", Access::ReadWrite, "r").await.unwrap();
        vendor.vend("s3://b/t", Access::ReadWrite, "r").await.unwrap();
        assert_eq!(fake.minted.load(Ordering::SeqCst), 2, "minted again near its expiry");
    }

    #[test]
    fn refresh_key_by_store() {
        assert_eq!(refresh_key("s3://b/t"), "client.refresh-credentials-endpoint");
        assert_eq!(refresh_key("gs://b/t"), "gcs.oauth2.refresh-credentials-endpoint");
        assert_eq!(
            refresh_key("abfss://c@a.dfs.core.windows.net/t"),
            "adls.refresh-credentials-endpoint"
        );
    }

    #[test]
    fn refresh_path_encoded() {
        assert_eq!(refresh_path("db", "t"), "v1/namespaces/db/tables/t/credentials");
        assert_eq!(
            refresh_path("a\u{1f}b", "my table"),
            "v1/namespaces/a%1Fb/tables/my%20table/credentials"
        );
    }

    #[test]
    fn prefix_is_directory() {
        assert_eq!(prefix("s3://b/lake/t"), "s3://b/lake/t/");
        assert_eq!(prefix("s3://b/lake/t/"), "s3://b/lake/t/");
    }
}
