//! Credentials for a table's files on Azure (REST-11): a service SAS on the table's directory (`sr=d`), signed with the
//! storage account's key, read and list for a reader, and add, create, write and delete besides for a writer.
//! <https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas>. The key is the one the server's own
//! store reads, from `AZURE_STORAGE_ACCOUNT_KEY` or its aliases; an emulator's, given none, when the store is one.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_util::future::BoxFuture;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use super::{Access, Minted, Minter};

/// The version a SAS is signed for, as Azure's own SDKs sign today.
const VERSION: &str = "2026-10-06";
/// How long a credential lasts, and how far before now it starts, for clocks that disagree.
const LASTING_S: i64 = 3600;
const SKEW_S: i64 = 60;
/// The key of an emulator's account, as Azurite and Floci-az hold it.
const EMULATOR_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// The account key, and whether the store is reached over plain HTTP, as an emulator is.
#[derive(Debug, Clone)]
pub struct Azure {
    key: Option<String>,
    /// The account of a location that names none, as `az://container/path` does.
    account: Option<String>,
    http: bool,
}

impl Azure {
    /// As the environment says, as the server's store reads it.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let emulator = var("AZURE_STORAGE_USE_EMULATOR").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let endpoint = var("AZURE_STORAGE_ENDPOINT").or_else(|| var("AZURE_ENDPOINT"));
        let key = var("AZURE_STORAGE_ACCOUNT_KEY")
            .or_else(|| var("AZURE_STORAGE_ACCESS_KEY"))
            .or_else(|| var("AZURE_STORAGE_MASTER_KEY"))
            .or_else(|| emulator.then(|| EMULATOR_KEY.to_string()));
        Self {
            key,
            account: var("AZURE_STORAGE_ACCOUNT_NAME").or_else(|| emulator.then(|| "devstoreaccount1".to_string())),
            http: emulator || endpoint.is_some_and(|e| e.starts_with("http://")),
        }
    }

    fn mint_azure(&self, prefix: &str, access: Access) -> Result<Minted, String> {
        let key = self.key.as_deref().ok_or_else(|| {
            "a SAS is signed with the storage account's key: set AZURE_STORAGE_ACCOUNT_KEY".to_string()
        })?;
        let place = Place::of(prefix, self.account.as_deref())?;
        let now = Utc::now();
        let (start, expiry) = (now - Duration::seconds(SKEW_S), now + Duration::seconds(LASTING_S));
        let protocol = if self.http { "https,http" } else { "https" };
        let sas = directory_sas(key, &place, access, start, expiry, protocol)?;
        let config = BTreeMap::from([
            (format!("adls.sas-token.{}", place.host), sas.clone()),
            (
                format!("adls.sas-token-expires-at-ms.{}", place.host),
                expiry.timestamp_millis().to_string(),
            ),
            // As Iceberg before 1.8 reads it, by the account alone.
            (format!("adls.sas-token.{}", place.account), sas),
        ]);
        Ok(Minted {
            config,
            expires_at_ms: expiry.timestamp_millis(),
        })
    }
}

impl Minter for Azure {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
        Box::pin(async move { self.mint_azure(prefix, access) })
    }
}

/// Where a prefix's files are: an account, its host as clients key credentials by, a container and a directory.
#[derive(Debug, PartialEq, Eq)]
struct Place {
    account: String,
    host: String,
    container: String,
    directory: String,
}

impl Place {
    /// `abfss://container@account.dfs.core.windows.net/dir/`, or `az://container/dir/` of `account`.
    fn of(prefix: &str, account: Option<&str>) -> Result<Self, String> {
        let (scheme, rest) = prefix
            .split_once("://")
            .ok_or_else(|| format!("{prefix} is not a store's location"))?;
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let directory = path.trim_matches('/').to_string();
        match authority.split_once('@') {
            Some((container, host)) => {
                let account = host.split('.').next().unwrap_or(host).to_string();
                Ok(Self {
                    account,
                    host: host.to_string(),
                    container: container.to_string(),
                    directory,
                })
            }
            None if matches!(scheme, "az" | "azure") => {
                let account = account
                    .ok_or_else(|| format!("{prefix} names no account: set AZURE_STORAGE_ACCOUNT_NAME"))?
                    .to_string();
                let host = format!("{account}.dfs.core.windows.net");
                Ok(Self {
                    account,
                    host,
                    container: authority.to_string(),
                    directory,
                })
            }
            None => Err(format!("{prefix} names no container and account")),
        }
    }
}

/// A service SAS on `place`'s directory, as its query string.
fn directory_sas(
    key: &str,
    place: &Place,
    access: Access,
    start: DateTime<Utc>,
    expiry: DateTime<Utc>,
    protocol: &str,
) -> Result<String, String> {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::STANDARD;
    let key = engine
        .decode(key)
        .map_err(|e| format!("the storage account's key is not base64: {e}"))?;
    let permissions = match access {
        Access::Read => "rl",
        Access::ReadWrite => "racwdl",
    };
    let (start, expiry) = (stamp(start), stamp(expiry));
    let resource = format!("/blob/{}/{}/{}", place.account, place.container, place.directory);
    let depth = place.directory.split('/').filter(|part| !part.is_empty()).count();
    // The service SAS's fields, in order (version 2020-12-06 on): permissions, start, expiry, resource, identifier, IP
    // range, protocol, version, resource kind, snapshot time, encryption scope, and the five response headers.
    let fields = [
        permissions,
        &start,
        &expiry,
        &resource,
        "",
        "",
        protocol,
        VERSION,
        "d",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ];
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|e| e.to_string())?;
    mac.update(fields.join("\n").as_bytes());
    let signature = engine.encode(mac.finalize().into_bytes());
    Ok(url::form_urlencoded::Serializer::new(String::new())
        .append_pair("sv", VERSION)
        .append_pair("sr", "d")
        .append_pair("sdd", &depth.to_string())
        .append_pair("sp", permissions)
        .append_pair("st", &start)
        .append_pair("se", &expiry)
        .append_pair("spr", protocol)
        .append_pair("sig", &signature)
        .finish())
}

fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn sig(sas: &str) -> String {
        url::form_urlencoded::parse(sas.as_bytes())
            .find(|(k, _)| k == "sig")
            .map(|(_, v)| v.into_owned())
            .unwrap()
    }

    /// The signatures Azure's own SDK (`azure-storage-file-datalake`, `generate_directory_sas`) gives the same SAS.
    #[test]
    fn directory_sas_as_azure_sdk() {
        let place = Place::of("abfss://lake@devstoreaccount1.dfs.core.windows.net/db/t/", None).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 11, 11, 16, 0, 0).unwrap();
        let expiry = Utc.with_ymd_and_hms(2026, 11, 11, 17, 0, 0).unwrap();
        let read = directory_sas(EMULATOR_KEY, &place, Access::Read, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&read), "Onn11MYmI3I8Aq/S6CEGsV+TEB9oC2KNUDqnGKJ5avs=");
        assert!(
            read.contains("sr=d") && read.contains("sdd=2") && read.contains("sp=rl"),
            "{read}"
        );
        let write = directory_sas(EMULATOR_KEY, &place, Access::ReadWrite, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&write), "fnQUD4R8bROTSEmCpeqTk0uHMHm1BsSXv9l34pu9M+A=");
    }

    #[test]
    fn place_of_location() {
        assert_eq!(
            Place::of("abfss://lake@acct.dfs.core.windows.net/db/t/", None).unwrap(),
            Place {
                account: "acct".into(),
                host: "acct.dfs.core.windows.net".into(),
                container: "lake".into(),
                directory: "db/t".into(),
            }
        );
        let az = Place::of("az://lake/db/t/", Some("acct")).unwrap();
        assert_eq!(
            (az.host.as_str(), az.container.as_str()),
            ("acct.dfs.core.windows.net", "lake")
        );
        assert!(
            Place::of("az://lake/db/t/", None).is_err(),
            "an az location needs the account"
        );
    }

    #[test]
    fn config_by_host() {
        let azure = Azure {
            key: Some(EMULATOR_KEY.into()),
            account: None,
            http: false,
        };
        let minted = azure
            .mint_azure("abfss://lake@acct.dfs.core.windows.net/db/t/", Access::Read)
            .unwrap();
        let sas = &minted.config["adls.sas-token.acct.dfs.core.windows.net"];
        assert!(sas.contains("spr=https&"), "https alone off an emulator: {sas}");
        assert_eq!(
            minted.config["adls.sas-token-expires-at-ms.acct.dfs.core.windows.net"],
            minted.expires_at_ms.to_string()
        );
        assert_eq!(&minted.config["adls.sas-token.acct"], sas);
        let none = Azure {
            key: None,
            account: None,
            http: false,
        };
        assert!(
            none.mint_azure("abfss://lake@acct.dfs.core.windows.net/t/", Access::Read)
                .is_err()
        );
    }
}
