//! Credentials for a table's files on Azure (REST-11): a SAS on the table's directory (`sr=d`), read and list for a
//! reader, and add, create, write and delete besides for a writer, signed one of two ways, as the server's own store is
//! reached:
//!
//! - with the storage account's key (`AZURE_STORAGE_ACCOUNT_KEY` or its aliases; an emulator's, given none), a service
//!   SAS <https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas>;
//! - with an Entra identity (a managed identity, a service principal, a workload identity, as `object_store` finds it),
//!   a user delegation SAS, signed with a key Azure delegates to that identity, which is kept while it lasts
//!   <https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas>. Accounts that refuse
//!   their keys, as Microsoft advises, are reached this way alone.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_util::future::BoxFuture;
use hmac::{Hmac, KeyInit, Mac};
use object_store::azure::{AzureCredential, MicrosoftAzureBuilder};
use sha2::Sha256;

use super::{Access, Minted, Minter, element};

/// The version a SAS is signed for, as Azure's own SDKs sign today.
const VERSION: &str = "2026-10-06";
/// How long a credential lasts, and how far before now it starts, for clocks that disagree.
const LASTING_S: i64 = 3600;
const SKEW_S: i64 = 60;
/// How long a delegated key is asked for, and how long before it expires it is asked for again.
const DELEGATION_S: i64 = 24 * 3600;
const DELEGATION_MARGIN_S: i64 = 2 * LASTING_S;
/// The key of an emulator's account, as Azurite and Floci-az hold it.
const EMULATOR_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// How the server signs, and where its store is.
#[derive(Debug, Clone)]
pub struct Azure {
    key: Option<String>,
    /// The account of a location that names none, as `az://container/path` does.
    account: Option<String>,
    /// The Blob endpoint of a store of its own, as an emulator's, instead of the account's on Azure.
    endpoint: Option<String>,
    http: bool,
    client: reqwest::Client,
    /// The keys delegated to the server's identity, by account.
    delegated: Arc<Mutex<HashMap<String, Delegated>>>,
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
            http: emulator || endpoint.as_deref().is_some_and(|e| e.starts_with("http://")),
            endpoint,
            client: reqwest::Client::new(),
            delegated: Arc::default(),
        }
    }

    async fn mint_azure(&self, prefix: &str, access: Access) -> Result<Minted, String> {
        let place = Place::of(prefix, self.account.as_deref())?;
        let now = Utc::now();
        let start = now - Duration::seconds(SKEW_S);
        let protocol = if self.http { "https,http" } else { "https" };
        let (sas, expiry) = match &self.key {
            Some(key) => {
                let expiry = now + Duration::seconds(LASTING_S);
                (service_sas(key, &place, access, start, expiry, protocol)?, expiry)
            }
            None => {
                let delegated = self.delegated(&place).await?;
                // A SAS lasts no longer than the key it is signed with.
                let expiry = (now + Duration::seconds(LASTING_S)).min(delegated.expiry);
                (
                    user_delegation_sas(&delegated, &place, access, start, expiry, protocol)?,
                    expiry,
                )
            }
        };
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

    /// The key delegated to the server's identity for `place`'s account: the one kept, while it has time left.
    async fn delegated(&self, place: &Place) -> Result<Delegated, String> {
        let kept = self
            .delegated
            .lock()
            .expect("delegated keys")
            .get(&place.account)
            .cloned();
        if let Some(kept) = kept.filter(|k| k.expiry - Duration::seconds(DELEGATION_MARGIN_S) > Utc::now()) {
            return Ok(kept);
        }
        let token = self.identity_token(place).await?;
        let delegated = self.ask_delegation(&place.account, &token).await?;
        self.delegated
            .lock()
            .expect("delegated keys")
            .insert(place.account.clone(), delegated.clone());
        Ok(delegated)
    }

    /// The Entra token the server's store reaches its account with.
    async fn identity_token(&self, place: &Place) -> Result<String, String> {
        let store = MicrosoftAzureBuilder::from_env()
            .with_account(&place.account)
            .with_container_name(&place.container)
            .build()
            .map_err(|e| format!("the server's Azure store: {e}"))?;
        let credential = store
            .credentials()
            .get_credential()
            .await
            .map_err(|e| format!("the server's Azure credential: {e}"))?;
        match credential.as_ref() {
            AzureCredential::BearerToken(token) => Ok(token.clone()),
            _ => Err(
                "a SAS is signed with the account's key or a key delegated to an Entra identity: \
                 set AZURE_STORAGE_ACCOUNT_KEY, or give the server an identity"
                    .into(),
            ),
        }
    }

    /// A key Azure delegates to the identity `token` is of, for a day.
    async fn ask_delegation(&self, account: &str, token: &str) -> Result<Delegated, String> {
        let blobs = match &self.endpoint {
            Some(endpoint) => endpoint.trim_end_matches('/').to_string(),
            None => format!("https://{account}.blob.core.windows.net"),
        };
        let url = format!("{blobs}/?restype=service&comp=userdelegationkey");
        let now = Utc::now();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>{}</Start><Expiry>{}</Expiry></KeyInfo>",
            stamp(now - Duration::seconds(SKEW_S)),
            stamp(now + Duration::seconds(DELEGATION_S))
        );
        let answer = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("x-ms-version", VERSION)
            .header("x-ms-date", now.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("content-type", "application/xml")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let ok = answer.status().is_success();
        let text = answer.text().await.map_err(|e| format!("{url}'s answer: {e}"))?;
        if !ok {
            let said = element(&text, "Message").unwrap_or(&text);
            return Err(format!("{url} refused a delegated key: {said}"));
        }
        Delegated::of(&text)
    }
}

impl Minter for Azure {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
        Box::pin(self.mint_azure(prefix, access))
    }
}

/// A key Azure delegated to an identity, as it answers `Get User Delegation Key`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Delegated {
    oid: String,
    tid: String,
    start: String,
    expiry: DateTime<Utc>,
    service: String,
    version: String,
    value: String,
}

impl Delegated {
    fn of(xml: &str) -> Result<Self, String> {
        let field = |name: &str| {
            element(xml, name)
                .map(str::to_string)
                .ok_or_else(|| format!("the delegated key has no {name}"))
        };
        let expiry = field("SignedExpiry")?;
        Ok(Self {
            oid: field("SignedOid")?,
            tid: field("SignedTid")?,
            start: field("SignedStart")?,
            expiry: DateTime::parse_from_rfc3339(&expiry)
                .map_err(|e| format!("its expiry {expiry}: {e}"))?
                .with_timezone(&Utc),
            service: field("SignedService")?,
            version: field("SignedVersion")?,
            value: field("Value")?,
        })
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

    fn resource(&self) -> String {
        format!("/blob/{}/{}/{}", self.account, self.container, self.directory)
    }

    fn depth(&self) -> usize {
        self.directory.split('/').filter(|part| !part.is_empty()).count()
    }
}

fn permissions(access: Access) -> &'static str {
    match access {
        Access::Read => "rl",
        Access::ReadWrite => "racwdl",
    }
}

fn sign(key: &str, fields: &[&str]) -> Result<String, String> {
    let engine = base64::engine::general_purpose::STANDARD;
    let key = engine
        .decode(key)
        .map_err(|e| format!("the signing key is not base64: {e}"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|e| e.to_string())?;
    mac.update(fields.join("\n").as_bytes());
    Ok(engine.encode(mac.finalize().into_bytes()))
}

/// A service SAS on `place`'s directory, signed with the account's key, as its query string.
fn service_sas(
    key: &str,
    place: &Place,
    access: Access,
    start: DateTime<Utc>,
    expiry: DateTime<Utc>,
    protocol: &str,
) -> Result<String, String> {
    let (sp, st, se, resource) = (permissions(access), stamp(start), stamp(expiry), place.resource());
    // Permissions, start, expiry, resource, identifier, IP range, protocol, version, resource kind, snapshot time,
    // encryption scope, and the five response headers (version 2020-12-06 on).
    let fields = [
        sp, &st, &se, &resource, "", "", protocol, VERSION, "d", "", "", "", "", "", "", "",
    ];
    let signature = sign(key, &fields)?;
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("sv", VERSION)
        .append_pair("sr", "d")
        .append_pair("sdd", &place.depth().to_string())
        .append_pair("sp", sp)
        .append_pair("st", &st)
        .append_pair("se", &se)
        .append_pair("spr", protocol)
        .append_pair("sig", &signature);
    Ok(query.finish())
}

/// A user delegation SAS on `place`'s directory, signed with a key delegated to an identity, as its query string.
fn user_delegation_sas(
    delegated: &Delegated,
    place: &Place,
    access: Access,
    start: DateTime<Utc>,
    expiry: DateTime<Utc>,
    protocol: &str,
) -> Result<String, String> {
    let (sp, st, se, resource) = (permissions(access), stamp(start), stamp(expiry), place.resource());
    let key_expiry = stamp(delegated.expiry);
    // Permissions, start, expiry, resource; the delegated key's object id, tenant id, start, expiry, service and
    // version; the authorized and unauthorized object ids, correlation id, delegated user's tenant and object ids; IP
    // range, protocol, version, resource kind, snapshot time, encryption scope; the request's headers and query; and the
    // five response headers (version 2026-04-06 on).
    let fields = [
        sp,
        &st,
        &se,
        &resource,
        &delegated.oid,
        &delegated.tid,
        &delegated.start,
        &key_expiry,
        &delegated.service,
        &delegated.version,
        "",
        "",
        "",
        "",
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
        "",
        "",
    ];
    let signature = sign(&delegated.value, &fields)?;
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("sv", VERSION)
        .append_pair("sr", "d")
        .append_pair("sdd", &place.depth().to_string())
        .append_pair("sp", sp)
        .append_pair("st", &st)
        .append_pair("se", &se)
        .append_pair("spr", protocol)
        .append_pair("skoid", &delegated.oid)
        .append_pair("sktid", &delegated.tid)
        .append_pair("skt", &delegated.start)
        .append_pair("ske", &key_expiry)
        .append_pair("sks", &delegated.service)
        .append_pair("skv", &delegated.version)
        .append_pair("sig", &signature);
    Ok(query.finish())
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

    fn hour() -> (DateTime<Utc>, DateTime<Utc>) {
        (
            Utc.with_ymd_and_hms(2026, 11, 11, 16, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 11, 17, 0, 0).unwrap(),
        )
    }

    fn place() -> Place {
        Place::of("abfss://lake@devstoreaccount1.dfs.core.windows.net/db/t/", None).unwrap()
    }

    /// The signatures Azure's own SDK (`azure-storage-file-datalake`, `generate_directory_sas`) gives the same SAS.
    #[test]
    fn service_sas_as_azure_sdk() {
        let (start, expiry) = hour();
        let read = service_sas(EMULATOR_KEY, &place(), Access::Read, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&read), "Onn11MYmI3I8Aq/S6CEGsV+TEB9oC2KNUDqnGKJ5avs=");
        assert!(
            read.contains("sr=d") && read.contains("sdd=2") && read.contains("sp=rl"),
            "{read}"
        );
        let write = service_sas(EMULATOR_KEY, &place(), Access::ReadWrite, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&write), "fnQUD4R8bROTSEmCpeqTk0uHMHm1BsSXv9l34pu9M+A=");
    }

    fn delegated() -> Delegated {
        Delegated::of(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><UserDelegationKey>\
             <SignedOid>11111111-1111-1111-1111-111111111111</SignedOid>\
             <SignedTid>22222222-2222-2222-2222-222222222222</SignedTid>\
             <SignedStart>2026-11-11T15:00:00Z</SignedStart><SignedExpiry>2026-11-12T15:00:00Z</SignedExpiry>\
             <SignedService>b</SignedService><SignedVersion>2026-10-06</SignedVersion>\
             <Value>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</Value>\
             </UserDelegationKey>",
        )
        .unwrap()
    }

    /// The same, for a user delegation SAS signed with a key delegated to an identity (`generate_directory_sas` given a
    /// `UserDelegationKey` with these fields).
    #[test]
    fn user_delegation_sas_as_azure_sdk() {
        let (start, expiry) = hour();
        let key = delegated();
        assert_eq!(key.expiry, Utc.with_ymd_and_hms(2026, 11, 12, 15, 0, 0).unwrap());
        let read = user_delegation_sas(&key, &place(), Access::Read, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&read), "FTYKpe3rH1zoSJQZFnXOWOtZfhActC60shUY1b8UXP4=");
        for field in [
            "skoid=11111111",
            "sktid=22222222",
            "skt=2026-11-11T15",
            "ske=2026-11-12T15",
            "sks=b",
        ] {
            assert!(read.contains(field), "{field} in {read}");
        }
        let write = user_delegation_sas(&key, &place(), Access::ReadWrite, start, expiry, "https,http").unwrap();
        assert_eq!(sig(&write), "Ihle1NE40be48SatIaEt5Jq2Vi+6q/iIngFHR5IF7DI=");
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

    #[tokio::test]
    async fn config_by_host() {
        let azure = Azure {
            key: Some(EMULATOR_KEY.into()),
            account: None,
            endpoint: None,
            http: false,
            client: reqwest::Client::new(),
            delegated: Arc::default(),
        };
        let minted = azure
            .mint_azure("abfss://lake@acct.dfs.core.windows.net/db/t/", Access::Read)
            .await
            .unwrap();
        let sas = &minted.config["adls.sas-token.acct.dfs.core.windows.net"];
        assert!(sas.contains("spr=https&"), "https alone off an emulator: {sas}");
        assert_eq!(
            minted.config["adls.sas-token-expires-at-ms.acct.dfs.core.windows.net"],
            minted.expires_at_ms.to_string()
        );
        assert_eq!(&minted.config["adls.sas-token.acct"], sas);
    }
}
