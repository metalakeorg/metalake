//! Credentials for a table's files on GCS (REST-11): an OAuth2 access token downscoped by Google's token service (STS)
//! with a Credential Access Boundary, which narrows the server's own token to the table's prefix, and to reading or to
//! reading and writing, as Polaris writes it (`legacyObjectReader` and `objectViewer` to read, `legacyBucketWriter` to
//! write). <https://cloud.google.com/iam/docs/downscoping-short-lived-credentials>
//!
//! The server's own token: a service account's key is exchanged for one at its `token_uri`, by a JWT the server signs
//! (`object_store` signs its own JWT and uses it as is, which STS does not take); otherwise the one the server's store
//! gets, from application default credentials or an instance's metadata server. An emulator, as Floci-gcp, is named by
//! `STORAGE_EMULATOR_HOST`, which serves the token service too.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::future::BoxFuture;
use object_store::gcp::GoogleCloudStorageBuilder;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Access, Minted, Minter};

const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const STS: &str = "https://sts.googleapis.com/v1/token";
const ACCESS_TOKEN: &str = "urn:ietf:params:oauth:token-type:access_token";
/// How long the token the server asks for itself lasts.
const LASTING_S: i64 = 3600;

/// A service account's key, as its JSON file has it.
#[derive(Debug, Clone, Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default)]
    private_key_id: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".into()
}

/// Where Google's services are, and the server's own service account if it has one.
#[derive(Debug, Clone)]
pub struct Gcs {
    account: Option<ServiceAccount>,
    /// An emulator's address, which serves storage and the token service at once.
    emulator: Option<String>,
    client: reqwest::Client,
}

impl Gcs {
    /// As the environment says, as the server's store reads it.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let key = var("GOOGLE_SERVICE_ACCOUNT_KEY").or_else(|| {
            var("GOOGLE_SERVICE_ACCOUNT")
                .or_else(|| var("GOOGLE_APPLICATION_CREDENTIALS"))
                .and_then(|path| std::fs::read_to_string(path).ok())
        });
        Self {
            account: key.and_then(|json| serde_json::from_str(&json).ok()),
            emulator: var("STORAGE_EMULATOR_HOST").map(|host| host.trim_end_matches('/').to_string()),
            client: reqwest::Client::new(),
        }
    }

    fn sts(&self) -> String {
        match &self.emulator {
            Some(host) => format!("{host}/v1/token"),
            None => STS.into(),
        }
    }

    /// The server's own access token.
    async fn own_token(&self, bucket: &str) -> Result<String, String> {
        if let Some(account) = &self.account {
            return self.exchange_jwt(account).await;
        }
        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| format!("the server's GCS store: {e}"))?;
        let credential = store
            .credentials()
            .get_credential()
            .await
            .map_err(|e| format!("the server's GCS credential: {e}"))?;
        Ok(credential.bearer.clone())
    }

    /// A service account's access token, for a JWT it signs (RFC 7523).
    async fn exchange_jwt(&self, account: &ServiceAccount) -> Result<String, String> {
        let assertion = jwt(account, chrono::Utc::now().timestamp())?;
        let form = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &assertion),
        ];
        let answer = self.post(&account.token_uri, &form).await?;
        token_of(&answer)
    }

    async fn post(&self, url: &str, form: &[(&str, &str)]) -> Result<Value, String> {
        let answer = self
            .client
            .post(url)
            .form(form)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let ok = answer.status().is_success();
        let body: Value = answer.json().await.map_err(|e| format!("{url}'s answer: {e}"))?;
        if !ok {
            let said = body["error_description"]
                .as_str()
                .or(body["error"].as_str())
                .unwrap_or("refused");
            return Err(format!("{url} refused: {said}"));
        }
        Ok(body)
    }

    async fn mint_gcs(&self, prefix: &str, access: Access) -> Result<Minted, String> {
        let (bucket, path) = bucket_and_path(prefix)?;
        let own = self.own_token(bucket).await?;
        let options = boundary(bucket, path, access).to_string();
        let form = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
            ("subject_token_type", ACCESS_TOKEN),
            ("requested_token_type", ACCESS_TOKEN),
            ("subject_token", own.as_str()),
            ("options", options.as_str()),
        ];
        let answer = self.post(&self.sts(), &form).await?;
        let token = token_of(&answer)?;
        let lasting_s = answer["expires_in"].as_i64().unwrap_or(LASTING_S);
        let expires_at_ms = chrono::Utc::now().timestamp_millis() + lasting_s * 1000;
        let mut config = BTreeMap::from([
            ("gcs.oauth2.token".to_string(), token),
            ("gcs.oauth2.token-expires-at".to_string(), expires_at_ms.to_string()),
        ]);
        if let Some(host) = &self.emulator {
            config.insert("gcs.service.host".into(), host.clone());
        }
        Ok(Minted { config, expires_at_ms })
    }
}

impl Minter for Gcs {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
        Box::pin(self.mint_gcs(prefix, access))
    }
}

fn token_of(answer: &Value) -> Result<String, String> {
    answer["access_token"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| "the token service answered no access_token".to_string())
}

/// A JWT a service account signs to be given an access token, issued at `now`.
fn jwt(account: &ServiceAccount, now: i64) -> Result<String, String> {
    let header = json!({"alg": "RS256", "typ": "JWT", "kid": account.private_key_id});
    let claims = json!({
        "iss": account.client_email,
        "scope": SCOPE,
        "aud": account.token_uri,
        "iat": now,
        "exp": now + LASTING_S,
    });
    let signed = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let key = RsaKeyPair::from_pkcs8(&pem_der(&account.private_key)?)
        .map_err(|e| format!("the service account's key: {e}"))?;
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        signed.as_bytes(),
        &mut signature,
    )
    .map_err(|e| format!("signing with the service account's key: {e}"))?;
    Ok(format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

/// The DER of a PEM key, its lines between the markers.
fn pem_der(pem: &str) -> Result<Vec<u8>, String> {
    let body: String = pem.lines().filter(|line| !line.starts_with("-----")).collect();
    base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .map_err(|e| format!("the service account's key is not PEM: {e}"))
}

/// The bucket and the object path a prefix such as `gs://bucket/lake/db/t/` names.
fn bucket_and_path(prefix: &str) -> Result<(&str, &str), String> {
    let rest = prefix
        .strip_prefix("gs://")
        .ok_or_else(|| format!("{prefix} is not a GCS location"))?;
    Ok(rest.split_once('/').unwrap_or((rest, "")))
}

/// A CEL string literal.
fn cel(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The Credential Access Boundary narrowing a token to `path` in `bucket`, and to reading or to reading and writing.
pub fn boundary(bucket: &str, path: &str, access: Access) -> Value {
    let resource = format!("//storage.googleapis.com/projects/_/buckets/{bucket}");
    let objects = format!(
        "resource.name.startsWith({})",
        cel(&format!("projects/_/buckets/{bucket}/objects/{path}"))
    );
    let listing = format!(
        "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith({})",
        cel(path)
    );
    let mut rules = vec![json!({
        "availableResource": resource,
        "availablePermissions": ["inRole:roles/storage.legacyObjectReader", "inRole:roles/storage.objectViewer"],
        "availabilityCondition": {"title": "metalake-read", "expression": format!("{objects} || {listing}")}
    })];
    if access == Access::ReadWrite {
        rules.push(json!({
            "availableResource": resource,
            "availablePermissions": ["inRole:roles/storage.legacyBucketWriter"],
            "availabilityCondition": {"title": "metalake-write", "expression": objects}
        }));
    }
    json!({"accessBoundary": {"accessBoundaryRules": rules}})
}

#[cfg(test)]
pub(crate) mod tests {
    use ring::signature::{RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};

    use super::*;

    /// A key made for these tests alone, which nothing trusts.
    pub(crate) const TEST_KEY: &str = include_str!("gcs_test_key.pem");

    #[test]
    fn boundary_narrowed() {
        let read = boundary("bucket", "lake/db/t/", Access::Read);
        let rules = read["accessBoundary"]["accessBoundaryRules"].as_array().unwrap();
        assert_eq!(rules.len(), 1, "reading alone");
        assert_eq!(
            rules[0]["availableResource"],
            "//storage.googleapis.com/projects/_/buckets/bucket"
        );
        assert_eq!(
            rules[0]["availabilityCondition"]["expression"],
            "resource.name.startsWith('projects/_/buckets/bucket/objects/lake/db/t/') || \
             api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('lake/db/t/')"
        );
        let write = boundary("bucket", "lake/db/t/", Access::ReadWrite);
        let rules = write["accessBoundary"]["accessBoundaryRules"].as_array().unwrap();
        assert_eq!(
            rules[1]["availablePermissions"][0],
            "inRole:roles/storage.legacyBucketWriter"
        );
        assert_eq!(cel("it's"), "'it\\'s'");
    }

    #[test]
    fn jwt_signed_by_service_account() {
        let account = ServiceAccount {
            client_email: "lake@project.iam.gserviceaccount.com".into(),
            private_key: TEST_KEY.into(),
            private_key_id: "key-1".into(),
            token_uri: default_token_uri(),
        };
        let token = jwt(&account, 1_794_412_800).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["iss"], "lake@project.iam.gserviceaccount.com");
        assert_eq!(claims["aud"], "https://oauth2.googleapis.com/token");
        assert_eq!(claims["exp"], 1_794_412_800 + LASTING_S);
        // Its signature is the key's.
        let key = RsaKeyPair::from_pkcs8(&pem_der(TEST_KEY).unwrap()).unwrap();
        let public = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key.public().as_ref());
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        assert!(
            public
                .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
                .is_ok()
        );
    }

    #[test]
    fn bucket_and_path_of_prefix() {
        assert_eq!(
            bucket_and_path("gs://bucket/lake/db/t/").unwrap(),
            ("bucket", "lake/db/t/")
        );
        assert!(bucket_and_path("s3://bucket/x").is_err());
    }
}
