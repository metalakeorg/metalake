//! Cloud SQL's token: an OAuth2 access token of the client's Google identity, with the scope Cloud SQL logs in by,
//! sent as the password over TLS. The identity, as Google's application default credentials find it: a service
//! account's key, whose JWT the client signs (RFC 7523); a user's credentials from `gcloud auth application-default
//! login`, refreshed; or the metadata server of the machine it runs on.
//! <https://cloud.google.com/sql/docs/postgres/iam-logins>

use std::fmt;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::future::BoxFuture;
use metalake_pg::{Credential, Password};
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::Deserialize;

use super::{error, http};

const SCOPE: &str = "https://www.googleapis.com/auth/sqlservice.login";
const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const METADATA: &str = "http://metadata.google.internal";

/// The client's Google identity, as its credentials file has it.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Identity {
    ServiceAccount {
        client_email: String,
        private_key: String,
        #[serde(default)]
        private_key_id: String,
        #[serde(default = "token_uri")]
        token_uri: String,
    },
    AuthorizedUser {
        client_id: String,
        client_secret: String,
        refresh_token: String,
        #[serde(default = "token_uri")]
        token_uri: String,
    },
}

fn token_uri() -> String {
    TOKEN_URI.into()
}

/// Who the identity is, never its key or its tokens: a catalog's handle is printed.
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceAccount { client_email, .. } => f
                .debug_struct("ServiceAccount")
                .field("client_email", client_email)
                .finish_non_exhaustive(),
            Self::AuthorizedUser { client_id, .. } => f
                .debug_struct("AuthorizedUser")
                .field("client_id", client_id)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Gcp {
    /// The identity of a credentials file, if one is found: `Err` says why one named could not be read.
    identity: Result<Option<Identity>, String>,
    /// The metadata server, `GCE_METADATA_HOST`'s when set.
    metadata: String,
}

impl Gcp {
    /// As the environment says: `GOOGLE_SERVICE_ACCOUNT_KEY`, the file `GOOGLE_SERVICE_ACCOUNT` or
    /// `GOOGLE_APPLICATION_CREDENTIALS` names, or gcloud's application default credentials, else the metadata server.
    pub(crate) fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let named = var("GOOGLE_SERVICE_ACCOUNT").or_else(|| var("GOOGLE_APPLICATION_CREDENTIALS"));
        let identity = match (var("GOOGLE_SERVICE_ACCOUNT_KEY"), named) {
            (Some(json), _) => read(&json, "GOOGLE_SERVICE_ACCOUNT_KEY").map(Some),
            (None, Some(path)) => std::fs::read_to_string(&path)
                .map_err(|e| format!("{path}: {e}"))
                .and_then(|json| read(&json, &path))
                .map(Some),
            (None, None) => Ok(gcloud_default().and_then(|json| read(&json, "gcloud's default").ok())),
        };
        let metadata = var("GCE_METADATA_HOST").map_or_else(|| METADATA.to_string(), |host| format!("http://{host}"));
        Self { identity, metadata }
    }

    async fn mint(&self) -> metalake_pg::Result<Password> {
        let client = http::client();
        match self.identity.clone().map_err(|e| error(format!("gcp-iam: {e}")))? {
            Some(Identity::ServiceAccount {
                client_email,
                private_key,
                private_key_id,
                token_uri,
            }) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let assertion = jwt(&client_email, &private_key, &private_key_id, &token_uri, now)?;
                let form = [
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", assertion.as_str()),
                ];
                let body = http::json(client.post(&token_uri).form(&form), "gcp-iam: Google's token service").await?;
                http::token(&body, "gcp-iam: Google's token service")
            }
            Some(Identity::AuthorizedUser {
                client_id,
                client_secret,
                refresh_token,
                token_uri,
            }) => {
                let form = [
                    ("grant_type", "refresh_token"),
                    ("client_id", client_id.as_str()),
                    ("client_secret", client_secret.as_str()),
                    ("refresh_token", refresh_token.as_str()),
                ];
                let body = http::json(client.post(&token_uri).form(&form), "gcp-iam: Google's token service").await?;
                http::token(&body, "gcp-iam: Google's token service")
            }
            None => {
                let url = format!(
                    "{}/computeMetadata/v1/instance/service-accounts/default/token",
                    self.metadata
                );
                let request = client
                    .get(&url)
                    .query(&[("scopes", SCOPE)])
                    .header("Metadata-Flavor", "Google");
                let what = "gcp-iam: no credentials file, and the metadata server";
                http::token(&http::json(request, what).await?, what)
            }
        }
    }
}

impl Credential for Gcp {
    fn password(&self) -> BoxFuture<'_, metalake_pg::Result<Password>> {
        Box::pin(self.mint())
    }
}

fn read(json: &str, from: &str) -> Result<Identity, String> {
    serde_json::from_str(json)
        .map_err(|e| format!("{from} is not a service account's key or a user's credentials: {e}"))
}

/// gcloud's application default credentials, where it writes them.
fn gcloud_default() -> Option<String> {
    std::fs::read_to_string(default_file()?).ok()
}

/// The file of gcloud's application default credentials, if this machine has one.
pub(super) fn default_file() -> Option<PathBuf> {
    let file = gcloud_dir(|name| std::env::var_os(name))?.join("application_default_credentials.json");
    file.is_file().then_some(file)
}

/// gcloud's configuration directory: `CLOUDSDK_CONFIG`, which names it, or `gcloud` in the system's place for it.
fn gcloud_dir(var: impl Fn(&str) -> Option<std::ffi::OsString>) -> Option<PathBuf> {
    if let Some(dir) = var("CLOUDSDK_CONFIG").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(dir));
    }
    let base = if cfg!(windows) {
        PathBuf::from(var("APPDATA")?)
    } else {
        PathBuf::from(var("HOME")?).join(".config")
    };
    Some(base.join("gcloud"))
}

/// A service account's JWT for its token service, for an hour from `now`.
fn jwt(email: &str, key_pem: &str, key_id: &str, audience: &str, now: u64) -> metalake_pg::Result<String> {
    let header = serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": key_id});
    let claims = serde_json::json!({
        "iss": email, "scope": SCOPE, "aud": audience, "iat": now, "exp": now + 3600,
    });
    let signed = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let der = pem_der(key_pem)?;
    let pair = RsaKeyPair::from_pkcs8(&der).map_err(|e| error(format!("gcp-iam: the service account's key: {e}")))?;
    let mut signature = vec![0; pair.public().modulus_len()];
    pair.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        signed.as_bytes(),
        &mut signature,
    )
    .map_err(|_| error("gcp-iam: signing the service account's JWT failed"))?;
    Ok(format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

fn pem_der(pem: &str) -> metalake_pg::Result<Vec<u8>> {
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .map_err(|e| error(format!("gcp-iam: the service account's key is not PEM: {e}")))
}

#[cfg(test)]
mod tests {
    use ring::signature::{RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};

    use super::*;

    const KEY: &str = include_str!("gcp_test_key.pem");

    fn account(token_uri: &str) -> Gcp {
        Gcp {
            identity: Ok(Some(Identity::ServiceAccount {
                client_email: "lake@p.iam.gserviceaccount.com".into(),
                private_key: KEY.into(),
                private_key_id: "k1".into(),
                token_uri: token_uri.into(),
            })),
            metadata: "http://127.0.0.1:9".into(),
        }
    }

    #[test]
    fn jwt_signed_for_cloud_sql() {
        let token = jwt("a@p.iam.gserviceaccount.com", KEY, "k1", TOKEN_URI, 1000).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        let claims: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["scope"], SCOPE);
        assert_eq!(
            (claims["iat"].as_u64(), claims["exp"].as_u64()),
            (Some(1000), Some(4600))
        );
        let pair = RsaKeyPair::from_pkcs8(&pem_der(KEY).unwrap()).unwrap();
        let public = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, pair.public().as_ref());
        let signed = format!("{}.{}", parts[0], parts[1]);
        public
            .verify(signed.as_bytes(), &URL_SAFE_NO_PAD.decode(parts[2]).unwrap())
            .unwrap();
    }

    #[test]
    fn gcloud_dir_named_by_cloudsdk_config() {
        let vars = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| (*v).into())
        };
        let named = gcloud_dir(vars(&[
            ("CLOUDSDK_CONFIG", "/etc/gcloud"),
            ("HOME", "/h"),
            ("APPDATA", "/a"),
        ]));
        assert_eq!(
            named,
            Some(PathBuf::from("/etc/gcloud")),
            "it names the directory itself"
        );
        let default = gcloud_dir(vars(&[("HOME", "/h"), ("APPDATA", "/a")])).unwrap();
        assert!(default.ends_with("gcloud") && default.starts_with(if cfg!(windows) { "/a" } else { "/h/.config" }));
    }

    #[test]
    fn secrets_not_printed() {
        let printed = format!("{:?}", account("http://t"));
        assert!(printed.contains("lake@p.iam.gserviceaccount.com"), "{printed}");
        assert!(
            !printed.contains("PRIVATE KEY") && !printed.contains(&KEY[40..80]),
            "{printed}"
        );
    }

    #[tokio::test]
    async fn service_account_exchanged() {
        let (address, got) = http::tests::answering(200, r#"{"access_token":"ya29.x","expires_in":3599}"#);
        let password = account(&format!("{address}/token")).mint().await.unwrap();
        assert_eq!(password.value, "ya29.x");
        let request = got.recv().unwrap();
        assert!(request.starts_with("POST /token "), "{request}");
        assert!(request.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="));
    }

    #[tokio::test]
    async fn metadata_server_asked_for_the_scope() {
        let (address, got) = http::tests::answering(200, r#"{"access_token":"m","expires_in":"100"}"#);
        let gcp = Gcp {
            identity: Ok(None),
            metadata: address,
        };
        assert_eq!(gcp.mint().await.unwrap().value, "m");
        let request = got.recv().unwrap().to_ascii_lowercase();
        assert!(
            request.contains("service-accounts/default/token?scopes=https%3a%2f%2fwww.googleapis.com"),
            "{request}"
        );
        assert!(request.contains("metadata-flavor: google"));
    }

    #[tokio::test]
    async fn refusal_said() {
        let (address, _got) = http::tests::answering(400, r#"{"error":"invalid_grant","error_description":"bad"}"#);
        let refused = account(&address).mint().await.unwrap_err().to_string();
        assert!(refused.contains("refused: bad"), "{refused}");
    }
}
