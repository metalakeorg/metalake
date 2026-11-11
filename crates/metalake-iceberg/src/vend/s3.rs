//! Credentials for a table's files on S3 (REST-11): minted by the store's token service (STS) from the credential the
//! server's own store uses, which `object_store` finds wherever it is (the environment, a profile, an instance's role,
//! a web identity). Narrowed to the table's prefix and access by a session policy of `AssumeRole`, given a role to
//! assume (`METALAKE_S3_ROLE_ARN`) or a store of its own endpoint, whose token service takes the caller's credential as
//! the role, as MinIO's does. With neither, `GetSessionToken`, which takes no policy: unnarrowed, for a writer alone.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use object_store::aws::AmazonS3Builder;
use serde_json::{Value, json};

use super::sigv4::{Signed, Signer};
use super::{Access, Minted, Minter};

/// The variable naming the role whose session a credential is, narrowed by a policy.
pub const ROLE: &str = "METALAKE_S3_ROLE_ARN";
/// How long a credential lasts: an hour, as the field's catalogs mint them.
const LASTING_S: u32 = 3600;
const STS_VERSION: &str = "2011-06-15";

/// Where S3's token service is, and how to sign for it, as the environment says.
#[derive(Debug, Clone)]
pub struct S3 {
    role: Option<String>,
    /// A store's own endpoint, as MinIO's, whose token service is at its root.
    endpoint: Option<String>,
    region: String,
    client: reqwest::Client,
}

impl S3 {
    /// As the environment says, as the server's store reads it.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        Self {
            role: var(ROLE),
            endpoint: var("AWS_ENDPOINT").or_else(|| var("AWS_ENDPOINT_URL")),
            region: var("AWS_REGION")
                .or_else(|| var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|| "us-east-1".into()),
            client: reqwest::Client::new(),
        }
    }

    /// Whether credentials are narrowed: with a role, or by a store's own token service.
    fn narrows(&self) -> bool {
        self.role.is_some() || self.endpoint.is_some()
    }

    fn token_service(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => format!("{}/", endpoint.trim_end_matches('/')),
            None => format!("https://sts.{}.amazonaws.com/", self.region),
        }
    }

    async fn mint_s3(&self, prefix: &str, access: Access) -> Result<Minted, String> {
        let (bucket, key) = bucket_and_key(prefix)?;
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| format!("the server's S3 store: {e}"))?;
        let own = store
            .credentials()
            .get_credential()
            .await
            .map_err(|e| format!("the server's S3 credential: {e}"))?;
        let mut form = vec![
            ("Version", STS_VERSION.to_string()),
            ("DurationSeconds", LASTING_S.to_string()),
        ];
        if self.narrows() {
            form.push(("Action", "AssumeRole".into()));
            form.push(("RoleSessionName", "metalake".into()));
            form.push(("Policy", policy(bucket, key, access).to_string()));
            if let Some(role) = &self.role {
                form.push(("RoleArn", role.clone()));
            }
        } else {
            if access == Access::Read {
                return Err(format!(
                    "a credential narrowed to reading needs a role to assume: set {ROLE}"
                ));
            }
            form.push(("Action", "GetSessionToken".into()));
        }
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form.iter().map(|(k, v)| (*k, v.as_str())))
            .finish();
        let url = url::Url::parse(&self.token_service()).map_err(|e| format!("the token service: {e}"))?;
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_string(),
        };
        let at = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let content_type = "application/x-www-form-urlencoded; charset=utf-8";
        let mut headers = vec![
            ("content-type", content_type),
            ("host", host.as_str()),
            ("x-amz-date", at.as_str()),
        ];
        if let Some(token) = &own.token {
            headers.push(("x-amz-security-token", token.as_str()));
        }
        let signer = Signer {
            key_id: &own.key_id,
            secret: &own.secret_key,
            region: &self.region,
            service: "sts",
        };
        let authorization = signer.authorization(&Signed {
            method: "POST",
            path: url.path(),
            query: "",
            headers: &headers,
            body: body.as_bytes(),
            at: &at,
        });
        let mut request = self
            .client
            .post(url.clone())
            .header("content-type", content_type)
            .header("x-amz-date", &at)
            .header("authorization", authorization)
            .body(body);
        if let Some(token) = &own.token {
            request = request.header("x-amz-security-token", token);
        }
        let answer = request
            .send()
            .await
            .map_err(|e| format!("the token service at {url}: {e}"))?;
        let ok = answer.status().is_success();
        let text = answer
            .text()
            .await
            .map_err(|e| format!("the token service's answer: {e}"))?;
        if !ok {
            return Err(format!("the token service refused: {}", refusal(&text)));
        }
        let issued = issued(&text)?;
        Ok(Minted {
            config: self.config(&issued),
            expires_at_ms: issued.expires_at_ms,
        })
    }

    /// What a client sets for a credential, in every spelling clients read (REST-11).
    fn config(&self, issued: &Issued) -> BTreeMap<String, String> {
        let mut config = BTreeMap::from([
            ("s3.access-key-id".to_string(), issued.key_id.clone()),
            ("s3.secret-access-key".to_string(), issued.secret.clone()),
            ("s3.session-token".to_string(), issued.token.clone()),
            (
                "s3.session-token-expires-at-ms".to_string(),
                issued.expires_at_ms.to_string(),
            ),
            ("client.region".to_string(), self.region.clone()),
            ("s3.region".to_string(), self.region.clone()),
        ]);
        if let Some(endpoint) = &self.endpoint {
            config.insert("s3.endpoint".into(), endpoint.clone());
            config.insert("s3.path-style-access".into(), "true".into());
        }
        config
    }
}

impl Minter for S3 {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
        Box::pin(self.mint_s3(prefix, access))
    }
}

/// The bucket and the key a prefix such as `s3://bucket/lake/db/t/` names.
fn bucket_and_key(prefix: &str) -> Result<(&str, &str), String> {
    let rest = prefix
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| format!("{prefix} is not a store's location"))?;
    Ok(rest.split_once('/').unwrap_or((rest, "")))
}

/// The session policy narrowing a credential to `key` in `bucket`, and to reading or to reading and writing.
pub fn policy(bucket: &str, key: &str, access: Access) -> Value {
    let mut objects = vec!["s3:GetObject", "s3:GetObjectVersion"];
    if access == Access::ReadWrite {
        objects.extend([
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
        ]);
    }
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": objects, "Resource": [format!("arn:aws:s3:::{bucket}/{key}*")]},
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")],
                "Condition": {"StringLike": {"s3:prefix": [format!("{key}*")]}}
            },
            {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [format!("arn:aws:s3:::{bucket}")]}
        ]
    })
}

/// A credential the token service issued.
#[derive(Debug, PartialEq, Eq)]
struct Issued {
    key_id: String,
    secret: String,
    token: String,
    expires_at_ms: i64,
}

/// The text of an element of an XML answer.
fn element<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(xml[start..end].trim())
}

/// The credential in an answer of `AssumeRole` or `GetSessionToken`.
fn issued(xml: &str) -> Result<Issued, String> {
    let field = |name: &str| {
        element(xml, name)
            .map(str::to_string)
            .ok_or_else(|| format!("the token service answered no {name}"))
    };
    let expiration = field("Expiration")?;
    let expires = DateTime::parse_from_rfc3339(&expiration).map_err(|e| format!("its expiration {expiration}: {e}"))?;
    Ok(Issued {
        key_id: field("AccessKeyId")?,
        secret: field("SecretAccessKey")?,
        token: field("SessionToken")?,
        expires_at_ms: expires.timestamp_millis(),
    })
}

/// What a token service says when it refuses.
fn refusal(xml: &str) -> String {
    match (element(xml, "Code"), element(xml, "Message")) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        _ => xml.chars().take(300).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ASSUMED: &str = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
        <AssumeRoleResult><Credentials><AccessKeyId>ASIAEXAMPLE</AccessKeyId>\
        <SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken>\
        <Expiration>2026-11-11T17:00:00Z</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>";

    #[test]
    fn sts_answer_read() {
        assert_eq!(
            issued(ASSUMED).unwrap(),
            Issued {
                key_id: "ASIAEXAMPLE".into(),
                secret: "secret".into(),
                token: "token".into(),
                expires_at_ms: 1_794_416_400_000,
            }
        );
        let refused = "<ErrorResponse><Error><Code>AccessDenied</Code><Message>no</Message></Error></ErrorResponse>";
        assert_eq!(refusal(refused), "AccessDenied: no");
        assert!(issued(refused).unwrap_err().contains("answered no"));
    }

    #[test]
    fn policy_narrowed() {
        let read = policy("bucket", "lake/db/t/", Access::Read);
        let objects = &read["Statement"][0];
        assert_eq!(objects["Resource"][0], "arn:aws:s3:::bucket/lake/db/t/*");
        assert!(
            !objects["Action"].as_array().unwrap().contains(&json!("s3:PutObject")),
            "reading alone"
        );
        assert_eq!(
            read["Statement"][1]["Condition"]["StringLike"]["s3:prefix"][0],
            "lake/db/t/*"
        );
        let write = policy("bucket", "lake/db/t/", Access::ReadWrite);
        let actions = write["Statement"][0]["Action"].as_array().unwrap();
        assert!(actions.contains(&json!("s3:PutObject")) && actions.contains(&json!("s3:DeleteObject")));
    }

    #[test]
    fn bucket_and_key_of_prefix() {
        assert_eq!(
            bucket_and_key("s3://bucket/lake/db/t/").unwrap(),
            ("bucket", "lake/db/t/")
        );
        assert_eq!(bucket_and_key("s3a://bucket/").unwrap(), ("bucket", ""));
        assert!(bucket_and_key("bucket/key").is_err());
    }

    #[test]
    fn config_spellings() {
        let mut s3 = S3 {
            role: None,
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: "eu-west-1".into(),
            client: reqwest::Client::new(),
        };
        let issued = issued(ASSUMED).unwrap();
        let config = s3.config(&issued);
        for key in [
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
            "s3.session-token-expires-at-ms",
            "client.region",
            "s3.region",
            "s3.endpoint",
            "s3.path-style-access",
        ] {
            assert!(config.contains_key(key), "{key}");
        }
        assert!(s3.narrows(), "a store's own endpoint narrows");
        assert_eq!(s3.token_service(), "http://127.0.0.1:9000/");
        s3.endpoint = None;
        assert!(!s3.narrows(), "AWS with no role does not");
        assert_eq!(s3.token_service(), "https://sts.eu-west-1.amazonaws.com/");
        assert!(!s3.config(&issued).contains_key("s3.endpoint"));
    }
}
