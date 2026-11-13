//! RDS and Aurora's token: a URL to `connect` as the database user, presigned with AWS Signature Version 4 for the
//! service `rds-db`, without its scheme, signed here and sent nowhere. It lasts fifteen minutes. The client's AWS
//! identity is the one its store reads (`object_store`'s chain: the environment, a web identity, a container's or an
//! instance's role). <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.html>

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use hmac::{Hmac, KeyInit, Mac};
use metalake_pg::{Credential, Password, Target};
use object_store::aws::AmazonS3Builder;
use sha2::{Digest, Sha256};

use super::error;

/// How long a token lasts: RDS takes no longer.
const LASTING: Duration = Duration::from_secs(900);
/// What signs it, and the region it is for.
const SERVICE: &str = "rds-db";

#[derive(Debug)]
pub(crate) struct Aws {
    target: Target,
    region: String,
}

impl Aws {
    /// For the database `target` names, in the region its host says, as RDS names hosts
    /// (`<name>.<id>.<region>.rds.amazonaws.com`), or else the environment's.
    pub(crate) fn new(target: Target) -> metalake_pg::Result<Self> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let region = region_of(&target.host)
            .or_else(|| var("AWS_REGION"))
            .or_else(|| var("AWS_DEFAULT_REGION"))
            .ok_or_else(|| {
                error(format!(
                    "aws-iam: {} is not an RDS host that names its region: set AWS_REGION",
                    target.host
                ))
            })?;
        Ok(Self { target, region })
    }
}

impl Credential for Aws {
    fn password(&self) -> BoxFuture<'_, metalake_pg::Result<Password>> {
        Box::pin(async move {
            let store = AmazonS3Builder::from_env()
                .with_bucket_name("metalake-catalog")
                .with_region(&self.region)
                .build()
                .map_err(|e| error(format!("aws-iam: the client's AWS identity: {e}")))?;
            let own = store
                .credentials()
                .get_credential()
                .await
                .map_err(|e| error(format!("aws-iam: the client's AWS identity: {e}")))?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let key = Key {
                id: &own.key_id,
                secret: &own.secret_key,
                token: own.token.as_deref(),
            };
            let value = token(&self.target, &self.region, &key, now);
            Ok(Password {
                value,
                expires: Some(Instant::now() + LASTING),
            })
        })
    }
}

/// The region of an RDS host: the label before `rds`.
fn region_of(host: &str) -> Option<String> {
    let labels: Vec<&str> = host.split('.').collect();
    let at = labels.iter().position(|l| *l == "rds")?;
    (at >= 1 && labels.get(at + 1) == Some(&"amazonaws")).then(|| labels[at - 1].to_string())
}

struct Key<'a> {
    id: &'a str,
    secret: &'a str,
    token: Option<&'a str>,
}

/// The token for `target`, signed at `now`, in seconds since the epoch.
fn token(target: &Target, region: &str, key: &Key<'_>, now: u64) -> String {
    let host = format!("{}:{}", target.host, target.port);
    let params = vec![
        ("Action".to_string(), "connect".to_string()),
        ("DBUser".to_string(), target.user.clone()),
    ];
    let empty = hex(&Sha256::digest(b""));
    let query = presign(key, region, SERVICE, &host, "/", params, now, LASTING.as_secs(), &empty);
    format!("{host}/?{query}")
}

/// A request presigned in its query (SigV4): its query string, the signature last. `payload` is the body's hash, as
/// the service wants it. <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html>
#[allow(clippy::too_many_arguments)]
fn presign(
    key: &Key<'_>,
    region: &str,
    service: &str,
    host: &str,
    path: &str,
    mut params: Vec<(String, String)>,
    now: u64,
    expires: u64,
    payload: &str,
) -> String {
    let at = stamp(now);
    let day = &at[..8];
    let scope = format!("{day}/{region}/{service}/aws4_request");
    params.push(("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()));
    params.push(("X-Amz-Credential".into(), format!("{}/{scope}", key.id)));
    params.push(("X-Amz-Date".into(), at.clone()));
    params.push(("X-Amz-Expires".into(), expires.to_string()));
    if let Some(token) = key.token {
        params.push(("X-Amz-Security-Token".into(), token.into()));
    }
    params.push(("X-Amz-SignedHeaders".into(), "host".into()));
    let mut encoded: Vec<(String, String)> = params.iter().map(|(k, v)| (encode(k), encode(v))).collect();
    encoded.sort();
    let query = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!("GET\n{path}\n{query}\nhost:{host}\n\nhost\n{payload}");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{at}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let signing = [day, region, service, "aws4_request"]
        .iter()
        .fold(format!("AWS4{}", key.secret).into_bytes(), |k, part| {
            hmac(&k, part.as_bytes())
        });
    format!("{query}&X-Amz-Signature={}", hex(&hmac(&signing, to_sign.as_bytes())))
}

/// RFC 3986's encoding, as SigV4 wants it: every byte but the unreserved.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// `YYYYMMDDTHHMMSSZ` of seconds since the epoch.
fn stamp(seconds: u64) -> String {
    let (days, rest) = ((seconds / 86_400) as i64, seconds % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2013-05-24T00:00:00Z, the day of AWS's examples.
    const EXAMPLE_DAY: u64 = 1_369_353_600;

    #[test]
    fn stamps() {
        assert_eq!(stamp(EXAMPLE_DAY), "20130524T000000Z");
        assert_eq!(stamp(0), "19700101T000000Z");
        assert_eq!(stamp(951_782_400 + 3661), "20000229T010101Z");
    }

    /// AWS's own example of a presigned URL, an S3 GET of `test.txt`, and the signature it gives.
    #[test]
    fn presign_aws_example() {
        let key = Key {
            id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            token: None,
        };
        let query = presign(
            &key,
            "us-east-1",
            "s3",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            vec![],
            EXAMPLE_DAY,
            86_400,
            "UNSIGNED-PAYLOAD",
        );
        assert!(
            query.ends_with("X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"),
            "{query}"
        );
        assert!(query.contains("X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"));
    }

    #[test]
    fn rds_token() {
        let target = Target {
            host: "db.abc123.eu-west-1.rds.amazonaws.com".into(),
            port: 5432,
            user: "spark_etl".into(),
        };
        let key = Key {
            id: "AKID",
            secret: "secret",
            token: Some("session/token"),
        };
        let token = token(&target, "eu-west-1", &key, EXAMPLE_DAY);
        assert!(
            token.starts_with("db.abc123.eu-west-1.rds.amazonaws.com:5432/?Action=connect&DBUser=spark_etl&"),
            "{token}"
        );
        assert!(token.contains("X-Amz-Credential=AKID%2F20130524%2Feu-west-1%2Frds-db%2Faws4_request"));
        assert!(token.contains("X-Amz-Expires=900&X-Amz-Security-Token=session%2Ftoken&X-Amz-SignedHeaders=host"));
        assert!(!token.starts_with("https://"));
    }

    #[test]
    fn regions() {
        assert_eq!(
            region_of("db.abc123.eu-west-1.rds.amazonaws.com").as_deref(),
            Some("eu-west-1")
        );
        assert_eq!(
            region_of("p.proxy-abc.us-east-2.rds.amazonaws.com").as_deref(),
            Some("us-east-2")
        );
        assert_eq!(region_of("db.example.com"), None);
        assert_eq!(region_of("rds.amazonaws.com"), None);
    }
}
