//! AWS Signature Version 4, for the requests the server makes to a token service (STS): what is signed is the
//! request's method, path, query, the headers named, and its body's hash, under a key derived from the secret, the day,
//! the region and the service. <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html>

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// What a request is signed with.
pub struct Signer<'a> {
    pub key_id: &'a str,
    pub secret: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

/// A request to sign: its headers lowercase and sorted by name, `host` and `x-amz-date` among them.
pub struct Signed<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
    /// `YYYYMMDDTHHMMSSZ`, as `x-amz-date` says it.
    pub at: &'a str,
}

impl Signer<'_> {
    /// The `Authorization` header of `request`.
    pub fn authorization(&self, request: &Signed<'_>) -> String {
        let day = &request.at[..8];
        let names: Vec<&str> = request.headers.iter().map(|(name, _)| *name).collect();
        let signed_headers = names.join(";");
        let canonical_headers: String = request
            .headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect();
        let canonical = format!(
            "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
            request.method,
            request.path,
            request.query,
            hex(&Sha256::digest(request.body))
        );
        let scope = format!("{day}/{}/{}/aws4_request", self.region, self.service);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
            request.at,
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let key = [day, self.region, self.service, "aws4_request"]
            .iter()
            .fold(format!("AWS4{}", self.secret).into_bytes(), |key, part| {
                hmac(&key, part.as_bytes())
            });
        let signature = hex(&hmac(&key, to_sign.as_bytes()));
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.key_id
        )
    }
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's own example of a signed request, IAM's `ListUsers`, and the signature it gives.
    #[test]
    fn sigv4_aws_example() {
        let signer = Signer {
            key_id: "AKIDEXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "iam",
        };
        let headers = [
            ("content-type", "application/x-www-form-urlencoded; charset=utf-8"),
            ("host", "iam.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ];
        let request = Signed {
            method: "GET",
            path: "/",
            query: "Action=ListUsers&Version=2010-05-08",
            headers: &headers,
            body: b"",
            at: "20150830T123600Z",
        };
        assert_eq!(
            signer.authorization(&request),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }
}
