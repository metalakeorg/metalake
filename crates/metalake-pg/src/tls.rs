//! TLS to the catalog's Postgres, as libpq names it. The driver knows `disable`, `prefer` and `require`
//! alone, and no `sslrootcert`: a connection string a cloud gives, `sslmode=verify-full&sslrootcert=…`, does not even
//! parse there. So the two are taken out of the string here, the driver is given the mode it knows, and how the server
//! is checked is rustls's: not at all for `prefer` and `require`, as libpq does; its chain for `verify-ca`; its chain
//! and its name for `verify-full`. The roots are the file `sslrootcert` names, or the system's when it names none or
//! names `system`.

use std::path::PathBuf;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::{Error, ErrorKind, Result};

/// How much of the server is checked: libpq's `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SslMode {
    /// No TLS.
    Disable,
    /// TLS when the server offers it, checking nothing: libpq's default.
    Prefer,
    /// TLS, checking nothing.
    Require,
    /// TLS, the server's chain checked against the roots.
    VerifyCa,
    /// TLS, the server's chain and its name checked.
    VerifyFull,
}

impl SslMode {
    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "disable" => Self::Disable,
            "prefer" => Self::Prefer,
            "require" => Self::Require,
            "verify-ca" => Self::VerifyCa,
            "verify-full" => Self::VerifyFull,
            other => {
                return Err(Error::new(
                    ErrorKind::Postgres,
                    format!("sslmode `{other}` is none of disable, prefer, require, verify-ca and verify-full"),
                ));
            }
        })
    }

    /// The mode as libpq names it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }

    /// The mode as the driver knows it.
    fn driver(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require | Self::VerifyCa | Self::VerifyFull => "require",
        }
    }
}

/// Where the roots that check the server come from: libpq's `sslrootcert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Roots {
    /// The system's, when it names none or names `system`.
    System,
    /// A PEM file of certificates.
    File(PathBuf),
}

/// A connection string's TLS, taken out of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tls {
    /// What it asked for; `prefer` when it asked for nothing.
    pub mode: SslMode,
    /// Whether it named the mode, as a rule may hold a mode it did not name to a stricter one.
    pub named: bool,
    pub roots: Roots,
}

/// A connection string split into what the driver reads and its TLS. The string is libpq's `key=value` pairs or a URL,
/// in which the two keys are query parameters.
pub fn split(postgres: &str) -> Result<(String, Tls)> {
    let (rest, mut mode, roots) = if is_url(postgres) {
        split_url(postgres)?
    } else {
        split_pairs(postgres)?
    };
    let named = mode.is_some();
    let mode = match mode.take() {
        Some(value) => SslMode::parse(&value)?,
        None => SslMode::Prefer,
    };
    let roots = match roots.as_deref() {
        None | Some("system") => Roots::System,
        Some(path) => Roots::File(PathBuf::from(path)),
    };
    Ok((rest, Tls { mode, named, roots }))
}

impl Tls {
    /// The connection string `rest` was split from, as libpq reads it: `rest` with the mode, if it was named, and the
    /// roots, if they are a file. What [`split`] takes out, this puts back.
    pub fn written(&self, rest: &str) -> String {
        let mut pairs = Vec::new();
        if self.named {
            pairs.push(("sslmode", self.mode.name().to_string()));
        }
        if let Roots::File(path) = &self.roots {
            pairs.push(("sslrootcert", path.to_string_lossy().into_owned()));
        }
        let mut out = rest.to_string();
        for (key, value) in pairs {
            out = if is_url(&out) {
                let joiner = if out.contains('?') { '&' } else { '?' };
                format!("{out}{joiner}{key}={}", encode(&value))
            } else if out.trim().is_empty() {
                format!("{key}={}", quote(&value))
            } else {
                format!("{out} {key}={}", quote(&value))
            };
        }
        out
    }

    /// The string the driver is given: `rest` with the mode it knows.
    pub fn driver_string(&self, rest: &str) -> String {
        let mode = self.mode.driver();
        if is_url(rest) {
            let joiner = if rest.contains('?') { '&' } else { '?' };
            format!("{rest}{joiner}sslmode={mode}")
        } else if rest.trim().is_empty() {
            format!("sslmode={mode}")
        } else {
            format!("{rest} sslmode={mode}")
        }
    }

    /// The connector that checks the server as the mode says.
    pub fn connector(&self) -> Result<MakeRustlsConnect> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::new(ErrorKind::Postgres, format!("TLS: {e}")))?;
        let config = match self.mode {
            SslMode::Disable | SslMode::Prefer | SslMode::Require => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Unchecked(provider)))
                .with_no_client_auth(),
            SslMode::VerifyCa => {
                let chain = WebPkiServerVerifier::builder_with_provider(Arc::new(self.roots()?), provider)
                    .build()
                    .map_err(|e| Error::new(ErrorKind::Postgres, format!("TLS: {e}")))?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(ChainOnly(chain)))
                    .with_no_client_auth()
            }
            SslMode::VerifyFull => builder.with_root_certificates(self.roots()?).with_no_client_auth(),
        };
        Ok(MakeRustlsConnect::new(config))
    }

    fn roots(&self) -> Result<RootCertStore> {
        let mut store = RootCertStore::empty();
        match &self.roots {
            Roots::System => {
                let found = rustls_native_certs::load_native_certs();
                store.add_parsable_certificates(found.certs);
                if store.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Postgres,
                        "TLS: the system has no root certificates to check the server by: name a file as sslrootcert",
                    ));
                }
            }
            Roots::File(path) => {
                let unreadable = |e: &dyn std::fmt::Display| {
                    Error::new(ErrorKind::Postgres, format!("TLS: sslrootcert {}: {e}", path.display()))
                };
                for cert in CertificateDer::pem_file_iter(path).map_err(|e| unreadable(&e))? {
                    store
                        .add(cert.map_err(|e| unreadable(&e))?)
                        .map_err(|e| unreadable(&e))?;
                }
                if store.is_empty() {
                    return Err(unreadable(&"it holds no certificate"));
                }
            }
        }
        Ok(store)
    }
}

/// Whether a connection string is a URL, as opposed to libpq's `key=value` pairs.
fn is_url(postgres: &str) -> bool {
    postgres.starts_with("postgres://") || postgres.starts_with("postgresql://")
}

type Parts = (String, Option<String>, Option<String>);

/// A URL without `sslmode` and `sslrootcert`, and their values.
fn split_url(url: &str) -> Result<Parts> {
    let Some((base, query)) = url.split_once('?') else {
        return Ok((url.to_string(), None, None));
    };
    let (mut mode, mut roots, mut kept) = (None, None, Vec::new());
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "sslmode" => mode = Some(decode(value)?),
            "sslrootcert" => roots = Some(decode(value)?),
            _ => kept.push(pair),
        }
    }
    let rest = if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok((rest, mode, roots))
}

/// A URL's query value, percent-encoded but for what needs no encoding, `/` among it, as a path keeps it readable.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => out.push(byte as char),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// A `key=value` pair's value, quoted as libpq reads it: within `'`, a `\` or a `'` escaped by a `\`.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Percent-decoding of a URL's query value.
fn decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = value.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok());
                let byte = hex.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Postgres,
                        format!("the connection URL's `{value}` is not percent-encoded"),
                    )
                })?;
                out.push(byte);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| Error::new(ErrorKind::Postgres, "the connection URL is not UTF-8"))
}

/// libpq's `key=value` pairs without `sslmode` and `sslrootcert`, and their values. Each kept value is written again
/// quoted, which the driver reads as libpq does.
fn split_pairs(pairs: &str) -> Result<Parts> {
    let (mut mode, mut roots, mut kept) = (None, None, Vec::new());
    for (key, value) in parse_pairs(pairs)? {
        match key.as_str() {
            "sslmode" => mode = Some(value),
            "sslrootcert" => roots = Some(value),
            _ => kept.push(format!("{key}={}", quote(&value))),
        }
    }
    Ok((kept.join(" "), mode, roots))
}

/// libpq's `key = value` pairs: a value is quoted with `'`, in which `\` escapes the next character, or runs to the
/// next whitespace.
fn parse_pairs(text: &str) -> Result<Vec<(String, String)>> {
    let malformed = || {
        Error::new(
            ErrorKind::Postgres,
            "the connection string is not libpq's `key=value` pairs",
        )
    };
    let mut chars = text.chars().peekable();
    let mut pairs = Vec::new();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.peek().is_none() {
            return Ok(pairs);
        }
        let mut key = String::new();
        while let Some(c) = chars.next_if(|c| *c != '=' && !c.is_whitespace()) {
            key.push(c);
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.next() != Some('=') || key.is_empty() {
            return Err(malformed());
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        if chars.next_if_eq(&'\'').is_some() {
            loop {
                match chars.next().ok_or_else(malformed)? {
                    '\'' => break,
                    '\\' => value.push(chars.next().ok_or_else(malformed)?),
                    c => value.push(c),
                }
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                value.push(if c == '\\' {
                    chars.next().ok_or_else(malformed)?
                } else {
                    c
                });
            }
        }
        pairs.push((key, value));
    }
}

/// libpq's `prefer` and `require`: the session is encrypted, and the server is not checked. Its handshake's signatures
/// still are, so that the session is the one its certificate's key holds.
#[derive(Debug)]
struct Unchecked(Arc<CryptoProvider>);

impl ServerCertVerifier for Unchecked {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// libpq's `verify-ca`: the server's chain is checked against the roots, and its name is not, as a Cloud SQL instance
/// reached by its address has none of that address in its certificate.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tls(mode: SslMode, named: bool, roots: Roots) -> Tls {
        Tls { mode, named, roots }
    }

    #[test]
    fn written_puts_back_what_split_took_out() {
        for given in [
            "postgresql://u%40x.com@h/db?sslmode=verify-ca&sslrootcert=C:/lakes/nube/server-ca.pem",
            "postgresql://u:p@h:5432/db?application_name=x&sslmode=verify-full&sslrootcert=%2Fetc%2Frds%20ca.pem",
            "postgresql://u@h/db?sslrootcert=system",
            "postgresql://u@h/db",
            r"host=h port=5432 sslmode=verify-ca sslrootcert='C:\\certs\\a b.pem' password='it\'s'",
            "host=h user=u sslmode=require",
            "sslrootcert=/certs/root.pem",
        ] {
            let (rest, tls) = split(given).unwrap();
            let written = tls.written(&rest);
            assert_eq!(split(&written).unwrap(), (rest, tls), "{given} -> {written}");
        }
        // A mode not named stays not named; the system's roots are its default, which needs no naming.
        let (rest, tls) = split("postgresql://u@h/db?sslrootcert=system").unwrap();
        assert_eq!(tls.written(&rest), "postgresql://u@h/db");
    }

    #[test]
    fn written_with_other_roots() {
        let (rest, mut tls) = split("postgresql://u@h/db?sslmode=verify-ca&sslrootcert=C:/lakes/ca.pem").unwrap();
        tls.roots = Roots::File(PathBuf::from("/pipeline/root.crt"));
        assert_eq!(
            tls.written(&rest),
            "postgresql://u@h/db?sslmode=verify-ca&sslrootcert=/pipeline/root.crt"
        );
        let (rest, mut tls) = split("host=h sslmode=verify-full sslrootcert=/etc/a.pem").unwrap();
        tls.roots = Roots::File(PathBuf::from("/home/me/.metalake/pipelines/x/root.crt"));
        assert_eq!(
            tls.written(&rest),
            "host='h' sslmode='verify-full' sslrootcert='/home/me/.metalake/pipelines/x/root.crt'"
        );
    }

    #[test]
    fn url_keys_taken_out() {
        let (rest, found) =
            split("postgresql://u@h:5432/db?application_name=x&sslmode=verify-full&sslrootcert=%2Fetc%2Frds.pem")
                .unwrap();
        assert_eq!(rest, "postgresql://u@h:5432/db?application_name=x");
        assert_eq!(
            found,
            tls(SslMode::VerifyFull, true, Roots::File("/etc/rds.pem".into()))
        );
        assert_eq!(
            found.driver_string(&rest),
            "postgresql://u@h:5432/db?application_name=x&sslmode=require"
        );
    }

    #[test]
    fn url_without_keys() {
        let (rest, found) = split("postgres://u@h/db").unwrap();
        assert_eq!(rest, "postgres://u@h/db");
        assert_eq!(found, tls(SslMode::Prefer, false, Roots::System));
        assert_eq!(found.driver_string(&rest), "postgres://u@h/db?sslmode=prefer");
        let (rest, found) = split("postgres://u@h/db?sslmode=require&sslrootcert=system").unwrap();
        assert_eq!(rest, "postgres://u@h/db");
        assert_eq!(found, tls(SslMode::Require, true, Roots::System));
    }

    #[test]
    fn pairs_keys_taken_out() {
        let (rest, found) =
            split("host=h  port = 5432 sslmode=verify-ca sslrootcert='C:\\\\certs\\\\a b.pem' password='it''s'")
                .unwrap_err_or_ok();
        // A quote is escaped by a backslash, not doubled: the last pair above is malformed.
        assert!(found.is_none() && rest.contains("not libpq"), "{rest}");

        let (rest, found) =
            split("host=h  port = 5432 sslmode=verify-ca sslrootcert='C:\\\\certs\\\\a b.pem' password='it\\'s'")
                .unwrap();
        assert_eq!(rest, "host='h' port='5432' password='it\\'s'");
        assert_eq!(
            found,
            tls(SslMode::VerifyCa, true, Roots::File("C:\\certs\\a b.pem".into()))
        );
        assert_eq!(
            found.driver_string(&rest),
            "host='h' port='5432' password='it\\'s' sslmode=require"
        );
        let config: tokio_postgres::Config = found.driver_string(&rest).parse().unwrap();
        assert_eq!(config.get_password(), Some(&b"it's"[..]));
    }

    #[test]
    fn modes() {
        for (given, mode, driver) in [
            ("disable", SslMode::Disable, "disable"),
            ("prefer", SslMode::Prefer, "prefer"),
            ("require", SslMode::Require, "require"),
            ("verify-ca", SslMode::VerifyCa, "require"),
            ("verify-full", SslMode::VerifyFull, "require"),
        ] {
            let (_, found) = split(&format!("host=h sslmode={given}")).unwrap();
            assert_eq!((found.mode, found.mode.driver()), (mode, driver));
        }
        let error = split("host=h sslmode=allow").unwrap_err();
        assert!(error.to_string().contains("sslmode `allow`"), "{error}");
    }

    #[test]
    fn connectors_built() {
        for mode in [SslMode::Disable, SslMode::Prefer, SslMode::Require] {
            tls(mode, true, Roots::File("/nowhere".into()))
                .connector()
                .expect("no roots are read");
        }
        let missing = tls(SslMode::VerifyFull, true, Roots::File("/nowhere/ca.pem".into())).connector();
        assert!(
            missing
                .err()
                .unwrap()
                .to_string()
                .contains("sslrootcert /nowhere/ca.pem")
        );
        let dir = std::env::temp_dir().join(format!("metalake-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let error = tls(SslMode::VerifyCa, true, Roots::File(empty))
            .connector()
            .err()
            .unwrap();
        assert!(error.to_string().contains("holds no certificate"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    trait UnwrapErrOrOk {
        fn unwrap_err_or_ok(self) -> (String, Option<Tls>);
    }

    impl UnwrapErrOrOk for Result<(String, Tls)> {
        fn unwrap_err_or_ok(self) -> (String, Option<Tls>) {
            match self {
                Ok((rest, tls)) => (rest, Some(tls)),
                Err(e) => (e.to_string(), None),
            }
        }
    }
}
