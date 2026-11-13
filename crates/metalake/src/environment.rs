//! What a lake takes from the environment, and how: where its catalog is, the catalog's password, and the credentials
//! of its object store. Secrets come from the environment and never from a file, and every process that reaches a lake,
//! a client or a pipeline's worker, reads them by these names and this rule.

use std::ffi::OsString;

use url::Url;

/// A connection string of the catalog's Postgres, as libpq's `key=value` pairs or a URL.
pub const POSTGRES: &str = "METALAKE_POSTGRES";
/// How a client signs in to that catalog, by name (`aws-iam`, `gcp-iam`, `azure-entra`): the password when unset.
pub const POSTGRES_AUTH: &str = "METALAKE_POSTGRES_AUTH";
/// The catalog's schema, when one Postgres database holds more than one lake.
pub const SCHEMA: &str = "METALAKE_SCHEMA";
/// A Ballista scheduler of the lake's (`df://host:port`), where a session's router may send a statement.
pub const SCHEDULER: &str = "METALAKE_SCHEDULER";
/// The catalog's password, which the driver does not read by itself.
pub const PASSWORD: &str = "PGPASSWORD";
/// The catalog's schema, unless one is named.
pub const DEFAULT_SCHEMA: &str = "metalake";
/// The services whose variables `object_store` reads, by the same names in lower case.
pub const STORAGE_PREFIXES: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];

/// Whether a connection string is a URL, as opposed to libpq's `key=value` pairs.
pub fn is_url(postgres: &str) -> bool {
    as_url(postgres).is_some()
}

fn as_url(postgres: &str) -> Option<Url> {
    Url::parse(postgres)
        .ok()
        .filter(|u| matches!(u.scheme(), "postgres" | "postgresql"))
}

/// Whether a connection string carries a password.
pub fn has_password(postgres: &str) -> bool {
    match as_url(postgres) {
        Some(url) => url.password().is_some() || url.query_pairs().any(|(k, _)| k == "password"),
        None => postgres.split_whitespace().any(|pair| pair.starts_with("password=")),
    }
}

/// A connection string with no password, to show or to keep.
pub fn without_password(postgres: &str) -> String {
    match as_url(postgres) {
        Some(mut url) => {
            let kept: Vec<(String, String)> = url
                .query_pairs()
                .into_owned()
                .filter(|(k, _)| k != "password")
                .collect();
            let _ = url.set_password(None);
            url.set_query(None);
            if !kept.is_empty() {
                url.query_pairs_mut().extend_pairs(kept);
            }
            url.to_string()
        }
        None => {
            let kept = postgres
                .split_whitespace()
                .filter(|pair| !pair.starts_with("password="));
            kept.collect::<Vec<_>>().join(" ")
        }
    }
}

/// The connection string with `password`, as `PGPASSWORD` gives it, unless it already has one.
pub fn with_password(postgres: &str, password: Option<&str>) -> String {
    match password {
        Some(password) if !has_password(postgres) => joined(postgres, password),
        _ => postgres.to_string(),
    }
}

fn joined(postgres: &str, password: &str) -> String {
    match as_url(postgres) {
        Some(mut url) => {
            let _ = url.set_password(Some(password));
            url.to_string()
        }
        None => {
            let quoted = password.replace('\\', "\\\\").replace('\'', "\\'");
            format!("{postgres} password='{quoted}'")
        }
    }
}

/// Whether a variable is one of the object store's.
pub fn is_storage_variable(name: &str) -> bool {
    STORAGE_PREFIXES.iter().any(|prefix| name.starts_with(prefix))
}

/// The options of `object_store` among these variables, which it names in lower case.
pub fn storage_options(vars: impl Iterator<Item = (OsString, OsString)>) -> Vec<(String, String)> {
    vars.filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, _)| is_storage_variable(key))
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_kept_with_and_without_password() {
        for given in [
            "postgresql://u:secret@h/db?sslmode=verify-full&sslrootcert=%2Fcerts%2Frds%20ca.pem",
            "host=h user=u password=secret sslmode=verify-full sslrootcert=/certs/rds.pem",
        ] {
            for string in [
                without_password(given),
                with_password(&without_password(given), Some("p")),
            ] {
                let (_, tls) = metalake_pg::tls::split(&string).unwrap();
                assert_eq!(tls.mode, metalake_pg::tls::SslMode::VerifyFull, "{string}");
                assert!(
                    matches!(tls.roots, metalake_pg::tls::Roots::File(ref p) if p.to_string_lossy().starts_with("/certs/rds")),
                    "{string}"
                );
            }
        }
    }

    #[test]
    fn detect_password() {
        for with in [
            "postgresql://u:secret@h/db",
            "postgres://u@h/db?password=secret",
            "host=h password=secret user=u",
        ] {
            assert!(has_password(with), "{with}");
            assert!(!without_password(with).contains("secret"), "{with}");
        }
        for without in [
            "postgresql://u@h/db",
            "host=h user=u",
            "host=h application_name=password",
        ] {
            assert!(!has_password(without), "{without}");
        }
        assert_eq!(without_password("postgres://u:s@h/db?a=1"), "postgres://u@h/db?a=1");
    }

    #[test]
    fn inject_password() {
        assert_eq!(
            with_password("postgresql://u@h/db", Some("a b/c")),
            "postgresql://u:a%20b%2Fc@h/db"
        );
        assert_eq!(with_password("host=h", Some(r"it's\")), r"host=h password='it\'s\\'");
        assert_eq!(
            with_password("host=h password=p", Some("q")),
            "host=h password=p",
            "it keeps its own"
        );
        assert_eq!(with_password("host=h", None), "host=h");
    }

    #[test]
    fn storage_env_options_lower_case() {
        let vars = [
            ("AWS_REGION", "eu"),
            ("GOOGLE_SERVICE_ACCOUNT", "k"),
            ("AZURE_X", "y"),
            ("PATH", "p"),
        ];
        let options = storage_options(vars.into_iter().map(|(k, v)| (k.into(), v.into())));
        assert_eq!(
            options,
            [
                ("aws_region".to_string(), "eu".to_string()),
                ("google_service_account".to_string(), "k".to_string()),
                ("azure_x".to_string(), "y".to_string()),
            ]
        );
        assert!(is_storage_variable("AWS_SECRET_ACCESS_KEY") && !is_storage_variable("PATH"));
        assert!(is_url("postgresql://h/db") && !is_url("host=h"));
    }
}
