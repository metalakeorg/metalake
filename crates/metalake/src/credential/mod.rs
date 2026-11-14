//! How a lake's client signs in to its catalog's Postgres: by the password the connection string or `PGPASSWORD`
//! gives, or by a cloud's token for the client's own identity, minted for each new connection. In each cloud the
//! identity is the user Postgres knows: an IAM role or user on AWS, a service account or user on Google Cloud, an Entra
//! principal on Azure. The token is what a cloud's managed Postgres takes as that user's password.

use std::sync::Arc;

use metalake_pg::tls::SslMode;
use metalake_pg::{Credential, ErrorKind};

#[cfg(feature = "s3")]
mod aws;
#[cfg(feature = "azure")]
mod azure;
#[cfg(feature = "gcs")]
mod gcp;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
mod http;

/// How a client signs in to its catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Auth {
    /// The connection string's password, or `PGPASSWORD`'s.
    #[default]
    Password,
    /// A token of RDS and Aurora, signed by the client's AWS identity.
    AwsIam,
    /// An access token of the client's Google identity, as Cloud SQL takes it.
    GcpIam,
    /// An access token of the client's Entra identity, as Azure Database for PostgreSQL takes it.
    AzureEntra,
}

impl Auth {
    /// Every name, as a lake's configuration and the faces spell it.
    pub const NAMES: [&str; 4] = ["password", "aws-iam", "gcp-iam", "azure-entra"];

    pub fn parse(name: &str) -> crate::Result<Self> {
        Ok(match name {
            "password" => Self::Password,
            "aws-iam" => Self::AwsIam,
            "gcp-iam" => Self::GcpIam,
            "azure-entra" => Self::AzureEntra,
            other => {
                return Err(error(format!("auth `{other}` is none of {}", Self::NAMES.join(", "))).into());
            }
        })
    }

    /// The auth `name` names; a password when it names none, as an unset or empty variable does.
    pub fn named(name: Option<&str>) -> crate::Result<Self> {
        match name.map(str::trim).filter(|n| !n.is_empty()) {
            Some(name) => Self::parse(name),
            None => Ok(Self::Password),
        }
    }

    /// The auth `METALAKE_POSTGRES_AUTH` names, as a process a client started finds it.
    pub fn from_env() -> crate::Result<Self> {
        Self::named(std::env::var(crate::environment::POSTGRES_AUTH).ok().as_deref())
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::AwsIam => "aws-iam",
            Self::GcpIam => "gcp-iam",
            Self::AzureEntra => "azure-entra",
        }
    }

    /// The credential a pool to `postgres` connects with; none for a password.
    #[cfg_attr(not(feature = "s3"), allow(unused_variables))]
    pub fn credential(self, postgres: &str) -> crate::Result<Option<Arc<dyn Credential>>> {
        #[allow(unused_variables)]
        let unbuilt = |cloud: &str| {
            error(format!(
                "auth `{}` needs this build's {cloud} support, which it was built without",
                self.name()
            ))
        };
        match self {
            Self::Password => Ok(None),
            #[cfg(feature = "s3")]
            Self::AwsIam => Ok(Some(Arc::new(aws::Aws::new(metalake_pg::Target::of(postgres)?)?))),
            #[cfg(not(feature = "s3"))]
            Self::AwsIam => Err(unbuilt("S3").into()),
            #[cfg(feature = "gcs")]
            Self::GcpIam => Ok(Some(Arc::new(gcp::Gcp::from_env()))),
            #[cfg(not(feature = "gcs"))]
            Self::GcpIam => Err(unbuilt("GCS").into()),
            #[cfg(feature = "azure")]
            Self::AzureEntra => Ok(Some(Arc::new(azure::Azure::from_env()))),
            #[cfg(not(feature = "azure"))]
            Self::AzureEntra => Err(unbuilt("Azure").into()),
        }
    }

    /// The connection string a pool connects by, and its credential, as the rules of a cloud's token hold them. A token
    /// is a bearer's: whoever reads it signs in, for minutes. So it goes only to a server whose certificate is checked,
    /// `verify-full` when the string names no mode and no less than `verify-ca` when it names one, and it is the only
    /// password: a string that names one is refused rather than either being chosen.
    pub fn prepared(self, postgres: &str) -> crate::Result<(String, Option<Arc<dyn Credential>>)> {
        if self == Self::Password {
            return Ok((postgres.to_string(), None));
        }
        let name = self.name();
        if crate::environment::has_password(postgres) {
            return Err(error(format!(
                "auth `{name}` signs in with a token: the connection string names a password"
            ))
            .into());
        }
        let (_, tls) = metalake_pg::tls::split(postgres)?;
        let postgres = match (tls.named, tls.mode) {
            (false, _) => with_mode(postgres, "verify-full"),
            (true, mode) if mode >= SslMode::VerifyCa => postgres.to_string(),
            (true, _) => {
                return Err(error(format!(
                    "auth `{name}` sends a token, which goes only to a server whose certificate is checked: \
                     sslmode verify-full or verify-ca, with sslrootcert naming the server's CA (RDS's bundle, \
                     a Cloud SQL instance's) unless the system's roots hold it"
                ))
                .into());
            }
        };
        let credential = self.credential(&postgres)?;
        Ok((postgres, credential))
    }
}

/// `postgres` with `sslmode=mode` added.
fn with_mode(postgres: &str, mode: &str) -> String {
    if crate::environment::is_url(postgres) {
        let joiner = if postgres.contains('?') { '&' } else { '?' };
        format!("{postgres}{joiner}sslmode={mode}")
    } else {
        format!("{postgres} sslmode={mode}")
    }
}

pub(crate) fn error(message: impl Into<String>) -> metalake_pg::Error {
    metalake_pg::Error::new(ErrorKind::Postgres, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        for name in Auth::NAMES {
            assert_eq!(Auth::parse(name).unwrap().name(), name);
        }
        let refused = Auth::parse("kerberos").unwrap_err().to_string();
        assert!(
            refused.contains("auth `kerberos` is none of password, aws-iam"),
            "{refused}"
        );
        assert!(Auth::Password.credential("host=h user=u").unwrap().is_none());
    }

    #[test]
    fn named_defaults_to_password() {
        assert_eq!(Auth::named(None).unwrap(), Auth::Password);
        assert_eq!(Auth::named(Some("")).unwrap(), Auth::Password);
        assert_eq!(Auth::named(Some(" gcp-iam ")).unwrap(), Auth::GcpIam);
        for name in Auth::NAMES {
            assert_eq!(Auth::named(Some(name)).unwrap().name(), name);
        }
        assert!(Auth::named(Some("kerberos")).is_err());
    }

    #[test]
    fn a_token_only_to_a_checked_server() {
        let (kept, none) = Auth::Password.prepared("host=h user=u sslmode=disable").unwrap();
        assert_eq!((kept.as_str(), none.is_none()), ("host=h user=u sslmode=disable", true));

        for auth in [Auth::AwsIam, Auth::GcpIam, Auth::AzureEntra] {
            for refused in [
                "host=h user=u sslmode=require",
                "postgresql://u@h/db?sslmode=prefer",
                "host=h user=u password=p sslmode=verify-full",
                "postgresql://u:p@h/db",
            ] {
                let error = auth.prepared(refused).expect_err(refused).to_string();
                assert!(error.contains(&format!("auth `{}`", auth.name())), "{error}");
            }
        }
    }

    #[cfg(all(feature = "s3", feature = "gcs", feature = "azure"))]
    #[test]
    fn verify_full_unless_named() {
        let (postgres, credential) = Auth::GcpIam.prepared("postgresql://u@h/db").unwrap();
        assert_eq!(postgres, "postgresql://u@h/db?sslmode=verify-full");
        assert!(credential.is_some());
        let (postgres, _) = Auth::AzureEntra.prepared("host=h user=u sslmode=verify-ca").unwrap();
        assert_eq!(postgres, "host=h user=u sslmode=verify-ca");
        let (postgres, _) = Auth::AwsIam
            .prepared("host=db.x.eu-west-1.rds.amazonaws.com user=u")
            .unwrap();
        assert_eq!(
            postgres,
            "host=db.x.eu-west-1.rds.amazonaws.com user=u sslmode=verify-full"
        );
    }
}
