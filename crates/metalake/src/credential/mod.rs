//! How a lake's client signs in to its catalog's Postgres: by the password the connection string or `PGPASSWORD`
//! gives, or by a cloud's token for the client's own identity, minted for each new connection. In each cloud the
//! identity is the user Postgres knows: an IAM role or user on AWS, a service account or user on Google Cloud, an Entra
//! principal on Azure. The token is what a cloud's managed Postgres takes as that user's password.

use std::sync::Arc;

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
}
