//! What the source and the sink open in each subtask: the catalog, the table on its branch, and the store of its files;
//! and how the core's errors reach Arroyo, by what to do about them.

use arroyo_rpc::connector_err;
use arroyo_rpc::errors::{DataflowError, ErrorDomain, RetryHint};
use metalake::environment;
use metalake::{Catalog, CatalogConfig, Ref, Storage, Table};
use metalake_pg::ErrorKind;

use crate::config::{MetaLakeProfile, MetaLakeTable};

/// Connections a subtask's catalog pool holds.
const CONNECTIONS: usize = 2;

/// A table on its branch, as one subtask reaches it.
pub struct Lake {
    pub catalog: Catalog,
    pub branch: Ref,
    pub table: Table,
    pub storage: Storage,
}

impl Lake {
    pub async fn open(profile: &MetaLakeProfile, at: &MetaLakeTable) -> Result<Self, DataflowError> {
        let name = at.qualified_name();
        let config = catalog_config(profile, CONNECTIONS).map_err(|e| user(&name, e))?;
        let catalog = Catalog::connect(config).await.map_err(|e| core(&name, e))?;
        let branch = catalog.branch(&at.branch).await.map_err(|e| core(&name, e))?;
        let namespace: Vec<&str> = at.namespace.iter().map(String::as_str).collect();
        let table = branch.table(&namespace, &at.table).await.map_err(|e| core(&name, e))?;
        let options = profile
            .storage
            .iter()
            .map(|(k, v)| Ok((k.clone(), v.sub_env_vars()?)))
            .collect::<anyhow::Result<Vec<(String, String)>>>()
            .map_err(|e| user(&name, e))?;
        let storage = Storage::new(table.location(), options).map_err(|e| core(&name, e))?;
        Ok(Self {
            catalog,
            branch,
            table,
            storage,
        })
    }

    /// The table as its branch has it now, for a commit that reads it.
    pub async fn latest_table(&self) -> metalake::Result<Table> {
        let namespace: Vec<&str> = self.table.namespace().iter().map(String::as_str).collect();
        self.branch.table(&namespace, self.table.name()).await
    }
}

/// How a worker reaches the profile's catalog: its connection string, its schema, and the way it signs in, with a pool of
/// `connections`.
pub fn catalog_config(profile: &MetaLakeProfile, connections: usize) -> anyhow::Result<CatalogConfig> {
    let mut config = CatalogConfig::new(postgres_of(profile)?, &profile.schema).auth(profile.auth()?);
    config.connections = connections;
    Ok(config)
}

/// The catalog's connection string, its `{{ VAR }}` read from the environment, with `PGPASSWORD` as a client adds it:
/// the driver does not read the password from the environment by itself.
pub fn postgres_of(profile: &MetaLakeProfile) -> anyhow::Result<String> {
    let postgres = profile.postgres.sub_env_vars()?;
    let password = std::env::var(environment::PASSWORD).ok();
    Ok(environment::with_password(&postgres, password.as_deref()))
}

/// A core error by what to do about it: try again for what may pass, stop for what the user has to change.
pub fn core(table: &str, e: metalake::Error) -> DataflowError {
    let (domain, retry) = classify(&e);
    DataflowError::ConnectorError {
        domain,
        retry,
        error: format!("{table}: {e}"),
        source: Some(e.into()),
    }
}

fn classify(e: &metalake::Error) -> (ErrorDomain, RetryHint) {
    use metalake::Error;
    match e {
        Error::Protocol(p) => match p.kind() {
            ErrorKind::Conflict | ErrorKind::Serialization | ErrorKind::Unknown | ErrorKind::Postgres => {
                (ErrorDomain::External, RetryHint::WithBackoff)
            }
            ErrorKind::Invalid | ErrorKind::Unsupported | ErrorKind::Permission => {
                (ErrorDomain::User, RetryHint::NoRetry)
            }
            _ => (ErrorDomain::Internal, RetryHint::NoRetry),
        },
        Error::Storage(_) => (ErrorDomain::External, RetryHint::WithBackoff),
        Error::NotFound(_) | Error::Invalid(_) | Error::Unsupported(_) | Error::Location(_) => {
            (ErrorDomain::User, RetryHint::NoRetry)
        }
        _ => (ErrorDomain::Internal, RetryHint::NoRetry),
    }
}

/// An error the user has to fix: the configuration, or data the declaration doesn't allow.
pub fn user(table: &str, e: impl std::fmt::Display) -> DataflowError {
    connector_err!(User, NoRetry, "{table}: {e}")
}

/// An error a plan should have refused.
pub fn internal(table: &str, e: impl std::fmt::Display) -> DataflowError {
    connector_err!(Internal, NoRetry, "{table}: {e}")
}
