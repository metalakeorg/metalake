//! Credentials a client is given for a table's files (REST-11), as Iceberg's REST catalog API spells them.

use std::collections::BTreeMap;

use serde::Serialize;

/// A credential for the files under `prefix`: a client takes, for each file, the one whose prefix is the longest it
/// starts with, and reads its `config` before the table's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: BTreeMap<String, String>,
}

/// `loadCredentials`: fresh credentials for a table's files.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadCredentialsResponse {
    pub storage_credentials: Vec<StorageCredential>,
}
