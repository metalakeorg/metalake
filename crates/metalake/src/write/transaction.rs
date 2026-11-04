//! A transaction: a value until it commits, then one call of the protocol (CMT-1 to CMT-11). It may write several
//! tables, create tables and namespaces, and carry an idempotency key that makes repeating it safe.

use arrow_schema::Schema;
use metalake_pg::records::{FileAdd, FileRef, TableExpect};
use metalake_pg::{Commit, ErrorKind};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::catalog::Ref;
use crate::error::{Error, Result};
use crate::partition::Transform;
use crate::schema::spec_fields;
use crate::storage::Storage;
use crate::table::Table;
use crate::vectors::Deletes;
use crate::write::delete::{self, TableDeletes};

/// How many times a commit is tried when the attempt may be repeated.
pub(crate) const ATTEMPTS: usize = 3;

/// What a commit did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// It committed, at this sequence.
    Committed(i64),
    /// The same idempotency key had committed already, at this sequence; nothing was written again (CMT-8).
    AlreadyApplied(i64),
}

impl Outcome {
    pub fn seq(self) -> i64 {
        match self {
            Self::Committed(s) | Self::AlreadyApplied(s) => s,
        }
    }
}

/// A table a transaction creates: its name, its columns as Arrow, and where its files go.
#[derive(Debug, Clone)]
pub struct NewTable<'a> {
    pub namespace: &'a [&'a str],
    pub name: &'a str,
    pub schema: &'a Schema,
    /// Where its files go; the catalog's warehouse, by namespace and name, when not given.
    pub location: Option<String>,
    /// The columns its files are clustered by, by name, the first indexed for pruning (STA-3).
    pub cluster_key: Vec<&'a str>,
    /// Its partition spec: a top-level column and a transform for each field (`identity`, `bucket[16]`, `day`…).
    pub partitioning: Vec<(&'a str, &'a str)>,
    pub properties: Vec<(&'a str, &'a str)>,
}

/// A transaction on a ref.
#[derive(Debug, Clone)]
pub struct Transaction {
    on: Ref,
    files: Vec<FileAdd>,
    removed: Vec<FileRef>,
    deletes: Vec<TableDeletes>,
    expected: Vec<TableExpect>,
    changes: Map<String, Value>,
    namespaces: Vec<Value>,
    replace: bool,
    app: Option<(String, i64)>,
    author: Option<String>,
    message: Option<String>,
}

impl Transaction {
    pub(crate) fn new(on: Ref) -> Self {
        Self {
            on,
            files: Vec::new(),
            removed: Vec::new(),
            deletes: Vec::new(),
            expected: Vec::new(),
            changes: Map::new(),
            namespaces: Vec::new(),
            replace: false,
            app: None,
            author: None,
            message: None,
        }
    }

    /// Adds files to a table; they are its writer's, whose `table_id` they carry.
    pub fn append(mut self, files: impl IntoIterator<Item = FileAdd>) -> Self {
        self.files.extend(files);
        self
    }

    /// Removes files of a table, by id.
    pub fn remove(mut self, table: &Table, file_ids: impl IntoIterator<Item = i64>) -> Self {
        let table_id = table.id();
        self.removed
            .extend(file_ids.into_iter().map(|file_id| FileRef { table_id, file_id }));
        self
    }

    /// Deletes rows of a table's files, which `storage` holds, as `table` read them: each file's vector becomes the
    /// union of these and the one it has. If another commit changes a vector first, the union is taken again over
    /// the new one, and the commit retried (DV-4).
    pub fn delete(mut self, table: &Table, storage: &Storage, deletes: Deletes) -> Self {
        if !deletes.is_empty() {
            self.deletes.push(TableDeletes {
                table: table.clone(),
                storage: storage.clone(),
                deletes,
            });
        }
        self
    }

    /// Makes the commit a `replace`: the files it removes are rewritten by those it adds, which hold the same live
    /// rows (CMT-11), as compaction does.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Refuses the commit if `table` changed since it was read (CMT-7): for writes that depend on what they read,
    /// such as an overwrite. Appends need none, since they do not conflict.
    pub fn expect_unchanged(mut self, table: &Table) -> Self {
        self.expected.push(TableExpect {
            table_id: table.id(),
            snapshot_id: table.snapshot_id(),
        });
        self
    }

    /// Creates a namespace (NS-1).
    pub fn create_namespace(mut self, namespace: &[&str]) -> Self {
        self.namespaces.push(json!({"namespace": namespace, "create": true}));
        self
    }

    /// Creates a table, and answers its id along with the transaction.
    pub fn create_table(mut self, new: NewTable<'_>) -> Result<(Self, Uuid)> {
        let id = Uuid::new_v4();
        let fields = spec_fields(new.schema)?;
        let column = |name: &str, what: &str| {
            fields
                .iter()
                .find(|f| f.parent_field_id.is_none() && f.name == name)
                .map(|f| f.field_id)
                .ok_or_else(|| Error::Schema(format!("the {what} names {name}, which is not a column")))
        };
        let cluster_key = new
            .cluster_key
            .iter()
            .map(|name| column(name, "cluster key"))
            .collect::<Result<Vec<_>>>()?;
        // Partition fields take ids from 1000, as Iceberg's do.
        let partition_spec = new
            .partitioning
            .iter()
            .enumerate()
            .map(|(i, (name, transform))| {
                let parsed = Transform::parse(transform)?;
                Ok(
                    json!({"source_ids": [column(name, "partition spec")?], "field_id": 1000 + i,
                          "name": parsed.field_name(name), "transform": transform}),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let location = match new.location {
            Some(l) => l,
            None => {
                let warehouse = self.on.catalog().warehouse().ok_or_else(|| {
                    Error::Location(format!("{} needs a location: the catalog has no warehouse", new.name))
                })?;
                let mut l = warehouse.trim_end_matches('/').to_string();
                for part in new.namespace.iter().chain([&new.name]) {
                    l = format!("{l}/{part}");
                }
                format!("{l}-{}", id.simple())
            }
        };
        let fields: Vec<Value> = fields
            .iter()
            .map(|f| {
                json!({"field_id": f.field_id, "new": true, "parent_field_id": f.parent_field_id,
                       "position": f.position, "name": f.name, "type": f.ty, "required": f.required})
            })
            .collect();
        let properties: Map<String, Value> = new.properties.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
        let mut definition = json!({
            "create": {"namespace": new.namespace, "name": new.name, "location": location},
            "fields": fields,
        });
        if !cluster_key.is_empty() {
            definition["cluster_key"] = json!(cluster_key);
        }
        if !partition_spec.is_empty() {
            definition["partition_spec"] = Value::Array(partition_spec);
        }
        if !properties.is_empty() {
            definition["properties"] = Value::Object(properties);
        }
        self.changes.insert(id.to_string(), definition);
        Ok((self, id))
    }

    /// The idempotency key (CMT-8): a writer's id and a version it never repeats for different data, such as the
    /// offset of a stream. A commit with a key may be repeated safely; once applied, repeating it applies nothing.
    pub fn idempotent(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.app = Some((app_id.into(), version));
        self
    }

    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// What the commit does, as history shows it.
    fn operation(&self) -> &'static str {
        let keeps = self.removed.is_empty() && self.deletes.is_empty();
        match (self.replace, self.files.is_empty(), keeps) {
            (true, _, _) => "replace",
            (false, false, true) => "append",
            (false, false, false) => "overwrite",
            (false, true, false) => "delete",
            (false, true, true) => "update",
        }
    }

    /// Commits, in one call. A failure Postgres had in serializing it is repeated; so is a lost connection when the
    /// transaction has an idempotency key, which makes the repeat answer what the first attempt did. Without a key,
    /// a lost connection is an error whose kind says the outcome is unknown. A delete whose file's vector changed
    /// under it is taken again over the new vector and retried (DV-4).
    pub async fn commit(self) -> Result<Outcome> {
        self.on.writable()?;
        let (app_id, app_version) = self.app.clone().unzip();
        let mut call = Commit {
            ref_id: self.on.id(),
            operation: self.operation().into(),
            files: self.files,
            removed: self.removed,
            vectors: Vec::new(),
            expected: self.expected,
            changes: (!self.changes.is_empty()).then_some(Value::Object(self.changes)),
            author: self.author,
            actor: Some("metalake".into()),
            app_id,
            app_version,
            message: self.message,
            definition: None,
            namespaces: (!self.namespaces.is_empty()).then_some(Value::Array(self.namespaces)),
        };
        let catalog = self.on.catalog();
        let (mut attempt, mut rebase) = (0, false);
        loop {
            attempt += 1;
            if !self.deletes.is_empty() {
                call.vectors.clear();
                for d in &self.deletes {
                    let at = if rebase { None } else { Some(d.table.seq()) };
                    call.vectors.extend(d.vectors(catalog, at).await?);
                }
            }
            let result = async { catalog.protocol().commit(&catalog.pg_session().await?, &call).await }.await;
            match result {
                Ok(r) if r.applied => return Ok(Outcome::Committed(r.commit_seq)),
                Ok(r) => return Ok(Outcome::AlreadyApplied(r.commit_seq)),
                Err(e) if attempt < ATTEMPTS && repeatable(e.kind(), call.app_id.is_some()) => continue,
                Err(e) if attempt < ATTEMPTS && !self.deletes.is_empty() && delete::rebases(&e) => rebase = true,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// Whether a commit that failed with `kind` may be tried again (ADR 0003 §3.6).
pub(crate) fn repeatable(kind: ErrorKind, idempotent: bool) -> bool {
    match kind {
        ErrorKind::Serialization => true,
        ErrorKind::Unknown => idempotent,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_what_cannot_have_taken_effect_twice_is_repeated() {
        assert!(repeatable(ErrorKind::Serialization, false));
        assert!(repeatable(ErrorKind::Unknown, true));
        assert!(!repeatable(ErrorKind::Unknown, false));
        for kind in [
            ErrorKind::Conflict,
            ErrorKind::Invalid,
            ErrorKind::Unsupported,
            ErrorKind::Postgres,
        ] {
            assert!(!repeatable(kind, true), "{kind:?}");
        }
    }
}
