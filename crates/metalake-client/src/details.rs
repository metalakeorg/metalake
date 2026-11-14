//! What the catalog says of a ref, a schema and a table, for a page that shows them as a catalog of assets does (ADR
//! 0007, the Catalog views): what each is, how much it holds, and its history. Read only, at the ref's head; a tag's
//! is the lake as it was when tagged.
//!
//! How much a table holds is summed from its live files as the catalog lists them, without reading one: rows less
//! those its deletion vectors delete, bytes, and how many files are stored or kept inline in the catalog.

use std::collections::BTreeMap;

use futures_util::StreamExt;
use metalake::{ICEBERG_METADATA, Scan, SnapshotEntry, Table};
use serde::Serialize;
use serde_json::Value;

use crate::iceberg::IcebergTable;
use crate::refs::RefInfo;
use crate::{Error, Lake, Result};

/// How much something holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Size {
    pub tables: i64,
    pub rows: i64,
    pub bytes: i64,
}

impl Size {
    fn add(&mut self, table: &TableSize) {
        self.tables += 1;
        self.rows += table.rows;
        self.bytes += table.bytes;
    }
}

/// How much a table holds, from its live files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TableSize {
    /// Its rows: those of its files, less those their deletion vectors delete.
    pub rows: i64,
    pub bytes: i64,
    /// Its files stored in the warehouse.
    pub files: i64,
    /// Its files kept in the catalog itself, until they are flushed (INL-1).
    pub inline_files: i64,
    /// The rows its deletion vectors delete.
    pub deleted_rows: i64,
}

/// A ref, what it holds, and its schemas.
#[derive(Debug, Clone, Serialize)]
pub struct RefDetails {
    #[serde(flatten)]
    pub info: RefInfo,
    /// Where the lake's files are.
    pub warehouse: Option<String>,
    pub size: Size,
    pub schemas: Vec<SchemaSummary>,
}

/// A schema, by its levels, and what it holds. A level may hold a dot: a schema is known by its levels, never by
/// their names joined.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaSummary {
    pub namespace: Vec<String>,
    pub size: Size,
}

/// A schema, its properties, and its tables.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaDetails {
    pub namespace: Vec<String>,
    pub properties: BTreeMap<String, String>,
    pub size: Size,
    pub tables: Vec<TableSummary>,
}

/// A table of a schema, what it holds, and when it last changed.
#[derive(Debug, Clone, Serialize)]
pub struct TableSummary {
    pub name: String,
    pub rows: i64,
    pub bytes: i64,
    /// When its latest snapshot on the ref was committed.
    pub changed_at: Option<String>,
}

/// A column of a table, nested ones included.
#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub id: i32,
    /// The column it is a field of, for a nested one.
    pub parent: Option<i32>,
    pub name: String,
    /// Its type, as the catalog writes it (Iceberg's).
    pub r#type: String,
    pub required: bool,
    pub doc: Option<String>,
    /// What a write that does not name it gives it.
    pub default: Option<Value>,
}

/// A table: what it is, how it is laid out, how much it holds, and where its Iceberg metadata stands.
#[derive(Debug, Clone, Serialize)]
pub struct TableDetails {
    pub namespace: Vec<String>,
    pub name: String,
    pub id: String,
    /// The prefix its files are written under.
    pub location: String,
    pub columns: Vec<Column>,
    #[serde(flatten)]
    pub size: TableSize,
    /// Its default partition spec, as Iceberg writes one; none for a table that has none.
    pub partition: Option<Value>,
    /// Its default sort order, as Iceberg writes one.
    pub sort_order: Option<Value>,
    /// The columns whose statistics are indexed for pruning.
    pub cluster_key: Vec<String>,
    pub properties: BTreeMap<String, String>,
    /// Where its Iceberg metadata stands, for a table set to write it on commit.
    pub iceberg: Option<IcebergTable>,
}

impl Lake {
    /// A ref: what it is, what it holds, and its schemas, each with what it holds.
    pub async fn ref_details(&self, on: &str) -> Result<RefDetails> {
        let info = self
            .refs(None)
            .await?
            .into_iter()
            .find(|r| r.name == on)
            .ok_or_else(|| Error::usage(format!("no ref {on}")))?;
        let named = self.find_ref(on).await?;
        let mut schemas: BTreeMap<Vec<String>, Size> = named
            .namespaces()
            .await?
            .into_iter()
            .map(|n| (n.namespace, Size::default()))
            .collect();
        let mut size = Size::default();
        for name in named.tables().await? {
            let table = named.table(&parts(&name.namespace), &name.name).await?;
            let held = self.table_size(&table).await?;
            size.add(&held);
            schemas.entry(name.namespace).or_default().add(&held);
        }
        Ok(RefDetails {
            info,
            warehouse: self.catalog.warehouse().map(str::to_owned),
            size,
            schemas: schemas
                .into_iter()
                .map(|(namespace, size)| SchemaSummary { namespace, size })
                .collect(),
        })
    }

    /// A schema of a ref, by its levels: its properties, and its tables, each with what it holds.
    pub async fn schema_details(&self, on: &str, schema: &[String]) -> Result<SchemaDetails> {
        let named = self.find_ref(on).await?;
        let wanted = schema.to_vec();
        let found = named
            .namespaces()
            .await?
            .into_iter()
            .find(|n| n.namespace == wanted)
            .ok_or_else(|| Error::usage(format!("no schema {} on {on}", schema.join("."))))?;
        let mut size = Size::default();
        let mut tables = Vec::new();
        for name in named.tables().await?.into_iter().filter(|t| t.namespace == wanted) {
            let table = named.table(&parts(&name.namespace), &name.name).await?;
            let held = self.table_size(&table).await?;
            size.add(&held);
            let latest = named.snapshots(&table, 1).await?;
            tables.push(TableSummary {
                name: name.name,
                rows: held.rows,
                bytes: held.bytes,
                changed_at: latest.into_iter().next().and_then(|s| s.committed_at),
            });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaDetails {
            namespace: wanted,
            properties: properties(&found.properties),
            size,
            tables,
        })
    }

    /// A table of a ref, by its schema's levels and its name.
    pub async fn table_details(&self, on: &str, schema: &[String], name: &str) -> Result<TableDetails> {
        let named = self.find_ref(on).await?;
        let table = named.table(&parts(schema), name).await?;
        let size = self.table_size(&table).await?;
        let column = |id: &i32| {
            table
                .fields()
                .iter()
                .find(|f| f.field_id == *id)
                .map(|f| f.name.clone())
        };
        // Asked only of a table set to write its Iceberg metadata on commit: reading that metadata needs the lake's
        // storage, which a catalog of tables at locations of their own may not have.
        let iceberg = if table.properties().get(ICEBERG_METADATA).map(String::as_str) == Some("on-commit") {
            // The table's own, not one found among the ref's by a name that two schemas' tables may share.
            Some(self.iceberg().state(&named, &table).await?)
        } else {
            None
        };
        Ok(TableDetails {
            namespace: table.namespace().to_vec(),
            name: table.name().to_owned(),
            id: table.id().to_string(),
            location: table.location().to_owned(),
            columns: columns(&table),
            size,
            partition: table.spec_id().and_then(|id| table.specs().get(&id).cloned()),
            sort_order: table
                .sort_order_id()
                .and_then(|id| table.sort_orders().get(&id).cloned()),
            cluster_key: table.cluster_key().iter().filter_map(column).collect(),
            properties: table.properties().clone(),
            iceberg,
        })
    }

    /// A table's snapshots on a ref, by its schema's levels and its name, the latest first.
    pub async fn table_snapshots(
        &self,
        on: &str,
        schema: &[String],
        name: &str,
        limit: i64,
    ) -> Result<Vec<SnapshotEntry>> {
        let named = self.find_ref(on).await?;
        let table = named.table(&parts(schema), name).await?;
        Ok(named.snapshots(&table, limit).await?)
    }

    /// What a table holds, from the files the catalog lists for it.
    async fn table_size(&self, table: &Table) -> Result<TableSize> {
        let mut size = TableSize::default();
        let mut files = Scan::new(&self.catalog, table).list().await?;
        while let Some(file) = files.next().await {
            let file = file?;
            let deleted = file.vector.as_ref().map_or(0, |v| v.cardinality);
            size.rows += file.record_count - deleted;
            size.deleted_rows += deleted;
            size.bytes += file.file_size;
            if file.path.is_some() {
                size.files += 1;
            } else {
                size.inline_files += 1;
            }
        }
        Ok(size)
    }
}

/// A namespace's parts, as a ref takes them.
fn parts(namespace: &[String]) -> Vec<&str> {
    namespace.iter().map(String::as_str).collect()
}

/// A table's columns, in their order, nested ones after their parent.
fn columns(table: &Table) -> Vec<Column> {
    let mut fields: Vec<_> = table.fields().to_vec();
    fields.sort_by_key(|f| (f.parent_field_id.unwrap_or(0), f.position));
    fields
        .into_iter()
        .map(|f| Column {
            id: f.field_id,
            parent: f.parent_field_id,
            name: f.name,
            r#type: f.ty,
            required: f.required,
            doc: f.doc,
            default: f.write_default,
        })
        .collect()
}

/// A namespace's properties, as text, which a property's value is.
fn properties(value: &Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), v.as_str().map_or_else(|| v.to_string(), str::to_owned)))
                .collect()
        })
        .unwrap_or_default()
}
