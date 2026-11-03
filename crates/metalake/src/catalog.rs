//! A catalog and its refs: where every read and write of the core starts.

use std::sync::Arc;

use metalake_pg::records::{RefRow, TableName};
use metalake_pg::{Pool, PooledSession, Protocol, Roles, Versions};

use crate::error::{Error, Result};
use crate::table::Table;

/// How to reach a catalog: the Postgres database, the catalog's schema in it, and how many connections to hold.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// A libpq connection string or URL.
    pub postgres: String,
    pub schema: String,
    pub connections: usize,
    /// Where new tables put their files when they name no location: a URL, such as `gs://lake/warehouse`.
    pub warehouse: Option<String>,
}

impl CatalogConfig {
    pub fn new(postgres: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            postgres: postgres.into(),
            schema: schema.into(),
            connections: 8,
            warehouse: None,
        }
    }

    pub fn warehouse(mut self, url: impl Into<String>) -> Self {
        self.warehouse = Some(url.into());
        self
    }
}

/// A catalog: a cheap handle over its pool, shared by every ref and table read from it.
#[derive(Debug, Clone)]
pub struct Catalog {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    pool: Pool,
    protocol: Protocol,
    versions: Versions,
    warehouse: Option<String>,
}

impl Catalog {
    /// Connects to a catalog and checks that the core speaks its versions (LAY-3).
    pub async fn connect(config: CatalogConfig) -> Result<Self> {
        let pool = Pool::new(&config.postgres, config.connections)?;
        let versions = metalake_pg::check_versions(&pool.get().await?, &config.schema).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                protocol: Protocol::new(&config.schema),
                versions,
                warehouse: config.warehouse,
            }),
        })
    }

    /// Creates a catalog in a new schema (`metalake init`), with its roles if given, and connects to it.
    pub async fn init(config: CatalogConfig, roles: Option<&Roles>) -> Result<Self> {
        let pool = Pool::new(&config.postgres, 1)?;
        let mut session = pool.get().await?;
        metalake_pg::init(&mut session, &config.schema, roles).await?;
        Self::connect(config).await
    }

    /// The versions of the catalog; whether the core may write it is `versions().writable()`.
    pub fn versions(&self) -> &Versions {
        &self.inner.versions
    }

    /// A branch, by name.
    pub async fn branch(&self, name: &str) -> Result<Ref> {
        self.find(name, "branch").await
    }

    /// A tag, by name.
    pub async fn tag(&self, name: &str) -> Result<Ref> {
        self.find(name, "tag").await
    }

    /// Every ref of the catalog that is not deleted.
    pub async fn refs(&self) -> Result<Vec<Ref>> {
        let rows = self.inner.protocol.refs(&self.session().await?).await?;
        Ok(rows.into_iter().map(|row| self.handle(row)).collect())
    }

    async fn find(&self, name: &str, kind: &str) -> Result<Ref> {
        self.refs()
            .await?
            .into_iter()
            .find(|r| r.name == name && r.kind == kind)
            .ok_or_else(|| Error::NotFound(format!("{kind} {name}")))
    }

    fn handle(&self, row: RefRow) -> Ref {
        Ref {
            catalog: self.clone(),
            id: row.ref_id,
            name: row.name,
            kind: row.kind,
            at: None,
        }
    }

    pub(crate) async fn session(&self) -> Result<PooledSession> {
        Ok(self.inner.pool.get().await?)
    }

    /// A session, with the SDK's error, for a call whose error decides whether it is repeated.
    pub(crate) async fn pg_session(&self) -> metalake_pg::Result<PooledSession> {
        self.inner.pool.get().await
    }

    pub(crate) fn protocol(&self) -> &Protocol {
        &self.inner.protocol
    }

    pub(crate) fn warehouse(&self) -> Option<&str> {
        self.inner.warehouse.as_deref()
    }
}

/// A branch or a tag, read at its latest state or at a sequence: a cheap handle.
#[derive(Debug, Clone)]
pub struct Ref {
    catalog: Catalog,
    id: i32,
    name: String,
    kind: String,
    at: Option<i64>,
}

impl Ref {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `branch` or `tag`.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// A transaction on this ref, which commits on its latest state.
    pub fn transaction(&self) -> crate::Transaction {
        crate::Transaction::new(self.clone())
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The same ref, read at `seq` instead of its latest state.
    pub fn at(&self, seq: i64) -> Self {
        Self {
            at: Some(seq),
            ..self.clone()
        }
    }

    /// The sequence a read of this ref is at: the one given, or a stable point taken now (VIS-11), so that every
    /// table read from one point sees the same commits.
    pub async fn read_point(&self) -> Result<i64> {
        match self.at {
            Some(seq) => Ok(seq),
            None => {
                let p = self.catalog.protocol();
                Ok(p.stable_point(&self.catalog.session().await?).await?.seq)
            }
        }
    }

    /// The tables this ref sees at its read point.
    pub async fn tables(&self) -> Result<Vec<TableName>> {
        let seq = self.read_point().await?;
        let p = self.catalog.protocol();
        Ok(p.tables_at(&self.catalog.session().await?, self.id, Some(seq)).await?)
    }

    /// A table by its name, as this ref sees it at its read point.
    pub async fn table(&self, namespace: &[&str], name: &str) -> Result<Table> {
        let seq = self.read_point().await?;
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        let found = p
            .tables_at(&session, self.id, Some(seq))
            .await?
            .into_iter()
            .find(|t| t.namespace == namespace && t.name == name)
            .ok_or_else(|| Error::NotFound(format!("table {}.{name} on {}", namespace.join("."), self.name)))?;
        let definition = p
            .describe(&session, self.id, found.table_id, Some(seq))
            .await?
            .ok_or_else(|| Error::NotFound(format!("table {} on {}", found.table_id, self.name)))?;
        Table::from_definition(self.id, seq, &definition)
    }
}
