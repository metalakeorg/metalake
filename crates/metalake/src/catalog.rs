//! A catalog and its refs: where every read and write of the core starts.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use metalake_pg::records::{RefRow, TableName};
use metalake_pg::{CreateRef, Pool, PooledSession, Protocol, Retention, Roles, Versions};

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

    /// The maintenance of this catalog: expiring, sweeping, orphans and commit times.
    pub fn maintenance(&self) -> crate::Maintenance {
        crate::Maintenance::new(self.clone())
    }

    /// How long a queued object waits before a sweep deletes it (RET-9), which reads should end within.
    pub async fn set_collect_grace(&self, grace: Duration) -> Result<()> {
        let session = self.session().await?;
        Ok(self.inner.protocol.set_collect_grace(&session, grace).await?)
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
            at: Point::Latest,
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
    at: Point,
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

    /// A transaction on this ref, which commits on its latest state; only a branch read at its latest takes one.
    pub fn transaction(&self) -> crate::Transaction {
        crate::Transaction::new(self.clone())
    }

    /// A merge of this branch into its parent, what it would do or doing it (MRG-1 to MRG-8).
    pub fn merge(&self) -> crate::Merge {
        crate::Merge::new(self.clone())
    }

    /// What this ref keeps (RET-1); expiring takes the rest.
    pub async fn set_retention(&self, retention: &Retention) -> Result<()> {
        let session = self.catalog.session().await?;
        Ok(self
            .catalog
            .protocol()
            .set_retention(&session, self.id, retention)
            .await?)
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The same ref, read at `seq` instead of its latest state.
    pub fn at(&self, seq: i64) -> Self {
        Self {
            at: Point::Seq(seq),
            ..self.clone()
        }
    }

    /// The same ref, read as of `time` (TT-3): each table at the sequence of its state then. A commit that wrote
    /// several tables has one time, so the tables read agree as one point would.
    pub fn as_of(&self, time: SystemTime) -> Self {
        Self {
            at: Point::Time(time),
            ..self.clone()
        }
    }

    /// The sequence a read of this ref is at: the one given, or a stable point taken now (VIS-11), so that every
    /// table read from one point sees the same commits. A read as of a time has a sequence per table instead.
    pub async fn read_point(&self) -> Result<i64> {
        match self.at {
            Point::Seq(seq) => Ok(seq),
            Point::Latest => self.stable().await,
            Point::Time(_) => Err(Error::Invalid(format!(
                "a read of {} as of a time reads each table at its own sequence",
                self.name
            ))),
        }
    }

    /// A sequence no commit at or below it can still change (VIS-11). The last one drawn becomes that once the commits
    /// in flight when it was taken have ended, which takes milliseconds. Any transaction left open on the server holds
    /// this up, so a catalog's Postgres keeps transactions short; a read that waits is one a caller can cancel, and it
    /// never reads a point that can still change.
    async fn stable(&self) -> Result<i64> {
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        let point = p.stable_point(&session).await?;
        let mut pause = Duration::from_millis(1);
        while !p.is_stable(&session, point.horizon).await? {
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_millis(50));
        }
        Ok(point.seq)
    }

    /// The tables this ref sees at its read point, each with the sequence a read of it is at.
    async fn tables_seen(&self) -> Result<Vec<(TableName, i64)>> {
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        Ok(match self.at {
            Point::Time(time) => p
                .tables_as_of(&session, self.id, time)
                .await?
                .into_iter()
                .map(|t| {
                    let name = TableName {
                        table_id: t.table_id,
                        namespace: t.namespace,
                        name: t.name,
                    };
                    (name, t.seq)
                })
                .collect(),
            _ => {
                let seq = self.read_point().await?;
                let names = p.tables_at(&session, self.id, Some(seq)).await?;
                names.into_iter().map(|t| (t, seq)).collect()
            }
        })
    }

    /// The tables this ref sees at its read point.
    pub async fn tables(&self) -> Result<Vec<TableName>> {
        Ok(self.tables_seen().await?.into_iter().map(|(t, _)| t).collect())
    }

    /// A table by its name, as this ref sees it at its read point.
    pub async fn table(&self, namespace: &[&str], name: &str) -> Result<Table> {
        let (found, seq) = self
            .tables_seen()
            .await?
            .into_iter()
            .find(|(t, _)| t.namespace == namespace && t.name == name)
            .ok_or_else(|| Error::NotFound(format!("table {}.{name} on {}", namespace.join("."), self.name)))?;
        let p = self.catalog.protocol();
        let definition = p
            .describe(&self.catalog.session().await?, self.id, found.table_id, Some(seq))
            .await?
            .ok_or_else(|| Error::NotFound(format!("table {} on {}", found.table_id, self.name)))?;
        Table::from_definition(self.id, seq, &definition)
    }

    /// A branch of this ref, from its read point (BR-1 to BR-3).
    pub async fn create_branch(&self, name: &str) -> Result<Ref> {
        self.create(name, "branch").await
    }

    /// A tag of this ref, at its read point: a name for that state, which no commit changes.
    pub async fn create_tag(&self, name: &str) -> Result<Ref> {
        self.create(name, "tag").await
    }

    async fn create(&self, name: &str, kind: &str) -> Result<Ref> {
        let at = match self.at {
            Point::Latest => None,
            Point::Seq(seq) => Some(seq),
            Point::Time(_) => {
                return Err(Error::Invalid(format!(
                    "a ref of {} as of a time has no one sequence to start from: give it one with at",
                    self.name
                )));
            }
        };
        let p = self.catalog.protocol();
        let new = CreateRef {
            name: name.into(),
            parent: self.id,
            kind: kind.into(),
            at,
            author: "metalake".into(),
        };
        let id = p.create_ref(&self.catalog.session().await?, &new).await?;
        Ok(Ref {
            catalog: self.catalog.clone(),
            id,
            name: name.into(),
            kind: kind.into(),
            at: Point::Latest,
        })
    }

    /// Deletes the ref; its rows go at the next collection (RET-6).
    pub async fn delete(self) -> Result<()> {
        let p = self.catalog.protocol();
        Ok(p.delete_ref(&self.catalog.session().await?, self.id).await?)
    }

    /// Whether this handle reads the ref's latest state, not a point of its past.
    pub fn is_latest(&self) -> bool {
        matches!(self.at, Point::Latest)
    }

    /// Whether a commit may write this ref: a branch, at its latest state.
    pub fn writable(&self) -> Result<()> {
        if self.kind != "branch" {
            return Err(Error::Invalid(format!(
                "{} is a {}, which no commit writes",
                self.name, self.kind
            )));
        }
        if !matches!(self.at, Point::Latest) {
            return Err(Error::Invalid(format!(
                "{} is read in the past here, and a commit writes its latest state",
                self.name
            )));
        }
        Ok(())
    }
}

/// Where a read of a ref is: its latest state, a sequence, or a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    Latest,
    Seq(i64),
    Time(SystemTime),
}
