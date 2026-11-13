//! A catalog and its refs: where every read and write of the core starts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::TryStreamExt;
use metalake_pg::records::{CommitEntry, ListedFile, NamespaceRow, RefRow, SnapshotEntry, TableName, TableSnapshot};
use metalake_pg::{CreateRef, Credential, Pool, PooledSession, Protocol, Retention, Roles, Versions};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::credential::Auth;
use crate::error::{Error, Result};
use crate::table::Table;

/// How to reach a catalog: the Postgres database, the catalog's schema in it, and how many connections to hold.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// A libpq connection string or URL.
    pub postgres: String,
    pub schema: String,
    pub connections: usize,
    /// Stored by `init`; for `connect`, where this connection's new tables go instead of the catalog's warehouse: a URL, such as `gs://lake/warehouse`.
    pub warehouse: Option<String>,
    /// How the client signs in: the string's password, or a cloud's token for its identity.
    pub auth: Auth,
}

impl CatalogConfig {
    pub fn new(postgres: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            postgres: postgres.into(),
            schema: schema.into(),
            connections: 8,
            warehouse: None,
            auth: Auth::Password,
        }
    }

    pub fn warehouse(mut self, url: impl Into<String>) -> Self {
        self.warehouse = Some(url.into());
        self
    }

    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }

    /// Where a pool connects and with what, as the auth's rules hold them.
    fn reach(&self) -> Result<Reach> {
        let (postgres, credential) = self.auth.prepared(&self.postgres)?;
        Ok(Reach { postgres, credential })
    }
}

/// Where a catalog's pools connect, and the credential they sign in with if not the string's password.
#[derive(Clone)]
struct Reach {
    postgres: String,
    credential: Option<Arc<dyn Credential>>,
}

/// Where, without the password `PGPASSWORD` put in the string: a catalog's handle is printed.
impl std::fmt::Debug for Reach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reach")
            .field("postgres", &crate::environment::without_password(&self.postgres))
            .field("credential", &self.credential)
            .finish()
    }
}

impl Reach {
    fn pool(&self, size: usize, wait: Duration) -> Result<Pool> {
        Ok(Pool::connecting(&self.postgres, size, wait, self.credential.clone())?)
    }
}

/// How long a caller waits for a connection of a catalog's pool when all are taken.
const WAIT: Duration = Duration::from_secs(60);

/// Where to read each table of some refs, from [`Catalog::latest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestPoints(HashMap<(i32, Uuid), i64>);

impl LatestPoints {
    /// The sequence to read `table` at, on its ref; none if it is not there any more.
    pub fn seq(&self, table: &Table) -> Option<i64> {
        self.0.get(&(table.ref_id(), table.id())).copied()
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
    /// Where the pool connects, for a connection of one's own (`Catalog::lead`).
    reach: Reach,
    protocol: Protocol,
    versions: Versions,
    warehouse: Option<String>,
}

impl Catalog {
    /// Connects to a catalog and checks that the core speaks its versions (LAY-3).
    pub async fn connect(config: CatalogConfig) -> Result<Self> {
        let reach = config.reach()?;
        let pool = reach.pool(config.connections, WAIT)?;
        let session = pool.get().await?;
        let versions = metalake_pg::check_versions(&session, &config.schema).await?;
        let warehouse = match config.warehouse {
            Some(w) => Some(w),
            None => metalake_pg::warehouse(&session, &config.schema).await?,
        };
        drop(session);
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                reach,
                protocol: Protocol::new(&config.schema),
                versions,
                warehouse,
            }),
        })
    }

    /// Creates a catalog in a new schema (`metalake init`), with the config's warehouse and its roles if given, and
    /// connects to it.
    pub async fn init(config: CatalogConfig, roles: Option<&Roles>) -> Result<Self> {
        let pool = config.reach()?.pool(1, WAIT)?;
        let mut session = pool.get().await?;
        metalake_pg::init(&mut session, &config.schema, config.warehouse.as_deref(), roles).await?;
        Self::connect(config).await
    }

    /// Closes this handle's connections, for every clone of it. What it read goes on existing, but it asks no more.
    pub fn close(&self) {
        self.inner.pool.close();
    }

    /// Upgrades a catalog to this core's protocol (`metalake migrate`), and answers the sequence of a migration of the
    /// format's rows, if there was one. It connects by itself, since `connect` refuses a catalog that needs this, and
    /// it holds every ref while it runs, so nothing calls it on its own.
    pub async fn migrate(config: &CatalogConfig) -> Result<Option<i64>> {
        let pool = config.reach()?.pool(1, WAIT)?;
        let mut session = pool.get().await?;
        Ok(metalake_pg::migrate(&mut session, &config.schema).await?)
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
    pub async fn collect_grace(&self) -> Result<Duration> {
        let session = self.session().await?;
        Ok(self.inner.protocol.collect_grace(&session).await?)
    }

    /// How long a queued object waits before a sweep deletes it (RET-9), which reads should end within.
    pub async fn set_collect_grace(&self, grace: Duration) -> Result<()> {
        let session = self.session().await?;
        Ok(self.inner.protocol.set_collect_grace(&session, grace).await?)
    }

    /// Where to read each table of `refs` now, seen at once: whatever several reads take from this agree, as one point
    /// would, since a commit of several tables is in the sequence of each or of none.
    pub async fn latest(&self, refs: &[i32]) -> Result<LatestPoints> {
        let tables = self.inner.protocol.tables_latest(&self.session().await?, refs).await?;
        Ok(LatestPoints(
            tables.into_iter().map(|t| ((t.ref_id, t.table_id), t.seq)).collect(),
        ))
    }

    /// Every snapshot of a table, of any ref, the earliest first.
    pub async fn table_snapshots(&self, table: &Table) -> Result<Vec<TableSnapshot>> {
        Ok(self
            .inner
            .protocol
            .table_snapshots(&self.session().await?, table.id())
            .await?)
    }

    /// The snapshots of a table reached from `heads`, back through the last `keep` of each and those committed within
    /// `max_age_ms`, the earliest first. Its cost is what it returns, not the table's history.
    pub async fn ancestors(
        &self,
        table: &Table,
        heads: &[i64],
        keep: i32,
        max_age_ms: Option<i64>,
    ) -> Result<Vec<TableSnapshot>> {
        let session = self.session().await?;
        let found = self
            .inner
            .protocol
            .ancestors(&session, table.id(), heads, keep, max_age_ms);
        Ok(found.await?)
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

    /// The tables each commit of `seqs` made a snapshot of, in one call: its sequence, its ref's id and the table's.
    pub async fn commit_tables(&self, seqs: &[i64]) -> Result<Vec<(i64, i32, uuid::Uuid)>> {
        let session = self.session().await?;
        Ok(self.inner.protocol.commit_tables(&session, seqs).await?)
    }

    /// Each ref and table it sees whose property `key` is `value` at its latest (TBL-2): a ref's id and a table's.
    pub async fn tables_with_property(&self, key: &str, value: &str) -> Result<Vec<(i32, uuid::Uuid)>> {
        let session = self.session().await?;
        Ok(self.inner.protocol.tables_with_property(&session, key, value).await?)
    }

    /// Every ref of the catalog that is not deleted, as the protocol describes it: its parent and where it began.
    pub async fn describe_refs(&self) -> Result<Vec<RefRow>> {
        Ok(self.inner.protocol.refs(&self.session().await?).await?)
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

    /// The catalog's pool.
    pub(crate) fn pool(&self) -> &Pool {
        &self.inner.pool
    }

    /// A pool of its own, connecting as this catalog's does.
    pub(crate) fn own_pool(&self, wait: Duration) -> Result<Pool> {
        self.inner.reach.pool(1, wait)
    }

    /// A point of the catalog to follow it from, once its sequence is stable (VIS-11): no commit at or below it is in
    /// flight, and every one after it takes a later time.
    pub(crate) async fn stable_point(&self) -> Result<(SystemTime, i64)> {
        let session = self.session().await?;
        let p = self.protocol();
        let point = p.stream_point(&session).await?;
        let mut pause = Duration::from_millis(1);
        while !p.is_stable(&session, point.horizon).await? {
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_millis(50));
        }
        Ok((point.at, point.seq))
    }

    /// Where a table goes that names no location: under the warehouse, by namespace and name.
    pub fn default_location(&self, namespace: &[&str], name: &str, table_id: uuid::Uuid) -> Result<String> {
        let warehouse = self
            .warehouse()
            .ok_or_else(|| Error::Location(format!("{name} needs a location: the catalog has no warehouse")))?;
        let mut location = warehouse.trim_end_matches('/').to_string();
        for part in namespace.iter().chain([&name]) {
            location = format!("{location}/{part}");
        }
        Ok(format!("{location}-{}", table_id.simple()))
    }

    /// Where this connection's new tables put their files when they name no location: the config's, or the catalog's.
    pub fn warehouse(&self) -> Option<&str> {
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

    /// A table's definition on this ref, at `at` or now, in the protocol's shape (`describe`).
    pub async fn definition(&self, table: &Table, at: Option<i64>) -> Result<serde_json::Value> {
        let p = self.catalog.protocol();
        let found = p
            .describe(&self.catalog.session().await?, self.id, table.id(), at)
            .await?;
        found.ok_or_else(|| Error::NotFound(format!("table {} on {}", table.name(), self.name)))
    }

    /// The files commit `at` added to `table` on this ref, with no vectors. Its cost is what it returns, not the table.
    pub async fn added_files(&self, table: &Table, at: i64) -> Result<Vec<ListedFile>> {
        let p = self.catalog.protocol();
        let session = self.catalog.session().await?;
        let files = p.added_files(&session, self.id(), table.id(), at).await?;
        Ok(files.try_collect().await?)
    }

    /// The files each commit of `ats` added to `table` on this ref, with no vectors, in one call: by commit, as each
    /// file's `begin_seq` names it, and then by file. Its cost is what it returns, not the table.
    pub async fn added_files_of(&self, table: &Table, ats: &[i64]) -> Result<Vec<ListedFile>> {
        let p = self.catalog.protocol();
        let session = self.catalog.session().await?;
        let files = p.added_files_of(&session, self.id(), table.id(), ats).await?;
        Ok(files.try_collect().await?)
    }

    /// The files of `table` this ref sees among `paths` and `file_ids`, as a path, if there is one, and an id.
    pub async fn files_at_paths(
        &self,
        table: &Table,
        paths: &[String],
        file_ids: &[i64],
    ) -> Result<Vec<(Option<String>, i64)>> {
        let p = self.catalog.protocol();
        let session = self.catalog.session().await?;
        Ok(p.files_at_paths(&session, self.id(), table.id(), paths, file_ids)
            .await?)
    }

    /// The commit an idempotency key applied on this ref, if any (CMT-5).
    pub async fn commit_of_key(&self, app_id: &str) -> Result<Option<i64>> {
        let p = self.catalog.protocol();
        Ok(p.commit_of_key(&self.catalog.session().await?, self.id(), app_id)
            .await?)
    }

    /// The version an idempotency key reached on this ref, if any (CMT-5): where a writer that keeps its key goes on
    /// from.
    pub async fn key_version(&self, app_id: &str) -> Result<Option<i64>> {
        let p = self.catalog.protocol();
        Ok(p.key_version(&self.catalog.session().await?, self.id(), app_id).await?)
    }

    /// The namespaces this ref sees, in order.
    pub async fn namespaces(&self) -> Result<Vec<NamespaceRow>> {
        let p = self.catalog.protocol();
        Ok(p.namespaces(&self.catalog.session().await?, &self.name).await?)
    }

    /// This ref's own commits, the latest first: what it inherited is its parent's, up to its `fork_seq`.
    pub async fn history(&self, limit: i64) -> Result<Vec<CommitEntry>> {
        let p = self.catalog.protocol();
        Ok(p.history(&self.catalog.session().await?, &self.name, limit).await?)
    }

    /// The snapshots of `table` on this ref, the latest first.
    pub async fn snapshots(&self, table: &Table, limit: i64) -> Result<Vec<SnapshotEntry>> {
        let p = self.catalog.protocol();
        let session = self.catalog.session().await?;
        Ok(p.snapshots(&session, &self.name, table.id(), limit).await?)
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

    /// One sequence for the whole catalog: the one given, or a stable one taken now (VIS-11), for a reader that must
    /// name a single point. It takes a transaction id, and waits for every transaction open on the server. Reads do
    /// not need it: a table is read at its own latest snapshot, and tables that must agree through
    /// [`Catalog::latest`]. A read as of a time has a sequence per table, and no such point.
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

    /// A sequence no commit at or below it can still change (VIS-11): the last one drawn, once the transactions open
    /// when it was taken have ended. There is no limit to the wait, since past one the sequence could still change;
    /// a caller cancels it by dropping the future.
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
            Point::Seq(seq) => {
                let names = p.tables_at(&session, self.id, Some(seq)).await?;
                names.into_iter().map(|t| (t, seq)).collect()
            }
            Point::Latest => p
                .tables_latest(&session, &[self.id])
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
        })
    }

    /// The tables this ref sees at its read point.
    pub async fn tables(&self) -> Result<Vec<TableName>> {
        Ok(self.tables_seen().await?.into_iter().map(|(t, _)| t).collect())
    }

    /// A table by its name, as this ref sees it at its read point. At the latest or at a sequence, it is found by its
    /// name alone, so that its cost does not grow with the ref's other tables; as of a time, among those the ref saw.
    pub async fn table(&self, namespace: &[&str], name: &str) -> Result<Table> {
        let missing = || Error::NotFound(format!("table {}.{name} on {}", namespace.join("."), self.name));
        let p = self.catalog.protocol();
        // One session at a time: a read that held one while it asked for another would wait on the pool, and as many
        // reads at once as the pool has connections would each hold one and wait for ever.
        let (session, table_id, seq) = match self.at {
            Point::Time(_) => {
                let seen = self.tables_seen().await?;
                let found = seen
                    .into_iter()
                    .find(|(t, _)| t.namespace == namespace && t.name == name);
                let (table_id, seq) = found.map(|(t, seq)| (t.table_id, seq)).ok_or_else(missing)?;
                (self.catalog.session().await?, table_id, seq)
            }
            Point::Latest | Point::Seq(_) => {
                let at = match self.at {
                    Point::Seq(seq) => Some(seq),
                    _ => None,
                };
                let levels: Vec<String> = namespace.iter().map(|level| level.to_string()).collect();
                let session = self.catalog.session().await?;
                let (table_id, seq) = p
                    .table_named(&session, self.id, &levels, name, at)
                    .await?
                    .ok_or_else(missing)?;
                (session, table_id, seq)
            }
        };
        let definition = p
            .describe(&session, self.id, table_id, Some(seq))
            .await?
            .ok_or_else(|| Error::NotFound(format!("table {table_id} on {}", self.name)))?;
        Table::from_definition(self.id, seq, &definition)
    }

    /// The table `id` as this ref sees it now, which a follower of the feed knows by its id alone: its name here, and
    /// then the table of that name, which is the one a read by name opens. Not found if this ref does not see it, or
    /// if its name came to be another table's meanwhile.
    pub async fn table_with_id(&self, id: Uuid) -> Result<Table> {
        let missing = || Error::NotFound(format!("table {id} on {}", self.name));
        let p = self.catalog.protocol();
        let definition = p
            .describe(&self.catalog.session().await?, self.id, id, None)
            .await?
            .ok_or_else(missing)?;
        let namespace: Vec<&str> = definition["namespace"]
            .as_array()
            .ok_or_else(missing)?
            .iter()
            .filter_map(|level| level.as_str())
            .collect();
        let name = definition["name"].as_str().ok_or_else(missing)?;
        let table = self.table(&namespace, name).await?;
        if table.id() != id {
            return Err(missing());
        }
        Ok(table)
    }

    /// `table` of this ref, read at `seq`.
    pub async fn table_at(&self, table: &Table, seq: i64) -> Result<Table> {
        let p = self.catalog.protocol();
        let definition = p
            .describe(&self.catalog.session().await?, self.id, table.id(), Some(seq))
            .await?
            .ok_or_else(|| Error::NotFound(format!("table {} on {} at {seq}", table.name(), self.name)))?;
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

    /// This handle without its catalog.
    pub fn value(&self) -> RefValue {
        RefValue {
            id: self.id,
            name: self.name.clone(),
            kind: self.kind.clone(),
            at: self.at,
        }
    }

    /// The handle of `value` on `catalog`: the same ref, read at the same point. Nothing is asked of the catalog.
    pub fn from_value(catalog: &Catalog, value: RefValue) -> Self {
        Self {
            catalog: catalog.clone(),
            id: value.id,
            name: value.name,
            kind: value.kind,
            at: value.at,
        }
    }

    /// Whether this handle reads the ref's latest state, not a point of its past.
    pub fn is_latest(&self) -> bool {
        matches!(self.at, Point::Latest)
    }

    /// The time this handle reads the ref as of, if it reads it as of one.
    pub(crate) fn time(&self) -> Option<SystemTime> {
        match self.at {
            Point::Time(time) => Some(time),
            _ => None,
        }
    }

    /// The same ref, at its latest state.
    pub(crate) fn latest(&self) -> Self {
        Self {
            at: Point::Latest,
            ..self.clone()
        }
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

/// A ref without its catalog: which ref, and where it is read. What a handle is in a process that has a catalog of
/// its own, as `Ref::from_value` makes it there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValue {
    id: i32,
    name: String,
    kind: String,
    at: Point,
}

/// Where a read of a ref is: its latest state, a sequence, or a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Point {
    Latest,
    Seq(i64),
    Time(SystemTime),
}
