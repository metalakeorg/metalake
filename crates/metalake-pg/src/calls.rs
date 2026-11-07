//! One typed call for each function of the protocol's interface (ADR 0003 §2.4), on any [`Session`].
//!
//! A call's statement is qualified with the catalog's schema and passes every argument by name, with the protocol's
//! defaults written here, so that each function has one statement and a session prepares it once.

use std::time::{Duration, SystemTime};

use futures_util::{Stream, TryStreamExt};
use postgres_types::ToSql;
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

use crate::records::{
    CommitEntry, CommitResult, FeedCommit, FileAdd, FileChange, FileRef, LATEST, ListedFile, NamespaceRow, PlannedFile,
    RefRow, SnapshotEntry, StablePoint, TableAsOf, TableExpect, TableLatest, TableName, TableSnapshot, VectorSet,
};
use crate::{Result, Session, quote_ident};

type Param<'a> = &'a (dyn ToSql + Sync);

/// The protocol of one catalog: the schema its functions live in.
#[derive(Debug, Clone)]
pub struct Protocol {
    schema: String,
}

/// A commit (§2.4): every write of data, definitions and namespaces, of one or several tables.
#[derive(Debug, Clone, Default)]
pub struct Commit {
    pub ref_id: i32,
    pub operation: String,
    pub files: Vec<FileAdd>,
    pub removed: Vec<FileRef>,
    pub vectors: Vec<VectorSet>,
    pub expected: Vec<TableExpect>,
    /// Definition changes by table, in the reference's shape.
    pub changes: Option<Value>,
    pub author: Option<String>,
    pub actor: Option<String>,
    /// The idempotency key (CMT-8): the same `app_id` and `app_version` commit once.
    pub app_id: Option<String>,
    pub app_version: Option<i64>,
    pub message: Option<String>,
    pub definition: Option<String>,
    /// Namespace changes (NS-1 to NS-5), in the reference's shape.
    pub namespaces: Option<Value>,
    /// What the commit adds to each table's snapshot summary, by table (CMT-12).
    pub summaries: Option<Value>,
}

/// A merge of a branch, or of a selection of it, into its parent (MRG-1 to MRG-8).
#[derive(Debug, Clone)]
pub struct Merge {
    pub branch: i32,
    pub vectors: Vec<VectorSet>,
    pub author: String,
    pub message: Option<String>,
    pub only_tables: Option<Vec<Uuid>>,
    pub only_namespaces: Option<Value>,
    pub split: bool,
}

impl Merge {
    /// The whole branch, with the protocol's defaults.
    pub fn of(branch: i32) -> Self {
        Self {
            branch,
            vectors: Vec::new(),
            author: "metalake".into(),
            message: None,
            only_tables: None,
            only_namespaces: None,
            split: false,
        }
    }
}

/// A branch or a tag of `parent`, at `at` or its latest (BR-1 to BR-3).
#[derive(Debug, Clone)]
pub struct CreateRef {
    pub name: String,
    pub parent: i32,
    pub kind: String,
    pub at: Option<i64>,
    pub author: String,
}

impl CreateRef {
    pub fn branch(name: impl Into<String>, parent: i32) -> Self {
        Self {
            name: name.into(),
            parent,
            kind: "branch".into(),
            at: None,
            author: "metalake".into(),
        }
    }
}

/// What a read keeps out: files of `field_id` whose bounds miss `[lo, hi]`, in the encoding of STA-5.
#[derive(Debug, Clone, Default)]
pub struct Prune {
    pub field_id: i32,
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
}

/// What a ref keeps (RET-1).
#[derive(Debug, Clone, Default)]
pub struct Retention {
    pub min_seq: Option<i64>,
    pub max_age: Option<Duration>,
    pub min_snapshots: Option<i32>,
}

/// A sweep under way (RET-9): the objects `due`, locked by its transaction until [`Sweep::done`] or until it is
/// dropped, which leaves them all queued.
pub struct Sweep<'s> {
    protocol: &'s Protocol,
    tx: deadpool_postgres::Transaction<'s>,
    pub due: Vec<String>,
}

impl Sweep<'_> {
    /// Forgets the objects the caller deleted from storage, and answers how many.
    pub async fn done(self, deleted: &[String]) -> Result<i64> {
        let forgotten = self.protocol.sweep_done(&self.tx, deleted).await?;
        self.tx.commit().await?;
        Ok(forgotten)
    }
}

/// The default batch of `expire` and of the change feed.
pub const BATCH: i32 = 1000;

impl Protocol {
    /// The protocol of the catalog in `schema`.
    pub fn new(schema: &str) -> Self {
        Self {
            schema: quote_ident(schema),
        }
    }

    fn sql(&self, call: &str) -> String {
        format!("select * from {}.{call}", self.schema)
    }

    async fn all(&self, s: &impl Session, call: &str, params: &[Param<'_>]) -> Result<Vec<Row>> {
        let statement = s.statement(&self.sql(call)).await?;
        Ok(s.client().query(&statement, params).await?)
    }

    async fn one(&self, s: &impl Session, call: &str, params: &[Param<'_>]) -> Result<Row> {
        let statement = s.statement(&self.sql(call)).await?;
        Ok(s.client().query_one(&statement, params).await?)
    }

    // Writing data and definitions.

    pub async fn commit(&self, s: &impl Session, c: &Commit) -> Result<CommitResult> {
        let row = self
            .one(
                s,
                "commit(ref_id => $1, operation => $2, files => $3, removed => $4, vectors => $5, expected => $6, \
                 changes => $7, author => $8, actor => $9, app_id => $10, app_version => $11, message => $12, \
                 definition => $13, namespaces => $14, summaries => $15)",
                &[
                    &c.ref_id,
                    &c.operation,
                    &c.files,
                    &c.removed,
                    &c.vectors,
                    &c.expected,
                    &c.changes,
                    &c.author,
                    &c.actor,
                    &c.app_id,
                    &c.app_version,
                    &c.message,
                    &c.definition,
                    &c.namespaces,
                    &c.summaries,
                ],
            )
            .await?;
        Ok(CommitResult {
            commit_seq: row.get("commit_seq"),
            applied: row.get("applied"),
        })
    }

    /// Registers the paths a writer is about to upload (CMT-10). Run it on its own, before uploading.
    pub async fn register_intents(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        paths: &[String],
        ttl: Duration,
    ) -> Result<()> {
        self.all(
            s,
            "register_intents(ref_id => $1, table_id => $2, paths => $3, ttl => make_interval(secs => $4))",
            &[&ref_id, &table_id, &paths, &ttl.as_secs_f64()],
        )
        .await?;
        Ok(())
    }

    /// Draws a field, spec or sort-order id of a table (ID-5).
    pub async fn next_id(&self, s: &impl Session, table_id: Uuid, kind: &str) -> Result<i32> {
        Ok(self
            .one(s, "next_id(t => $1, kind => $2)", &[&table_id, &kind])
            .await?
            .get(0))
    }

    // Reading.

    /// The files a read of `table_id` on `ref_id` at `at` opens, pruned on one field, as they stream from Postgres.
    pub async fn plan<S: Session>(
        &self,
        s: &S,
        ref_id: i32,
        table_id: Uuid,
        at: Option<i64>,
        prune: Option<&Prune>,
    ) -> Result<impl Stream<Item = Result<PlannedFile>> + Send + use<S>> {
        let statement = s
            .statement(&self.sql("plan(ref_id => $1, table_id => $2, at => $3, field_id => $4, lo => $5, hi => $6)"))
            .await?;
        let at = at.unwrap_or(LATEST);
        let (field, lo, hi) = match prune {
            Some(p) => (Some(p.field_id), p.lo.clone(), p.hi.clone()),
            None => (None, None, None),
        };
        let params: [Param<'_>; 6] = [&ref_id, &table_id, &at, &field, &lo, &hi];
        let rows = s.client().query_raw(&statement, params).await?;
        Ok(rows.map_ok(|r| PlannedFile::from_row(&r)).map_err(Into::into))
    }

    /// The files a read of `table_id` on `ref_id` at `at` opens, each with its statistics, as they stream from Postgres.
    pub async fn listed_files<S: Session>(
        &self,
        s: &S,
        ref_id: i32,
        table_id: Uuid,
        at: Option<i64>,
    ) -> Result<impl Stream<Item = Result<ListedFile>> + Send + use<S>> {
        let statement = s
            .statement(&self.sql("listed_files(ref_id => $1, table_id => $2, at => $3)"))
            .await?;
        let at = at.unwrap_or(LATEST);
        let params: [Param<'_>; 3] = [&ref_id, &table_id, &at];
        let rows = s.client().query_raw(&statement, params).await?;
        Ok(rows.map_ok(|r| ListedFile::from_row(&r)).map_err(Into::into))
    }

    /// The visible files of `table_id` among `paths` and `file_ids`, each as its path, if it has one, and its id.
    pub async fn files_at_paths(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        paths: &[String],
        file_ids: &[i64],
    ) -> Result<Vec<(Option<String>, i64)>> {
        let statement = s
            .statement(&self.sql("files_at_paths(ref_id => $1, table_id => $2, paths => $3, file_ids => $4)"))
            .await?;
        let rows = s
            .client()
            .query(&statement, &[&ref_id, &table_id, &paths, &file_ids])
            .await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    /// The commit an idempotency key applied on `ref_id`, if any (CMT-5).
    pub async fn commit_of_key(&self, s: &impl Session, ref_id: i32, app_id: &str) -> Result<Option<i64>> {
        let statement = s
            .statement(&self.sql("commit_of_key(ref_id => $1, app_id => $2)"))
            .await?;
        let row = s.client().query_one(&statement, &[&ref_id, &app_id]).await?;
        Ok(row.get(0))
    }

    /// The refs of the catalog that are not deleted.
    pub async fn refs(&self, s: &impl Session) -> Result<Vec<RefRow>> {
        let statement = s
            .statement(&format!(
                "select ref_id, name, kind, protected, parent, fork_seq, created_seq from {}.refs order by ref_id",
                self.schema
            ))
            .await?;
        let rows = s.client().query(&statement, &[]).await?;
        Ok(rows.iter().map(RefRow::from_row).collect())
    }

    /// The namespaces of a ref, in order.
    pub async fn namespaces(&self, s: &impl Session, on: &str) -> Result<Vec<NamespaceRow>> {
        let statement = s
            .statement(&format!(
                "select namespace, properties from {}.namespaces where ref = $1 order by namespace",
                self.schema
            ))
            .await?;
        let rows = s.client().query(&statement, &[&on]).await?;
        Ok(rows
            .iter()
            .map(|r| NamespaceRow {
                namespace: r.get("namespace"),
                properties: r.get("properties"),
            })
            .collect())
    }

    /// The commits of a ref, the latest first.
    pub async fn history(&self, s: &impl Session, on: &str, limit: i64) -> Result<Vec<CommitEntry>> {
        let statement = s
            .statement(&format!(
                "select commit_seq, {UTC} as committed_at, author, operation, message from {}.history \
                 where ref = $1 order by commit_seq desc limit $2",
                self.schema
            ))
            .await?;
        let rows = s.client().query(&statement, &[&on, &limit]).await?;
        Ok(rows.iter().map(CommitEntry::from_row).collect())
    }

    /// The snapshots of a table on a ref, the latest first.
    pub async fn snapshots(&self, s: &impl Session, on: &str, table: Uuid, limit: i64) -> Result<Vec<SnapshotEntry>> {
        let statement = s
            .statement(&format!(
                "select snapshot_id, parent_snapshot_id, operation, {UTC} as committed_at from {}.snapshots \
                 where ref = $1 and table_id = $2 order by snapshot_id desc limit $3",
                self.schema
            ))
            .await?;
        let rows = s.client().query(&statement, &[&on, &table, &limit]).await?;
        Ok(rows.iter().map(SnapshotEntry::from_row).collect())
    }

    /// Every snapshot of a table, of any ref, the earliest first.
    pub async fn table_snapshots(&self, s: &impl Session, table: Uuid) -> Result<Vec<TableSnapshot>> {
        let statement = s
            .statement(&format!(
                "select snapshot_id, parent_snapshot_id, ref, schema_seq, summary, \
                 (extract(epoch from committed_at) * 1000)::bigint as timestamp_ms from {}.snapshots \
                 where table_id = $1 order by snapshot_id",
                self.schema
            ))
            .await?;
        let rows = s.client().query(&statement, &[&table]).await?;
        Ok(rows
            .iter()
            .map(|r| TableSnapshot {
                snapshot_id: r.get("snapshot_id"),
                parent_snapshot_id: r.get("parent_snapshot_id"),
                ref_name: r.get("ref"),
                schema_seq: r.get("schema_seq"),
                summary: r.get("summary"),
                timestamp_ms: r.get("timestamp_ms"),
            })
            .collect())
    }

    /// The tables visible at a read (TBL-1).
    pub async fn tables_at(&self, s: &impl Session, ref_id: i32, at: Option<i64>) -> Result<Vec<TableName>> {
        let at = at.unwrap_or(LATEST);
        let rows = self
            .all(s, "tables_at(ref_id => $1, at => $2)", &[&ref_id, &at])
            .await?;
        Ok(rows
            .iter()
            .map(|r| TableName {
                table_id: r.get("table_id"),
                namespace: r.get("namespace"),
                name: r.get("name"),
            })
            .collect())
    }

    /// A table's definition at a read, in the reference's shape, or none if the read does not see it: its name and
    /// location, snapshot, fields, partition specs and sort orders (the default's id and all it has had), cluster
    /// key and properties.
    pub async fn describe(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        at: Option<i64>,
    ) -> Result<Option<Value>> {
        let at = at.unwrap_or(LATEST);
        Ok(self
            .one(
                s,
                "describe(ref_id => $1, table_id => $2, at => $3) as d",
                &[&ref_id, &table_id, &at],
            )
            .await?
            .get("d"))
    }

    /// The snapshot of a table a read at `at` sees (CMT-2).
    pub async fn snapshot_at(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        at: Option<i64>,
    ) -> Result<Option<i64>> {
        let at = at.unwrap_or(LATEST);
        Ok(self
            .one(s, "snapshot_at(r => $1, t => $2, s => $3)", &[&ref_id, &table_id, &at])
            .await?
            .get(0))
    }

    /// The sequence of a read of a table as of a time (TT-3).
    pub async fn as_of_time(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        at: SystemTime,
    ) -> Result<Option<i64>> {
        Ok(self
            .one(s, "as_of_time(r => $1, t => $2, at => $3)", &[&ref_id, &table_id, &at])
            .await?
            .get(0))
    }

    /// The tables a ref saw as of a time, each with the sequence to read it at and its name then (TT-3).
    /// The tables of `ref_ids` now, each with the sequence to read it at, seen in one statement: a commit of several
    /// tables is in the sequence of each or of none.
    pub async fn tables_latest(&self, s: &impl Session, ref_ids: &[i32]) -> Result<Vec<TableLatest>> {
        let rows = self.all(s, "tables_latest(ref_ids => $1)", &[&ref_ids]).await?;
        Ok(rows
            .iter()
            .map(|r| TableLatest {
                ref_id: r.get("ref_id"),
                table_id: r.get("table_id"),
                namespace: r.get("namespace"),
                name: r.get("name"),
                seq: r.get("seq"),
            })
            .collect())
    }

    pub async fn tables_as_of(&self, s: &impl Session, ref_id: i32, at: SystemTime) -> Result<Vec<TableAsOf>> {
        let rows = self
            .all(s, "tables_as_of(ref_id => $1, at => $2)", &[&ref_id, &at])
            .await?;
        Ok(rows
            .iter()
            .map(|r| TableAsOf {
                table_id: r.get("table_id"),
                namespace: r.get("namespace"),
                name: r.get("name"),
                seq: r.get("seq"),
            })
            .collect())
    }

    /// A sequence to read at, and the horizon after which reading at it is repeatable (VIS-11).
    pub async fn stable_point(&self, s: &impl Session) -> Result<StablePoint> {
        let statement = s
            .statement(&format!(
                "select seq, horizon::text::bigint as horizon from {}.stable_point()",
                self.schema
            ))
            .await?;
        let row = s.client().query_one(&statement, &[]).await?;
        Ok(StablePoint {
            seq: row.get("seq"),
            horizon: row.get::<_, i64>("horizon") as u64,
        })
    }

    pub async fn is_stable(&self, s: &impl Session, horizon: u64) -> Result<bool> {
        let horizon = horizon as i64;
        Ok(self
            .one(s, "is_stable(horizon => $1::text::xid8)", &[&horizon.to_string()])
            .await?
            .get(0))
    }

    /// The change feed after a cursor, or from the start, of the catalog or of one table (CHG-1).
    pub async fn changes(
        &self,
        s: &impl Session,
        after: Option<(SystemTime, i64)>,
        lim: Option<i32>,
        table_id: Option<Uuid>,
    ) -> Result<Vec<FeedCommit>> {
        let (after_at, after_seq) = after.unzip();
        let lim = lim.unwrap_or(BATCH);
        let rows = self
            .all(
                s,
                "changes(after_at => coalesce($1::timestamptz, '-infinity'), after_seq => $2::bigint, \
                 lim => $3::int, tab => $4::uuid)",
                &[&after_at, &after_seq, &lim, &table_id],
            )
            .await?;
        Ok(rows.iter().map(FeedCommit::from_row).collect())
    }

    /// The files added, removed and whose vector changed between two retained reads of a table (CHG-2).
    pub async fn table_changes(
        &self,
        s: &impl Session,
        ref_id: i32,
        table_id: Uuid,
        a: i64,
        b: i64,
    ) -> Result<Vec<FileChange>> {
        let rows = self
            .all(
                s,
                "table_changes(r => $1, t => $2, a => $3, b => $4)",
                &[&ref_id, &table_id, &a, &b],
            )
            .await?;
        Ok(rows.iter().map(FileChange::from_row).collect())
    }

    /// A commit's time (TT-1).
    pub async fn commit_time(&self, s: &impl Session, seq: i64) -> Result<SystemTime> {
        Ok(self.one(s, "commit_time(seq => $1)", &[&seq]).await?.get(0))
    }

    // Branches and tags.

    pub async fn create_ref(&self, s: &impl Session, r: &CreateRef) -> Result<i32> {
        Ok(self
            .one(
                s,
                "create_ref(name => $1, parent => $2, kind => $3, at => $4, author => $5)",
                &[&r.name, &r.parent, &r.kind, &r.at, &r.author],
            )
            .await?
            .get(0))
    }

    pub async fn delete_ref(&self, s: &impl Session, ref_id: i32) -> Result<()> {
        self.all(s, "delete_ref(ref_id => $1)", &[&ref_id]).await?;
        Ok(())
    }

    /// Merges a branch, or a selection of it, into its parent; none when it has nothing to merge.
    pub async fn merge(&self, s: &impl Session, m: &Merge) -> Result<Option<CommitResult>> {
        let row = self
            .one(
                s,
                "merge(branch => $1, vectors => $2, author => $3, message => $4, only_tables => $5, \
                 only_namespaces => $6, split => $7)",
                &[
                    &m.branch,
                    &m.vectors,
                    &m.author,
                    &m.message,
                    &m.only_tables,
                    &m.only_namespaces,
                    &m.split,
                ],
            )
            .await?;
        let applied = row.get("applied");
        Ok(row
            .get::<_, Option<i64>>("commit_seq")
            .map(|commit_seq| CommitResult { commit_seq, applied }))
    }

    /// What the same merge would do, or why it would conflict, without doing it. Its author and message are unused.
    pub async fn merge_report(&self, s: &impl Session, m: &Merge) -> Result<Value> {
        Ok(self
            .one(
                s,
                "merge_report(branch => $1, vectors => $2, only_tables => $3, only_namespaces => $4, split => $5)",
                &[&m.branch, &m.vectors, &m.only_tables, &m.only_namespaces, &m.split],
            )
            .await?
            .get(0))
    }

    // Retention and collection.

    pub async fn set_retention(&self, s: &impl Session, ref_id: i32, r: &Retention) -> Result<()> {
        let max_age = r.max_age.map(|d| d.as_secs_f64());
        self.all(
            s,
            "set_retention(r => $1, min_seq => $2, max_age => make_interval(secs => $3), min_snapshots => $4)",
            &[&ref_id, &r.min_seq, &max_age, &r.min_snapshots],
        )
        .await?;
        Ok(())
    }

    /// How long a queued object waits before a sweep may delete it (RET-9).
    pub async fn set_collect_grace(&self, s: &impl Session, grace: Duration) -> Result<()> {
        let grace = grace.as_secs_f64();
        self.all(s, "set_collect_grace(grace => make_interval(secs => $1))", &[&grace])
            .await?;
        Ok(())
    }

    /// Deletes up to `lim` rows no retained read reaches and queues their objects (RET-4); call it until it answers 0.
    pub async fn expire(&self, s: &impl Session, lim: Option<i32>) -> Result<i64> {
        let lim = lim.unwrap_or(BATCH);
        Ok(self.one(s, "expire(lim => $1)", &[&lim]).await?.get(0))
    }

    /// The queued objects past the grace (RET-9), locked for the caller's transaction: delete them from storage, then
    /// call [`Protocol::sweep_done`] in the same transaction.
    pub async fn sweep_due(&self, tx: &impl Session, lim: Option<i32>) -> Result<Vec<String>> {
        let rows = self.all(tx, "sweep_due(lim => $1)", &[&lim]).await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    pub async fn sweep_done(&self, tx: &impl Session, paths: &[String]) -> Result<i64> {
        Ok(self.one(tx, "sweep_done(paths => $1)", &[&paths]).await?.get(0))
    }

    /// Starts a sweep of up to `lim` objects past the grace, in a transaction of its own.
    pub async fn sweep<'s>(&'s self, s: &'s mut crate::PooledSession, lim: Option<i32>) -> Result<Sweep<'s>> {
        let tx = s.transaction().await?;
        let due = self.sweep_due(&tx, lim).await?;
        Ok(Sweep {
            protocol: self,
            tx,
            due,
        })
    }

    /// The expired intents whose file never went in (RET-7), as of `at` or now.
    pub async fn orphans(&self, s: &impl Session, at: Option<SystemTime>) -> Result<Vec<String>> {
        let rows = self.all(s, "orphans(at => coalesce($1, now()))", &[&at]).await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    pub async fn forget_intents(&self, s: &impl Session, paths: &[String]) -> Result<i64> {
        Ok(self.one(s, "forget_intents(paths => $1)", &[&paths]).await?.get(0))
    }

    /// Makes expired intents of the paths nothing knows, on a table and ref, and answers how many (RET-7b).
    pub async fn adopt_orphans(&self, s: &impl Session, ref_id: i32, table_id: Uuid, paths: &[String]) -> Result<i64> {
        let call = "adopt_orphans(ref_id => $1, table_id => $2, paths => $3)";
        Ok(self.one(s, call, &[&ref_id, &table_id, &paths]).await?.get(0))
    }

    /// Whether a table's inline files on a ref are due a flush (INL-2).
    pub async fn inline_flush_due(&self, s: &impl Session, r: i32, t: Uuid) -> Result<bool> {
        Ok(self
            .one(s, "inline_flush_due(r => $1, t => $2)", &[&r, &t])
            .await?
            .get(0))
    }

    /// Copies commit times, oldest first (TT-1).
    pub async fn fill_committed_at(&self, s: &impl Session, lim: Option<i32>) -> Result<i64> {
        Ok(self.one(s, "fill_committed_at(lim => $1)", &[&lim]).await?.get(0))
    }
}

/// The names of the interface's functions and the calls that make them, for the test that holds them to `pg_proc`:
/// (function, its parameters in order, whether only a writer may call it).
/// A view's `committed_at`, as text, whatever the session's time zone.
const UTC: &str = r#"to_char(committed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')"#;

pub const INTERFACE: &[(&str, &[&str], bool)] = &[
    (
        "commit",
        &[
            "ref_id",
            "operation",
            "files",
            "removed",
            "vectors",
            "expected",
            "changes",
            "author",
            "actor",
            "app_id",
            "app_version",
            "message",
            "definition",
            "namespaces",
            "summaries",
        ],
        true,
    ),
    ("register_intents", &["ref_id", "table_id", "paths", "ttl"], true),
    ("next_id", &["t", "kind"], true),
    ("plan", &["ref_id", "table_id", "at", "field_id", "lo", "hi"], false),
    ("listed_files", &["ref_id", "table_id", "at"], false),
    ("files_at_paths", &["ref_id", "table_id", "paths", "file_ids"], false),
    ("commit_of_key", &["ref_id", "app_id"], false),
    ("tables_at", &["ref_id", "at"], false),
    ("describe", &["ref_id", "table_id", "at"], false),
    ("snapshot_at", &["r", "t", "s"], false),
    ("as_of_time", &["r", "t", "at"], false),
    ("tables_as_of", &["ref_id", "at"], false),
    ("tables_latest", &["ref_ids"], false),
    ("stable_point", &[], false),
    ("is_stable", &["horizon"], false),
    ("changes", &["after_at", "after_seq", "lim", "tab"], false),
    ("table_changes", &["r", "t", "a", "b"], false),
    ("commit_time", &["seq"], false),
    ("create_ref", &["name", "parent", "kind", "at", "author"], true),
    ("delete_ref", &["ref_id"], true),
    (
        "merge",
        &[
            "branch",
            "vectors",
            "author",
            "message",
            "only_tables",
            "only_namespaces",
            "split",
        ],
        true,
    ),
    (
        "merge_report",
        &["branch", "vectors", "only_tables", "only_namespaces", "split"],
        true,
    ),
    ("set_retention", &["r", "min_seq", "max_age", "min_snapshots"], true),
    ("set_collect_grace", &["grace"], true),
    ("expire", &["lim"], true),
    ("sweep_due", &["lim"], false),
    ("sweep_done", &["paths"], true),
    ("orphans", &["at"], false),
    ("forget_intents", &["paths"], true),
    ("adopt_orphans", &["ref_id", "table_id", "paths"], true),
    ("fill_committed_at", &["lim"], true),
    ("inline_flush_due", &["r", "t"], false),
];
