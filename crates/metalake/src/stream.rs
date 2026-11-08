//! Following a table (CHG-3, CHG-4): what a table holds, then what each commit changes, as rows inserted and deleted.
//!
//! It is a library, as a scan is: no thread and no timer of its own. [`TableStream::next`] says what is new, as tasks an
//! engine may hand to its workers, and a cursor. The caller keeps the cursor, and a stream opened from it goes on from
//! there, so that a reader that keeps its cursor with what it did with the rows takes each commit once.
//!
//! The rows are the table's columns and three more: [`CHANGE_TYPE`], [`COMMIT_SEQ`] and [`COMMIT_TIMESTAMP`].

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use metalake_pg::records::CommitChange;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::{Catalog, Ref};
use crate::error::{Error, Result};
use crate::scan::{Scan, ScanTask, TaskVector, vector};
use crate::storage::Storage;
use crate::table::Table;
use crate::vectors::DeletionVector;

/// The column that says what happened to a row: `insert` or `delete`.
pub const CHANGE_TYPE: &str = "_change_type";
/// The column that says which commit did it. A row of the first read has that read's sequence.
pub const COMMIT_SEQ: &str = "_commit_seq";
/// The column that says when that commit was made (TT-1), in microseconds, as Postgres keeps it. A row of the first read
/// has the time of that read.
pub const COMMIT_TIMESTAMP: &str = "_commit_timestamp";

/// Commits a step looks at, at most.
const FEED_BATCH: i32 = 1000;

/// Where a reader starts.
#[derive(Debug, Clone, PartialEq)]
pub enum Start {
    /// What the table holds now, then what comes.
    Snapshot,
    /// Only what comes.
    Latest,
    /// Where a reader was.
    Cursor(StreamCursor),
}

/// All a reader's state: its place in the feed, and the read it started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCursor {
    at: SystemTime,
    seq: i64,
    floor: i64,
}

/// What happened to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowChange {
    Insert,
    Delete,
}

impl RowChange {
    fn name(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Delete => "delete",
        }
    }
}

/// Which rows of a task's file it delivers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskRows {
    /// Those its vector leaves.
    Live,
    /// Those its vector holds and this one, the vector before, does not.
    DeletedSince(Option<TaskVector>),
}

/// One file's part of a step: it travels, for an engine to hand to a worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamTask {
    pub seq: i64,
    pub committed_at: SystemTime,
    pub change: RowChange,
    pub rows: TaskRows,
    pub file: ScanTask,
}

/// What a step found: every file of the commits it took, and the cursor past them. A reader keeps the cursor once it dealt
/// with all of its tasks.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamStep {
    pub tasks: Vec<StreamTask>,
    pub cursor: StreamCursor,
}

/// A reader of a table's changes on a ref.
#[derive(Debug, Clone)]
pub struct TableStream {
    catalog: Catalog,
    branch: Ref,
    table: Table,
    scan: Scan,
    schema: SchemaRef,
    schema_seq: i64,
    start: Start,
    cursor: Option<StreamCursor>,
}

impl Ref {
    /// A reader of `table` on this ref. A ref with a parent is refused: a merge changes what it inherits with no snapshot
    /// of it, so the feed would not say (CHG-3).
    pub async fn stream(&self, table: &Table, start: Start) -> Result<TableStream> {
        self.writable()?;
        let catalog = self.catalog();
        let refs = catalog.describe_refs().await?;
        if refs.iter().any(|r| r.ref_id == self.id() && r.parent.is_some()) {
            return Err(Error::Unsupported(format!(
                "following {}: a merge would change what it inherits with no snapshot of it",
                self.name()
            )));
        }
        let head = table
            .snapshot_id()
            .ok_or_else(|| Error::Invalid(format!("{} has no snapshot to follow from", table.name())))?;
        let schema_seq = schema_seq(catalog, table, head).await?;
        let scan = Scan::new(catalog, table);
        let mut fields = scan.schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new(CHANGE_TYPE, DataType::Utf8, false)));
        fields.push(Arc::new(ArrowField::new(COMMIT_SEQ, DataType::Int64, false)));
        fields.push(Arc::new(ArrowField::new(
            COMMIT_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )));
        let cursor = match &start {
            Start::Cursor(cursor) => Some(cursor.clone()),
            _ => None,
        };
        Ok(TableStream {
            catalog: catalog.clone(),
            branch: self.clone(),
            table: table.clone(),
            scan,
            schema: Arc::new(Schema::new(fields)),
            schema_seq,
            start,
            cursor,
        })
    }
}

impl TableStream {
    /// What its rows look like.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Where the reader is, once it took a step.
    pub fn cursor(&self) -> Option<&StreamCursor> {
        self.cursor.as_ref()
    }

    /// What is new, or nothing yet. It does not wait: a caller asks again when it wants to.
    pub async fn next(&mut self) -> Result<Option<StreamStep>> {
        match self.cursor.clone() {
            None => self.begin().await.map(Some),
            Some(cursor) => self.tail(cursor).await,
        }
    }

    /// The batches of one of a step's tasks.
    pub fn open(&self, task: StreamTask, storage: &Storage) -> BoxStream<'static, Result<RecordBatch>> {
        let (scan, schema, storage) = (self.scan.clone(), self.schema.clone(), storage.clone());
        let StreamTask {
            seq,
            committed_at,
            change,
            rows,
            file,
        } = task;
        stream::once(async move {
            let batches = match rows {
                TaskRows::Live => scan.open(file, &storage),
                TaskRows::DeletedSince(before) => {
                    let rows = deleted_since(&file, before.as_ref(), &storage).await?;
                    scan.open_only(ScanTask { vector: None, ..file }, rows, &storage)
                }
            };
            Ok::<_, Error>(batches)
        })
        .try_flatten()
        .map(move |batch| with_change(&schema, &batch?, change, seq, committed_at))
        .boxed()
    }

    /// CHG-4: the read at a stream point, and a cursor for the feed from the time taken before it.
    async fn begin(&mut self) -> Result<StreamStep> {
        let (at, seq) = self.point().await?;
        let cursor = StreamCursor { at, seq: 0, floor: seq };
        let tasks = match self.start {
            Start::Snapshot => {
                let read = self.branch.table_at(&self.table, seq).await?;
                let files: Vec<ScanTask> = Scan::new(&self.catalog, &read).plan().await?.try_collect().await?;
                let task = |file| StreamTask {
                    seq,
                    committed_at: at,
                    change: RowChange::Insert,
                    rows: TaskRows::Live,
                    file,
                };
                files.into_iter().map(task).collect()
            }
            _ => Vec::new(),
        };
        self.cursor = Some(cursor.clone());
        Ok(StreamStep { tasks, cursor })
    }

    /// A stream point, once its sequence is stable (VIS-11).
    async fn point(&self) -> Result<(SystemTime, i64)> {
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        let point = p.stream_point(&session).await?;
        let mut pause = Duration::from_millis(1);
        while !p.is_stable(&session, point.horizon).await? {
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_millis(50));
        }
        Ok((point.at, point.seq))
    }

    /// The commits after the cursor, each one whole, as a step of its tasks.
    async fn tail(&mut self, mut cursor: StreamCursor) -> Result<Option<StreamStep>> {
        let session = self.catalog.session().await?;
        let p = self.catalog.protocol();
        let (id, table_id) = (self.branch.id(), self.table.id());
        let feed = p
            .changes(
                &session,
                Some((cursor.at, cursor.seq)),
                Some(FEED_BATCH),
                Some(table_id),
            )
            .await?;
        if feed.is_empty() {
            return Ok(None);
        }

        // CHG-1: the feed says nothing of what collection took, so a reader that fell behind asks.
        let position = cursor.seq.max(cursor.floor);
        let floor = p.retention_floor(&session, id).await?;
        if position < floor {
            let current = p.snapshot_at(&session, id, table_id, Some(floor)).await?;
            if current.is_some_and(|s| s > position) {
                return Err(Error::Invalid(format!(
                    "following {}: the reader is at {position} and {} keeps from {floor}",
                    self.table.name(),
                    self.branch.name()
                )));
            }
        }

        let ours = |seq: i64, ref_id: i32| ref_id == id && seq > cursor.floor;
        let last = feed.iter().rev().find(|c| ours(c.commit_seq, c.ref_id));
        if let Some(last) = last
            && schema_seq(&self.catalog, &self.table, last.commit_seq).await? > self.schema_seq
        {
            return Err(Error::Unsupported(format!(
                "{} changed its definition: open the stream again, from its cursor",
                self.table.name()
            )));
        }

        let mut tasks = Vec::new();
        for commit in feed {
            cursor.at = commit.committed_at;
            cursor.seq = commit.commit_seq;
            // CMT-11: a replace changes no row.
            if commit.ref_id != id || commit.commit_seq <= cursor.floor || commit.operation == "replace" {
                continue;
            }
            let changes = p.commit_changes(&session, id, table_id, commit.commit_seq).await?;
            for change in changes {
                tasks.push(task_of(commit.commit_seq, commit.committed_at, change)?);
            }
        }
        self.cursor = Some(cursor.clone());
        Ok(Some(StreamStep { tasks, cursor }))
    }
}

/// The definition a snapshot of `table` was written under.
async fn schema_seq(catalog: &Catalog, table: &Table, snapshot: i64) -> Result<i64> {
    let found = catalog.ancestors(table, &[snapshot], 1, None).await?;
    let found = found.last().map(|s| s.schema_seq);
    found.ok_or_else(|| Error::Corrupt(format!("{} has no snapshot {snapshot}", table.name())))
}

/// What a file's change delivers: an added file's live rows; a removed file's rows that were live, by the vector it had;
/// and the rows a new vector took from a file that stays.
fn task_of(seq: i64, committed_at: SystemTime, c: CommitChange) -> Result<StreamTask> {
    let (before, after) = (optional_vector(&c.vector_before)?, optional_vector(&c.vector_after)?);
    let (change, rows, vector) = match c.change.as_str() {
        "added" => (RowChange::Insert, TaskRows::Live, after),
        "removed" => (RowChange::Delete, TaskRows::Live, before),
        "vector" => (RowChange::Delete, TaskRows::DeletedSince(before), after),
        other => return Err(Error::Corrupt(format!("a commit's change is {other}"))),
    };
    let file = ScanTask {
        file_id: c.file_id,
        path: c.path,
        content: c.content,
        file_size: c.file_size,
        footer_size: c.footer_size,
        footer: c.footer,
        record_count: c.record_count,
        spec_id: c.spec_id,
        partition: c.partition,
        vector,
    };
    Ok(StreamTask {
        seq,
        committed_at,
        change,
        rows,
        file,
    })
}

fn optional_vector(value: &Option<Value>) -> Result<Option<TaskVector>> {
    value.as_ref().map(vector_of).transpose()
}

/// A vector as CHG-2 gives it, in JSON.
fn vector_of(v: &Value) -> Result<TaskVector> {
    let number = |key: &str| v.get(key).and_then(Value::as_i64);
    let needed = |key: &str| number(key).ok_or_else(|| Error::Corrupt(format!("a vector without {key}: {v}")));
    let inline = v.get("inline").and_then(Value::as_str).map(hex).transpose()?;
    Ok(TaskVector {
        begin_seq: 0,
        path: v.get("path").and_then(Value::as_str).map(str::to_string),
        offset: number("content_offset"),
        size: needed("content_size")?,
        cardinality: needed("cardinality")?,
        inline,
    })
}

/// The bytes of a `bytea` as JSON writes it: `\x` and two digits for each.
fn hex(text: &str) -> Result<Vec<u8>> {
    let corrupt = || Error::Corrupt(format!("not bytes: {text}"));
    let digits = text.strip_prefix("\\x").ok_or_else(corrupt)?;
    if !digits.len().is_multiple_of(2) || !digits.is_ascii() {
        return Err(corrupt());
    }
    (0..digits.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).map_err(|_| corrupt()))
        .collect()
}

/// The rows the file's vector holds and `before` does not.
async fn deleted_since(file: &ScanTask, before: Option<&TaskVector>, storage: &Storage) -> Result<DeletionVector> {
    let after = file.vector(storage).await?.unwrap_or_default();
    let before = vector(file.file_id, before, storage).await?.unwrap_or_default();
    Ok(DeletionVector::from_positions(
        after.positions().filter(|p| !before.contains(*p)),
    ))
}

fn with_change(
    schema: &SchemaRef,
    batch: &RecordBatch,
    change: RowChange,
    seq: i64,
    committed_at: SystemTime,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let micros = committed_at
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .ok_or_else(|| Error::Corrupt(format!("commit {seq} has a time before 1970")))?;
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![change.name(); rows])));
    columns.push(Arc::new(Int64Array::from(vec![seq; rows])));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![micros; rows]).with_timezone("UTC"),
    ));
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Schema(e.to_string()))
}
