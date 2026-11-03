//! The records the protocol takes and answers (`sql/v0/02-types.sql`). A record sent is the SQL composite type,
//! field for field; a record read is the row a function returns. Ids keep the protocol's types (`int` refs, `uuid`
//! tables, `bigint` files and sequences); the core gives them names.

use postgres_types::ToSql;
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

/// The sequence a read at a ref's latest state passes (VIS-1).
pub const LATEST: i64 = 9_223_372_036_854_775_806;
/// The `end_seq` of a row no commit has closed.
pub const OPEN: i64 = i64::MAX;

/// A data file a commit adds, with its statistics (CMT-6, STA-1 to STA-5). `path` is null for an inline file, whose
/// bytes are in `content` (INL-1).
#[derive(Debug, Clone, PartialEq, ToSql)]
#[postgres(name = "file_add")]
pub struct FileAdd {
    pub table_id: Uuid,
    pub file_id: Option<i64>,
    pub path: Option<String>,
    pub spec_id: i32,
    pub partition: Value,
    pub sort_order_id: Option<i32>,
    pub record_count: i64,
    pub file_size: i64,
    pub footer_size: i32,
    pub first_row_id: Option<i64>,
    pub stat_field_ids: Vec<i32>,
    pub lower: Vec<Option<Vec<u8>>>,
    pub upper: Vec<Option<Vec<u8>>>,
    pub null_count: Vec<Option<i64>>,
    pub nan_count: Vec<Option<i64>>,
    pub value_count: Option<Vec<Option<i64>>>,
    pub content: Option<Vec<u8>>,
    pub footer: Option<Vec<u8>>,
}

/// A file a commit removes.
#[derive(Debug, Clone, PartialEq, ToSql)]
#[postgres(name = "file_ref")]
pub struct FileRef {
    pub table_id: Uuid,
    pub file_id: i64,
}

/// A deletion vector a commit or a merge sets on a file (DV-1 to DV-5): in a Puffin file at `path`, or inline.
#[derive(Debug, Clone, PartialEq, ToSql)]
#[postgres(name = "vector_set")]
pub struct VectorSet {
    pub table_id: Uuid,
    pub file_id: i64,
    pub base_seq: Option<i64>,
    pub path: Option<String>,
    pub content_offset: Option<i64>,
    pub content_size: i64,
    pub cardinality: i64,
    pub inline: Option<Vec<u8>>,
}

/// The snapshot a commit expects a table to be at, or none (CMT-7).
#[derive(Debug, Clone, PartialEq, ToSql)]
#[postgres(name = "table_expect")]
pub struct TableExpect {
    pub table_id: Uuid,
    pub snapshot_id: Option<i64>,
}

/// What a commit or a merge did: its sequence, and whether it was applied now or had been, under the same
/// idempotency key (CMT-8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitResult {
    pub commit_seq: i64,
    pub applied: bool,
}

/// A file a read opens, with its visible deletion vector (`plan`, VIS-1 to VIS-10, DV-1).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedFile {
    pub file_id: i64,
    pub path: Option<String>,
    pub spec_id: i32,
    pub partition: Value,
    pub record_count: i64,
    pub file_size: i64,
    pub footer_size: i32,
    pub first_row_id: Option<i64>,
    pub content: Option<Vec<u8>>,
    pub footer: Option<Vec<u8>>,
    pub vector: Option<PlannedVector>,
}

/// The deletion vector of a planned file.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedVector {
    pub begin_seq: i64,
    pub path: Option<String>,
    pub offset: Option<i64>,
    pub size: i64,
    pub cardinality: i64,
    pub inline: Option<Vec<u8>>,
}

/// A commit of the change feed (CHG-1).
#[derive(Debug, Clone, PartialEq)]
pub struct FeedCommit {
    pub commit_seq: i64,
    pub ref_id: i32,
    pub committed_at: std::time::SystemTime,
    pub operation: String,
}

/// A file that changed between two reads of a table (CHG-2): `added`, `removed`, or `vector` when its deletion vector
/// differs, with the vector at each read.
#[derive(Debug, Clone, PartialEq)]
pub struct FileChange {
    pub change: String,
    pub file_id: i64,
    pub path: Option<String>,
    pub vector_before: Option<Value>,
    pub vector_after: Option<Value>,
}

/// A stable read point (VIS-11): the sequence to read at, and the horizon after which it is repeatable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StablePoint {
    pub seq: i64,
    pub horizon: u64,
}

/// A table visible at a read: its id and its name there (TBL-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub table_id: Uuid,
    pub namespace: Vec<String>,
    pub name: String,
}

/// A table a ref saw as of a time, with the sequence to read it at (TT-3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAsOf {
    pub table_id: Uuid,
    pub namespace: Vec<String>,
    pub name: String,
    pub seq: i64,
}

/// A ref of the catalog, as the view `refs` shows it (BR-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefRow {
    pub ref_id: i32,
    pub name: String,
    /// `branch` or `tag`.
    pub kind: String,
    pub protected: bool,
    /// The parent's name, for a ref forked from another.
    pub parent: Option<String>,
    pub fork_seq: Option<i64>,
    pub created_seq: i64,
}

impl RefRow {
    pub(crate) fn from_row(row: &Row) -> Self {
        Self {
            ref_id: row.get("ref_id"),
            name: row.get("name"),
            kind: row.get("kind"),
            protected: row.get("protected"),
            parent: row.get("parent"),
            fork_seq: row.get("fork_seq"),
            created_seq: row.get("created_seq"),
        }
    }
}

impl PlannedFile {
    pub(crate) fn from_row(row: &Row) -> Self {
        let vector = row
            .get::<_, Option<i64>>("vector_begin_seq")
            .map(|begin_seq| PlannedVector {
                begin_seq,
                path: row.get("vector_path"),
                offset: row.get("vector_offset"),
                size: row.get("vector_size"),
                cardinality: row.get("vector_cardinality"),
                inline: row.get("vector_inline"),
            });
        Self {
            file_id: row.get("file_id"),
            path: row.get("path"),
            spec_id: row.get("spec_id"),
            partition: row.get("partition"),
            record_count: row.get("record_count"),
            file_size: row.get("file_size"),
            footer_size: row.get("footer_size"),
            first_row_id: row.get("first_row_id"),
            content: row.get("content"),
            footer: row.get("footer"),
            vector,
        }
    }
}

impl FeedCommit {
    pub(crate) fn from_row(row: &Row) -> Self {
        Self {
            commit_seq: row.get("commit_seq"),
            ref_id: row.get("ref_id"),
            committed_at: row.get("committed_at"),
            operation: row.get("operation"),
        }
    }
}

impl FileChange {
    pub(crate) fn from_row(row: &Row) -> Self {
        Self {
            change: row.get("change"),
            file_id: row.get("file_id"),
            path: row.get("path"),
            vector_before: row.get("vector_before"),
            vector_after: row.get("vector_after"),
        }
    }
}
