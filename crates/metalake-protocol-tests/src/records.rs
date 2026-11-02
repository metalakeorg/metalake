//! The records of the protocol's calls (`sql/v0/02-types.sql`), as the core will send them.

use postgres_types::ToSql;
use uuid::Uuid;

#[derive(Debug, Clone, ToSql)]
#[postgres(name = "file_add")]
pub struct FileAdd {
    pub table_id: Uuid,
    pub file_id: Option<i64>,
    pub path: Option<String>,
    pub spec_id: i32,
    pub partition: serde_json::Value,
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

#[derive(Debug, Clone, ToSql)]
#[postgres(name = "file_ref")]
pub struct FileRef {
    pub table_id: Uuid,
    pub file_id: i64,
}

#[derive(Debug, Clone, ToSql)]
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

#[derive(Debug, Clone, ToSql)]
#[postgres(name = "table_expect")]
pub struct TableExpect {
    pub table_id: Uuid,
    pub snapshot_id: Option<i64>,
}

/// The bound encoding of a `long` (STA-5).
pub fn encode_long(v: i64) -> Vec<u8> {
    ((v as u64) ^ (1 << 63)).to_be_bytes().to_vec()
}

/// A stored file with one statistics field, the long `field`, holding `[lo, hi]`.
pub fn stored_file(table_id: Uuid, path: String, records: i64, field: i32, lo: i64, hi: i64) -> FileAdd {
    FileAdd {
        table_id,
        file_id: None,
        path: Some(path),
        spec_id: 0,
        partition: serde_json::json!([]),
        sort_order_id: None,
        record_count: records,
        file_size: 1_000_000,
        footer_size: 10_000,
        first_row_id: None,
        stat_field_ids: vec![field],
        lower: vec![Some(encode_long(lo))],
        upper: vec![Some(encode_long(hi))],
        null_count: vec![Some(0)],
        nan_count: vec![None],
        value_count: None,
        content: None,
        footer: None,
    }
}
