//! The records of the protocol's calls, the SDK's, and the helpers the tests build them with.

pub use metalake_pg::records::{FileAdd, FileRef, TableExpect, VectorSet};
use uuid::Uuid;

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
