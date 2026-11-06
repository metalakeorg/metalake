//! A deletion vector as the position delete file a client of format 2 reads (ICE-4).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use metalake::{Error, Result};
use parquet::arrow::ArrowWriter;

/// The ids Iceberg reserves for a position delete file's columns.
const FILE_PATH: i32 = 2147483546;
const POS: i32 = 2147483545;

fn column(name: &str, ty: DataType, id: i32) -> Field {
    let metadata = HashMap::from([(metalake::schema::FIELD_ID.to_string(), id.to_string())]);
    Field::new(name, ty, false).with_metadata(metadata)
}

/// A Parquet file of one row per position, in order, of the data file at `path`.
pub fn position_deletes(path: &str, positions: impl Iterator<Item = u64>) -> Result<Vec<u8>> {
    let positions: Vec<i64> = positions.map(|p| p as i64).collect();
    let paths = vec![path; positions.len()];
    let schema = Arc::new(Schema::new(vec![
        column("file_path", DataType::Utf8, FILE_PATH),
        column("pos", DataType::Int64, POS),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(paths)),
        Arc::new(Int64Array::from(positions)),
    ];
    let failed = |e: &dyn std::fmt::Display| Error::Corrupt(format!("a delete file could not be written: {e}"));
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| failed(&e))?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).map_err(|e| failed(&e))?;
    writer.write(&batch).map_err(|e| failed(&e))?;
    writer.close().map_err(|e| failed(&e))?;
    Ok(bytes)
}
