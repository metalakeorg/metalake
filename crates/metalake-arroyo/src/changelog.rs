//! A sink's rows as Arroyo gives them to an updating sink: Debezium's envelope, the row in `after` with `op = c` when
//! inserted, in `before` with `op = d` when deleted, and in both with `op = u` when updated. The sink takes them apart
//! into the rows to insert and the rows to delete, each with the declared columns.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Fields, Schema};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ChangelogError {
    #[error("Arroyo gave a change '{0}', neither an insert, an update nor a delete")]
    UnknownChange(String),
    #[error("{0}")]
    Internal(String),
}

impl From<arrow_schema::ArrowError> for ChangelogError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Internal(e.to_string())
    }
}

/// The rows of an epoch's batch, apart: those to insert and those to delete.
#[derive(Debug)]
pub struct Changes {
    pub inserts: RecordBatch,
    pub deletes: RecordBatch,
}

/// The declared columns of an updating sink's input `schema`: the fields of its `after` struct.
pub fn declared(schema: &Schema) -> Result<Fields, ChangelogError> {
    match schema.field_with_name("after").map(|f| f.data_type()) {
        Ok(DataType::Struct(fields)) => Ok(fields.clone()),
        _ => Err(internal("the sink's input is not an updating table's")),
    }
}

/// `batch`, in Debezium's envelope, apart: an insert is its `after`, a delete its `before`, an update both.
pub fn changes(batch: &RecordBatch) -> Result<Changes, ChangelogError> {
    let op = column(batch, "op")?
        .as_string_opt::<i32>()
        .ok_or_else(|| internal("op is not Utf8"))?;
    let mut inserted = Vec::with_capacity(op.len());
    let mut deleted = Vec::with_capacity(op.len());
    for change in op.iter() {
        let (insert, delete) = match change {
            Some("c" | "r") => (true, false),
            Some("u") => (true, true),
            Some("d") => (false, true),
            other => return Err(ChangelogError::UnknownChange(other.unwrap_or("null").to_string())),
        };
        inserted.push(insert);
        deleted.push(delete);
    }
    Ok(Changes {
        inserts: rows(column(batch, "after")?, &BooleanArray::from(inserted))?,
        deletes: rows(column(batch, "before")?, &BooleanArray::from(deleted))?,
    })
}

/// The rows of `side`, a struct of the declared columns, where `chosen`.
fn rows(side: &ArrayRef, chosen: &BooleanArray) -> Result<RecordBatch, ChangelogError> {
    let side: &StructArray = side
        .as_struct_opt()
        .ok_or_else(|| internal("before and after are not structs"))?;
    // filtered before the batch is built: a column NOT NULL holds nulls where its side is absent
    let columns = side
        .columns()
        .iter()
        .map(|c| arrow_select::filter::filter(c, chosen))
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(side.fields().clone()));
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(chosen.true_count()));
    Ok(RecordBatch::try_new_with_options(schema, columns, &options)?)
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, ChangelogError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| internal(&format!("the sink's input has no {name}")))
}

fn internal(message: &str) -> ChangelogError {
    ChangelogError::Internal(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use arroyo_rpc::TIMESTAMP_FIELD;

    fn fields() -> Fields {
        Fields::from(vec![Field::new("k", DataType::Int64, false)])
    }

    /// What Arroyo gives an updating sink: `(op, before k, after k)` per row.
    fn envelope(rows: &[(&str, Option<i64>, Option<i64>)]) -> RecordBatch {
        let side = |keys: Vec<Option<i64>>| -> ArrayRef {
            let valid = NullBuffer::from(keys.iter().map(Option::is_some).collect::<Vec<_>>());
            // an absent side's columns hold nulls, as Arroyo's take gives them
            Arc::new(StructArray::new(
                fields(),
                vec![Arc::new(Int64Array::from(keys))],
                Some(valid),
            ))
        };
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("before", side(rows.iter().map(|r| r.1).collect())),
            ("after", side(rows.iter().map(|r| r.2).collect())),
            ("op", Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)))),
            (
                TIMESTAMP_FIELD,
                Arc::new(TimestampNanosecondArray::from(vec![0; rows.len()])),
            ),
        ];
        RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(|(n, c)| (n, c, n != "op"))).unwrap()
    }

    fn keys(batch: &RecordBatch) -> Vec<i64> {
        batch.column(0).as_primitive::<Int64Type>().values().to_vec()
    }

    #[test]
    fn changelog_maps_inserts_updates_and_deletes() {
        let changes = changes(&envelope(&[
            ("c", None, Some(1)),
            ("u", Some(2), Some(3)),
            ("d", Some(4), None),
            ("r", None, Some(5)),
        ]))
        .unwrap();
        assert_eq!(keys(&changes.inserts), [1, 3, 5]);
        assert_eq!(keys(&changes.deletes), [2, 4]);
        assert_eq!(changes.inserts.schema().fields(), &fields(), "the declared columns");
    }

    #[test]
    fn changelog_inserts_only_has_no_deletes() {
        let changes = changes(&envelope(&[("c", None, Some(1)), ("c", None, Some(2))])).unwrap();
        assert_eq!(keys(&changes.inserts), [1, 2]);
        assert_eq!(changes.deletes.num_rows(), 0);
    }

    #[test]
    fn changelog_unknown_change_refused() {
        let e = changes(&envelope(&[("x", None, Some(1))])).unwrap_err();
        assert_eq!(e, ChangelogError::UnknownChange("x".to_string()));
    }

    #[test]
    fn declared_reads_after_fields() {
        let schema = envelope(&[]).schema();
        assert_eq!(declared(&schema).unwrap(), fields());
        assert!(declared(&Schema::new(fields())).is_err());
    }
}
