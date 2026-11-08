//! A sink's rows as Arroyo gives them to an updating sink: Debezium's envelope, the row in `after` with `op = c` when
//! inserted, in `before` with `op = d` when deleted, and in both with `op = u` when updated. The sink takes them as
//! runs of rows to insert and rows to delete, in the order they came, each with the declared columns: a delete may
//! undo an insert before it, never one after it.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, UInt32Array};
use arrow_schema::{DataType, Fields, Schema};

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

/// Consecutive rows of one kind of change.
#[derive(Debug, Clone, PartialEq)]
pub enum Run {
    Insert(RecordBatch),
    Delete(RecordBatch),
}

/// The declared columns of an updating sink's input `schema`: the fields of its `after` struct.
pub fn declared(schema: &Schema) -> Result<Fields, ChangelogError> {
    match schema.field_with_name("after").map(|f| f.data_type()) {
        Ok(DataType::Struct(fields)) => Ok(fields.clone()),
        _ => Err(internal("the sink's input is not an updating table's")),
    }
}

/// `batch`, in Debezium's envelope, as runs in its order: an insert is its `after`, a delete its `before`, and an
/// update a delete of the one then an insert of the other.
pub fn runs(batch: &RecordBatch) -> Result<Vec<Run>, ChangelogError> {
    let op = column(batch, "op")?
        .as_string_opt::<i32>()
        .ok_or_else(|| internal("op is not Utf8"))?;
    let mut kinds: Vec<(bool, u32)> = Vec::with_capacity(op.len());
    for (row, change) in op.iter().enumerate() {
        let row = row as u32;
        match change {
            Some("c" | "r") => kinds.push((true, row)),
            Some("d") => kinds.push((false, row)),
            Some("u") => kinds.extend([(false, row), (true, row)]),
            other => return Err(ChangelogError::UnknownChange(other.unwrap_or("null").to_string())),
        }
    }
    let (before, after) = (side(batch, "before")?, side(batch, "after")?);
    kinds
        .chunk_by(|a, b| a.0 == b.0)
        .map(|run| {
            let indices = UInt32Array::from_iter_values(run.iter().map(|(_, row)| *row));
            Ok(match run[0].0 {
                true => Run::Insert(rows(after, &indices)?),
                false => Run::Delete(rows(before, &indices)?),
            })
        })
        .collect()
}

fn side<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StructArray, ChangelogError> {
    column(batch, name)?
        .as_struct_opt()
        .ok_or_else(|| internal("before and after are not structs"))
}

/// The rows of `side`, a struct of the declared columns, at `indices`.
fn rows(side: &StructArray, indices: &UInt32Array) -> Result<RecordBatch, ChangelogError> {
    // taken before the batch is built: a column NOT NULL holds nulls where its side is absent
    let columns = side
        .columns()
        .iter()
        .map(|c| arrow_select::take::take(c, indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(side.fields().clone()));
    let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
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

    /// The kind and keys of each run.
    fn shape(runs: &[Run]) -> Vec<(&str, Vec<i64>)> {
        runs.iter()
            .map(|r| match r {
                Run::Insert(b) => ("insert", keys(b)),
                Run::Delete(b) => ("delete", keys(b)),
            })
            .collect()
    }

    #[test]
    fn changelog_runs_preserves_order() {
        let runs = runs(&envelope(&[
            ("c", None, Some(1)),
            ("r", None, Some(2)),
            ("d", Some(1), None),
            ("u", Some(2), Some(3)),
            ("c", None, Some(4)),
        ]))
        .unwrap();
        assert_eq!(
            shape(&runs),
            [("insert", vec![1, 2]), ("delete", vec![1, 2]), ("insert", vec![3, 4])],
            "an update is a delete then an insert"
        );
        let Run::Insert(first) = &runs[0] else { unreachable!() };
        assert_eq!(first.schema().fields(), &fields(), "the declared columns");
    }

    #[test]
    fn changelog_empty_batch_skips() {
        assert_eq!(runs(&envelope(&[])).unwrap(), []);
    }

    #[test]
    fn changelog_unknown_change_refused() {
        let e = runs(&envelope(&[("x", None, Some(1))])).unwrap_err();
        assert_eq!(e, ChangelogError::UnknownChange("x".to_string()));
    }

    #[test]
    fn declared_reads_after_fields() {
        let schema = envelope(&[]).schema();
        assert_eq!(declared(&schema).unwrap(), fields());
        assert!(declared(&Schema::new(fields())).is_err());
    }
}
