//! The core's rows as Arroyo's updating source emits them. The core gives a table's columns and what happened to each
//! row (`_change_type`, `_commit_seq`, `_commit_timestamp`); Arroyo takes, for an updating table, Debezium's envelope:
//! the row in `after` with `op = c` when inserted, in `before` with `op = d` when deleted, and the row's time. Its
//! unrolling turns them into inserts and retractions.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{ArrayRef, RecordBatch, StringArray, StructArray, TimestampNanosecondArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Fields};
use arroyo_rpc::TIMESTAMP_FIELD;
use arroyo_rpc::df::ArroyoSchema;
use metalake::{CHANGE_TYPE, COMMIT_SEQ, COMMIT_TIMESTAMP};

use crate::schema::compatible;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EnvelopeError {
    #[error("column '{0}' is declared, and the table's rows don't have it")]
    MissingColumn(String),
    #[error("column '{column}' is {from} in the table, and declared {to}")]
    Unconvertible {
        column: String,
        from: DataType,
        to: DataType,
    },
    #[error("column '{column}' is declared NOT NULL, and commit {seq} has a null in it")]
    NullInRequired { column: String, seq: i64 },
    #[error("commit {seq} has a change '{change}', neither an insert nor a delete")]
    UnknownChange { change: String, seq: i64 },
    #[error("the time of commit {seq} is beyond what Arroyo's nanoseconds hold")]
    TimestampOverflow { seq: i64 },
    #[error("{0}")]
    Internal(String),
}

impl EnvelopeError {
    /// Whether the table's data caused it, rather than a plan that should have refused it.
    pub fn is_data(&self) -> bool {
        matches!(
            self,
            Self::NullInRequired { .. } | Self::UnknownChange { .. } | Self::TimestampOverflow { .. }
        )
    }
}

impl From<arrow_schema::ArrowError> for EnvelopeError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Internal(e.to_string())
    }
}

/// The core's `batch` as Arroyo's updating source emits it, in the schema `out` the planner gave the source:
/// `before` and `after`, structs of the declared columns, `op`, and the row's time.
pub fn envelope(batch: &RecordBatch, out: &ArroyoSchema) -> Result<RecordBatch, EnvelopeError> {
    let seqs = core_column(batch, COMMIT_SEQ)?.as_primitive_opt::<Int64Type>();
    let seqs = seqs.ok_or_else(|| internal(format!("{COMMIT_SEQ} is not Int64")))?;
    let changes = core_column(batch, CHANGE_TYPE)?.as_string_opt::<i32>();
    let changes = changes.ok_or_else(|| internal(format!("{CHANGE_TYPE} is not Utf8")))?;
    let times = core_column(batch, COMMIT_TIMESTAMP)?.as_primitive_opt::<TimestampMicrosecondType>();
    let times = times.ok_or_else(|| internal(format!("{COMMIT_TIMESTAMP} is not in microseconds")))?;

    let mut ops = Vec::with_capacity(batch.num_rows());
    for (i, change) in changes.iter().enumerate() {
        ops.push(match change {
            Some("insert") => "c",
            Some("delete") => "d",
            other => {
                return Err(EnvelopeError::UnknownChange {
                    change: other.unwrap_or("null").to_string(),
                    seq: seqs.value(i),
                });
            }
        });
    }
    let inserted = BooleanBuffer::collect_bool(ops.len(), |i| ops[i] == "c");

    let fields = declared(out)?;
    let columns = fields
        .iter()
        .map(|field| {
            let column = batch
                .column_by_name(field.name())
                .ok_or_else(|| EnvelopeError::MissingColumn(field.name().clone()))?;
            let column = if column.data_type() == field.data_type() {
                column.clone()
            } else if compatible(column.data_type(), field.data_type()) {
                arrow_cast::cast(column, field.data_type())?
            } else {
                return Err(EnvelopeError::Unconvertible {
                    column: field.name().clone(),
                    from: column.data_type().clone(),
                    to: field.data_type().clone(),
                });
            };
            if !field.is_nullable()
                && let Some(nulls) = column.logical_nulls()
                && let Some(row) = (0..column.len()).find(|r| nulls.is_null(*r))
            {
                return Err(EnvelopeError::NullInRequired {
                    column: field.name().clone(),
                    seq: seqs.value(row),
                });
            }
            Ok(column)
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;

    // the same columns in both, each valid where its change is
    let after: ArrayRef = Arc::new(StructArray::try_new(
        fields.clone(),
        columns.clone(),
        Some(NullBuffer::new(inserted.clone())),
    )?);
    let before: ArrayRef = Arc::new(StructArray::try_new(
        fields,
        columns,
        Some(NullBuffer::new(!&inserted)),
    )?);
    let op: ArrayRef = Arc::new(StringArray::from(ops));

    let mut nanos = Vec::with_capacity(times.len());
    for (i, micros) in times.values().iter().enumerate() {
        let seq = seqs.value(i);
        nanos.push(
            micros
                .checked_mul(1_000)
                .ok_or(EnvelopeError::TimestampOverflow { seq })?,
        );
    }
    let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(nanos));

    let out_columns = out
        .schema
        .fields()
        .iter()
        .map(|f| -> Result<ArrayRef, EnvelopeError> {
            Ok(match f.name().as_str() {
                "before" => before.clone(),
                "after" => after.clone(),
                "op" => op.clone(),
                TIMESTAMP_FIELD => time.clone(),
                other => return Err(internal(format!("an updating source has no column '{other}'"))),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(out.schema.clone(), out_columns)?)
}

/// The declared columns: the fields of `out`'s `after` struct.
fn declared(out: &ArroyoSchema) -> Result<Fields, EnvelopeError> {
    match out.schema.field_with_name("after").map(|f| f.data_type()) {
        Ok(DataType::Struct(fields)) => Ok(fields.clone()),
        _ => Err(internal("the source's schema is not an updating table's".to_string())),
    }
}

fn core_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, EnvelopeError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| internal(format!("the core's rows have no {name}")))
}

fn internal(message: String) -> EnvelopeError {
    EnvelopeError::Internal(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int64Array, StringViewArray, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema, TimeUnit};

    const SECOND: i64 = 1_000_000;

    /// A batch as the core gives it: a key, a name, a time, a column nobody declares, and what happened to each row.
    fn core(keys: &[Option<i64>], changes: &[&str], seqs: &[i64]) -> RecordBatch {
        let n = keys.len();
        let micros = |seq: &i64| 1_700_000_000 * SECOND + seq * SECOND;
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("k", Arc::new(Int64Array::from(keys.to_vec()))),
            (
                "name",
                Arc::new(StringViewArray::from_iter_values((0..n).map(|i| format!("n{i}")))),
            ),
            (
                "at",
                Arc::new(TimestampMicrosecondArray::from(vec![5 * SECOND; n]).with_timezone("UTC")),
            ),
            ("unread", Arc::new(Float64Array::from(vec![0.5; n]))),
            (CHANGE_TYPE, Arc::new(StringArray::from(changes.to_vec()))),
            (COMMIT_SEQ, Arc::new(Int64Array::from(seqs.to_vec()))),
            (
                COMMIT_TIMESTAMP,
                Arc::new(
                    TimestampMicrosecondArray::from(seqs.iter().map(micros).collect::<Vec<_>>()).with_timezone("UTC"),
                ),
            ),
        ];
        RecordBatch::try_from_iter(columns).unwrap()
    }

    /// The schema the planner gives an updating source of these declared columns.
    fn out(declared: Vec<Field>) -> ArroyoSchema {
        let row = DataType::Struct(declared.into());
        let schema = Schema::new(vec![
            Field::new("before", row.clone(), true),
            Field::new("after", row, true),
            Field::new("op", DataType::Utf8, false),
            Field::new(TIMESTAMP_FIELD, DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]);
        ArroyoSchema::new_unkeyed(Arc::new(schema), 3)
    }

    fn key_only() -> ArroyoSchema {
        out(vec![Field::new("k", DataType::Int64, true)])
    }

    fn structs(batch: &RecordBatch) -> (&StructArray, &StructArray) {
        (batch.column(0).as_struct(), batch.column(1).as_struct())
    }

    fn ops(batch: &RecordBatch) -> Vec<&str> {
        batch.column(2).as_string::<i32>().iter().map(Option::unwrap).collect()
    }

    #[test]
    fn inserts_go_in_after_and_deletes_in_before() {
        let enveloped = envelope(
            &core(
                &[Some(1), Some(2), Some(3)],
                &["insert", "delete", "insert"],
                &[7, 8, 9],
            ),
            &key_only(),
        )
        .unwrap();
        let (before, after) = structs(&enveloped);
        assert_eq!(ops(&enveloped), ["c", "d", "c"]);
        assert_eq!(
            (0..3).map(|i| after.is_valid(i)).collect::<Vec<_>>(),
            [true, false, true]
        );
        assert_eq!(
            (0..3).map(|i| before.is_valid(i)).collect::<Vec<_>>(),
            [false, true, false]
        );
        let keys = |s: &StructArray| s.column(0).as_primitive::<Int64Type>().values().to_vec();
        assert_eq!(
            keys(after),
            [1, 2, 3],
            "both hold the row, each valid where its change is"
        );
        assert_eq!(keys(before), [1, 2, 3]);
    }

    #[test]
    fn the_row_time_is_its_commits_in_nanoseconds() {
        let enveloped = envelope(&core(&[Some(1), Some(2)], &["insert", "delete"], &[3, 4]), &key_only()).unwrap();
        let times = enveloped
            .column(3)
            .as_primitive::<arrow_array::types::TimestampNanosecondType>();
        assert_eq!(
            times.values().to_vec(),
            [(1_700_000_003 * SECOND) * 1_000, (1_700_000_004 * SECOND) * 1_000]
        );
        assert_eq!(
            enveloped.schema(),
            out(vec![Field::new("k", DataType::Int64, true)]).schema
        );
    }

    #[test]
    fn the_declared_columns_in_their_order_cast_to_their_types() {
        let declared = out(vec![
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("name", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
        ]);
        let enveloped = envelope(&core(&[Some(1)], &["insert"], &[1]), &declared).unwrap();
        let after = enveloped.column(1).as_struct();
        assert_eq!(
            after.column_names(),
            ["at", "name", "k"],
            "the declaration's order, without 'unread'"
        );
        let at = after
            .column(0)
            .as_primitive::<arrow_array::types::TimestampNanosecondType>();
        assert_eq!(at.value(0), 5 * SECOND * 1_000, "the same instant");
        assert_eq!(after.column(1).as_string::<i32>().value(0), "n0");
    }

    #[test]
    fn a_null_in_a_required_column_names_its_commit() {
        let required = out(vec![Field::new("k", DataType::Int64, false)]);
        let batch = core(&[Some(1), None], &["insert", "insert"], &[10, 11]);
        let error = envelope(&batch, &required).unwrap_err();
        assert_eq!(
            error,
            EnvelopeError::NullInRequired {
                column: "k".into(),
                seq: 11
            }
        );
        assert!(error.is_data());
        let fine = core(&[Some(1), Some(2)], &["insert", "delete"], &[10, 11]);
        assert!(
            envelope(&fine, &required).is_ok(),
            "a required column with no null, in both structs"
        );
    }

    #[test]
    fn what_it_refuses() {
        let error = |batch: RecordBatch, out: ArroyoSchema| envelope(&batch, &out).unwrap_err();
        let unknown = error(core(&[Some(1)], &["update"], &[4]), key_only());
        assert_eq!(
            unknown,
            EnvelopeError::UnknownChange {
                change: "update".into(),
                seq: 4
            }
        );

        let mut late = core(&[Some(1)], &["insert"], &[4]);
        let time = TimestampMicrosecondArray::from(vec![i64::MAX / 1_000 + 1]).with_timezone("UTC");
        late = RecordBatch::try_new(late.schema(), {
            let mut columns = late.columns().to_vec();
            columns[6] = Arc::new(time);
            columns
        })
        .unwrap();
        assert_eq!(error(late, key_only()), EnvelopeError::TimestampOverflow { seq: 4 });

        let missing = error(
            core(&[Some(1)], &["insert"], &[1]),
            out(vec![Field::new("x", DataType::Int64, true)]),
        );
        assert_eq!(missing, EnvelopeError::MissingColumn("x".into()));
        let other = error(
            core(&[Some(1)], &["insert"], &[1]),
            out(vec![Field::new("k", DataType::Utf8, true)]),
        );
        assert!(
            matches!(other, EnvelopeError::Unconvertible { .. }) && !other.is_data(),
            "{other}"
        );

        let not_updating = ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new(TIMESTAMP_FIELD, DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            ])),
            1,
        );
        assert!(matches!(
            error(core(&[Some(1)], &["insert"], &[1]), not_updating),
            EnvelopeError::Internal(_)
        ));
    }
}
