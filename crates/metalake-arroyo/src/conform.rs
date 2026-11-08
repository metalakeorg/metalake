//! A sink's rows as the table takes them. Arroyo gives the declared columns and its own event time; the core's writer
//! takes the table's columns, by name, in order and of its types (`TableWriter::write`).

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use arrow_schema::{DataType, Schema};

use crate::schema::compatible;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConformError {
    #[error("the table requires column '{0}', and the rows don't have it")]
    MissingRequired(String),
    #[error("column '{column}' is {from} in the rows, and {to} in the table")]
    Unconvertible {
        column: String,
        from: DataType,
        to: DataType,
    },
    #[error("column '{0}' is required in the table, and a row has a null in it")]
    NullInRequired(String),
    #[error("{0}")]
    Arrow(String),
}

impl ConformError {
    /// Whether the rows caused it, rather than a plan that should have refused it.
    pub fn is_data(&self) -> bool {
        matches!(self, Self::NullInRequired(_))
    }
}

impl From<arrow_schema::ArrowError> for ConformError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Arrow(e.to_string())
    }
}

/// `batch` as `table`'s rows: each of its columns in order, cast where the type differs within a family, nulls for an
/// optional column the rows don't have. Columns of the rows the table lacks, as Arroyo's event time, are left out.
pub fn conform(batch: &RecordBatch, table: &Schema) -> Result<RecordBatch, ConformError> {
    let columns = table
        .fields()
        .iter()
        .map(|field| {
            let Some(column) = batch.column_by_name(field.name()) else {
                return match field.is_nullable() {
                    true => Ok(new_null_array(field.data_type(), batch.num_rows())),
                    false => Err(ConformError::MissingRequired(field.name().clone())),
                };
            };
            let column: ArrayRef = if column.data_type() == field.data_type() {
                column.clone()
            } else if compatible(field.data_type(), column.data_type()) {
                arrow_cast::cast(column, field.data_type())?
            } else {
                return Err(ConformError::Unconvertible {
                    column: field.name().clone(),
                    from: column.data_type().clone(),
                    to: field.data_type().clone(),
                });
            };
            if !field.is_nullable() && column.null_count() > 0 {
                return Err(ConformError::NullInRequired(field.name().clone()));
            }
            Ok(column)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(Arc::new(table.clone()), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMicrosecondType};
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_schema::{Field, TimeUnit};
    use arroyo_rpc::TIMESTAMP_FIELD;

    fn table() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("note", DataType::Utf8, true),
        ])
    }

    /// What Arroyo gives a sink declared `(note, id, at)`: another order, its own time unit, and its event time.
    fn arroyo_rows(ids: Vec<Option<i64>>) -> RecordBatch {
        let n = ids.len();
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("note", Arc::new(StringArray::from(vec!["x"; n]))),
            ("id", Arc::new(Int64Array::from(ids))),
            (
                "at",
                Arc::new(TimestampNanosecondArray::from(vec![1_500_000_000_001_000; n])),
            ),
            (TIMESTAMP_FIELD, Arc::new(TimestampNanosecondArray::from(vec![7; n]))),
        ];
        RecordBatch::try_from_iter(columns).unwrap()
    }

    #[test]
    fn conform_orders_and_casts_columns() {
        let rows = conform(&arroyo_rows(vec![Some(1), Some(2)]), &table()).unwrap();
        assert_eq!(
            rows.schema().as_ref(),
            &table(),
            "the table's columns, without Arroyo's time"
        );
        assert_eq!(rows.column(0).as_primitive::<Int64Type>().values().to_vec(), [1, 2]);
        let at = rows.column(1).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(
            at.value(0),
            1_500_000_000_001,
            "nanoseconds to the table's microseconds"
        );
    }

    #[test]
    fn conform_fills_undeclared_optional_columns() {
        let only_id =
            RecordBatch::try_from_iter(vec![("id", Arc::new(Int64Array::from(vec![3])) as ArrayRef)]).unwrap();
        let rows = conform(&only_id, &table()).unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert!(rows.column(1).is_null(0) && rows.column(2).is_null(0));
    }

    #[test]
    fn conform_invalid_rows_refused() {
        let error = conform(&arroyo_rows(vec![Some(1), None]), &table()).unwrap_err();
        assert_eq!(error, ConformError::NullInRequired("id".into()));
        assert!(error.is_data());

        let no_id =
            RecordBatch::try_from_iter(vec![("note", Arc::new(StringArray::from(vec!["x"])) as ArrayRef)]).unwrap();
        assert_eq!(
            conform(&no_id, &table()).unwrap_err(),
            ConformError::MissingRequired("id".into())
        );

        let text_id =
            RecordBatch::try_from_iter(vec![("id", Arc::new(StringArray::from(vec!["1"])) as ArrayRef)]).unwrap();
        let error = conform(&text_id, &table()).unwrap_err();
        assert!(matches!(error, ConformError::Unconvertible { .. }) && !error.is_data());
    }
}
