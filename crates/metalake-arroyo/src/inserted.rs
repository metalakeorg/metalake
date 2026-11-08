//! The rows a sink's subtask inserted in the epoch under way, and where its writer put each: a delete of one of them
//! deletes it there, in the commit that adds its file (DV-4), as Iceberg's delta writer keeps its `insertedRowMap`.
//! Rows are told apart by the table's key, if the sink declares one, or else by every declared column, in Arrow's row
//! format: their bytes, so that two rows are the same exactly when their values are. Equal rows are a multiset, each
//! with its own location, so that a delete takes one of them.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{ArrowError, Fields};
use metalake::RowLocation;

/// What a delete found among the rows inserted: where those it found are, and the indices of those it didn't.
#[derive(Debug, Default, PartialEq)]
pub struct Taken {
    pub located: Vec<RowLocation>,
    pub unmatched: Vec<u32>,
}

pub struct InsertedRows {
    /// The columns that tell rows apart, by name.
    columns: Vec<String>,
    converter: RowConverter,
    rows: HashMap<OwnedRow, Vec<RowLocation>>,
}

impl InsertedRows {
    /// For rows of the `declared` columns, told apart by `key`, or by all of them when it is empty.
    pub fn new(declared: &Fields, key: &[String]) -> Result<Self, ArrowError> {
        let fields: Vec<_> = match key {
            [] => declared.iter().collect(),
            key => key
                .iter()
                .map(|name| {
                    declared
                        .find(name)
                        .map(|(_, f)| f)
                        .ok_or_else(|| ArrowError::SchemaError(format!("key column '{name}' is not declared")))
                })
                .collect::<Result<_, _>>()?,
        };
        let converter = RowConverter::new(fields.iter().map(|f| SortField::new(f.data_type().clone())).collect())?;
        Ok(Self {
            columns: fields.iter().map(|f| f.name().clone()).collect(),
            converter,
            rows: HashMap::new(),
        })
    }

    /// Keeps `rows`, each where `located` says it was written.
    pub fn insert(&mut self, rows: &RecordBatch, located: &[RowLocation]) -> Result<(), ArrowError> {
        let converted = self.converter.convert_columns(&self.key_columns(rows)?)?;
        for (row, at) in converted.iter().zip(located) {
            self.rows.entry(row.owned()).or_default().push(*at);
        }
        Ok(())
    }

    /// Takes one inserted row for each of `rows` it has, which no later delete finds again.
    pub fn take(&mut self, rows: &RecordBatch) -> Result<Taken, ArrowError> {
        let converted = self.converter.convert_columns(&self.key_columns(rows)?)?;
        let mut taken = Taken::default();
        for (i, row) in converted.iter().enumerate() {
            let key = row.owned();
            let found = self.rows.get_mut(&key).and_then(Vec::pop);
            if self.rows.get(&key).is_some_and(Vec::is_empty) {
                self.rows.remove(&key);
            }
            match found {
                Some(at) => taken.located.push(at),
                None => taken.unmatched.push(i as u32),
            }
        }
        Ok(taken)
    }

    /// Forgets every row: the epoch's files are prepared, and a later delete looks for its row in the table.
    pub fn clear(&mut self) {
        self.rows.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn key_columns(&self, rows: &RecordBatch) -> Result<Vec<arrow_array::ArrayRef>, ArrowError> {
        self.columns
            .iter()
            .map(|name| {
                rows.column_by_name(name)
                    .cloned()
                    .ok_or_else(|| ArrowError::SchemaError(format!("the rows have no column '{name}'")))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn declared() -> Fields {
        Fields::from(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ])
    }

    fn rows(values: &[(i64, &str)]) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(values.iter().map(|v| v.0))),
            Arc::new(StringArray::from_iter_values(values.iter().map(|v| v.1))),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(declared())), columns).unwrap()
    }

    fn at(position: u64) -> RowLocation {
        RowLocation { file_id: 7, position }
    }

    #[test]
    fn inserted_take_equal_rows_one_each() {
        let mut inserted = InsertedRows::new(&declared(), &[]).unwrap();
        inserted
            .insert(&rows(&[(1, "a"), (1, "a"), (2, "b")]), &[at(0), at(1), at(2)])
            .unwrap();

        // of two equal rows, a delete takes one; a row never inserted, or another value, is not found
        let taken = inserted.take(&rows(&[(1, "a"), (3, "c"), (2, "x")])).unwrap();
        assert_eq!(taken.located, [at(1)]);
        assert_eq!(taken.unmatched, [1, 2]);
        let taken = inserted.take(&rows(&[(1, "a"), (1, "a")])).unwrap();
        assert_eq!((taken.located, taken.unmatched), (vec![at(0)], vec![1]));
        assert!(!inserted.is_empty(), "(2, b) is left");
    }

    #[test]
    fn inserted_take_by_key_skips_other_columns() {
        let mut inserted = InsertedRows::new(&declared(), &["k".to_string()]).unwrap();
        inserted.insert(&rows(&[(1, "a")]), &[at(0)]).unwrap();
        let taken = inserted.take(&rows(&[(1, "changed")])).unwrap();
        assert_eq!(taken.located, [at(0)]);
        assert!(inserted.is_empty());
    }

    #[test]
    fn inserted_clear_forgets_rows() {
        let mut inserted = InsertedRows::new(&declared(), &[]).unwrap();
        inserted.insert(&rows(&[(1, "a")]), &[at(0)]).unwrap();
        inserted.clear();
        assert_eq!(inserted.take(&rows(&[(1, "a")])).unwrap().unmatched, [0]);
    }

    #[test]
    fn inserted_undeclared_key_refused() {
        assert!(InsertedRows::new(&declared(), &["x".to_string()]).is_err());
    }
}
