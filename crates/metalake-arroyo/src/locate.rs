//! Rows to delete that a sink did not write in the epoch, found in the table as its commit reads it, as Delta's MERGE
//! finds the rows it changes: one scan with each row's file and position, pruned on the first column that tells rows
//! apart, and each row to delete matched to one live row of the same values, so that of N equal rows a delete takes
//! one. A row found for no delete is left; a delete that finds no row deletes nothing, as a delete by value does in
//! Iceberg, Delta and Flink's upserts.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{ArrowError, DataType};
use futures::TryStreamExt;
use metalake::partition::Scalar;
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Catalog, Scan, Storage, Table};

#[derive(Debug, thiserror::Error)]
pub enum LocateError {
    #[error(transparent)]
    Core(#[from] metalake::Error),
    #[error("{0}")]
    Arrow(#[from] ArrowError),
}

/// What a search found: the rows to delete, and how many deletes found none.
#[derive(Debug, Default)]
pub struct Located {
    pub deletes: Deletes,
    pub unmatched: usize,
}

/// Finds `rows`, of the declared columns, among `table`'s live rows: by `key`, or by every declared column when it is
/// empty.
pub async fn locate(
    catalog: &Catalog,
    table: &Table,
    storage: &Storage,
    rows: &[RecordBatch],
    key: &[String],
) -> Result<Located, LocateError> {
    let Some(first) = rows.first() else {
        return Ok(Located::default());
    };
    let names: Vec<String> = match key {
        [] => first.schema().fields().iter().map(|f| f.name().clone()).collect(),
        key => key.to_vec(),
    };
    // compared as the table holds them: the declared types are cast as the sink casts what it writes
    let table_schema = table.schema();
    let types = names
        .iter()
        .map(|n| Ok(table_schema.field_with_name(n)?.data_type().clone()))
        .collect::<Result<Vec<DataType>, ArrowError>>()?;
    let converter = RowConverter::new(types.iter().cloned().map(SortField::new).collect())?;

    let mut wanted: HashMap<OwnedRow, usize> = HashMap::new();
    let mut prune: Option<Bounds> = None;
    for batch in rows {
        let columns = names
            .iter()
            .zip(&types)
            .map(|(name, ty)| {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| ArrowError::SchemaError(format!("the rows have no column '{name}'")))?;
                arrow_cast::cast(column, ty)
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        Bounds::widen(&mut prune, &columns[0]);
        for row in converter.convert_columns(&columns)?.iter() {
            *wanted.entry(row.owned()).or_default() += 1;
        }
    }

    let mut scan = Scan::new(catalog, table).columns(&names)?.with_positions();
    if let Some(bounds) = &prune {
        scan = bounds.apply(scan, &names[0])?;
    }
    let mut located = Located::default();
    let mut batches = scan.read(scan.plan().await?, storage);
    while !wanted.is_empty()
        && let Some(batch) = batches.try_next().await?
    {
        let found = converter.convert_columns(&batch.columns()[..names.len()])?;
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .map(|c| c.as_primitive::<Int64Type>().clone())
        };
        let (files, positions) = (column(FILE_ID), column(POSITION));
        let (Some(files), Some(positions)) = (files, positions) else {
            return Err(ArrowError::SchemaError("the scan has no positions".to_string()).into());
        };
        for (i, row) in found.iter().enumerate() {
            let key = row.owned();
            let Some(left) = wanted.get_mut(&key) else {
                continue;
            };
            located.deletes.insert(files.value(i), positions.value(i) as u64);
            *left -= 1;
            if *left == 0 {
                wanted.remove(&key);
            }
        }
    }
    located.unmatched = wanted.values().sum();
    Ok(located)
}

/// The range of the rows' first column, for the scan to skip files that cannot hold one: integers and strings, the
/// usual keys; another type is not pruned.
#[derive(Debug, PartialEq)]
enum Bounds {
    Long(i64, i64),
    String(String, String),
    None,
}

impl Bounds {
    fn widen(bounds: &mut Option<Bounds>, column: &ArrayRef) {
        let of_column = match column.data_type() {
            DataType::Int64 => longs(column.as_primitive::<Int64Type>().iter()),
            DataType::Int32 => longs(column.as_primitive::<Int32Type>().iter().map(|v| v.map(i64::from))),
            DataType::Utf8 => strings(column.as_string::<i32>().iter()),
            DataType::LargeUtf8 => strings(column.as_string::<i64>().iter()),
            _ => Bounds::None,
        };
        *bounds = Some(match (bounds.take(), of_column) {
            (None, b) => b,
            (Some(Bounds::Long(a, b)), Bounds::Long(c, d)) => Bounds::Long(a.min(c), b.max(d)),
            (Some(Bounds::String(a, b)), Bounds::String(c, d)) => Bounds::String(a.min(c), b.max(d)),
            _ => Bounds::None,
        });
    }

    fn apply(&self, scan: Scan, column: &str) -> Result<Scan, metalake::Error> {
        match self {
            Bounds::Long(lo, hi) => scan.filter(column, Some(Scalar::Long(*lo)), Some(Scalar::Long(*hi))),
            Bounds::String(lo, hi) => scan.filter(column, Some(Scalar::String(lo)), Some(Scalar::String(hi))),
            Bounds::None => Ok(scan),
        }
    }
}

/// The range of non-null integers; a null leaves the rows unpruned, as a range holds no null.
fn longs(values: impl Iterator<Item = Option<i64>>) -> Bounds {
    let mut range: Option<(i64, i64)> = None;
    for v in values {
        let Some(v) = v else { return Bounds::None };
        range = Some(range.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
    }
    range.map_or(Bounds::None, |(lo, hi)| Bounds::Long(lo, hi))
}

fn strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Bounds {
    let mut range: Option<(&str, &str)> = None;
    for v in values {
        let Some(v) = v else { return Bounds::None };
        range = Some(range.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
    }
    range.map_or(Bounds::None, |(lo, hi)| Bounds::String(lo.to_string(), hi.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};

    fn column(values: ArrayRef) -> Option<Bounds> {
        let mut bounds = None;
        Bounds::widen(&mut bounds, &values);
        bounds
    }

    #[test]
    fn bounds_maps_integers_and_strings() {
        let mut bounds = column(Arc::new(Int64Array::from(vec![5, 2, 9])));
        assert_eq!(bounds, Some(Bounds::Long(2, 9)));
        Bounds::widen(&mut bounds, &(Arc::new(Int64Array::from(vec![12])) as ArrayRef));
        assert_eq!(bounds, Some(Bounds::Long(2, 12)));
        assert_eq!(
            column(Arc::new(StringArray::from(vec!["m", "b"]))),
            Some(Bounds::String("b".into(), "m".into()))
        );
    }

    #[test]
    fn bounds_null_or_other_type_skips() {
        assert_eq!(
            column(Arc::new(Int64Array::from(vec![Some(1), None]))),
            Some(Bounds::None)
        );
        let floats: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![1.0]));
        assert_eq!(column(floats), Some(Bounds::None));
    }
}
