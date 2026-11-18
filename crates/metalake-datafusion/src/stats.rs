//! What a file says of itself, from the footer the catalog keeps, without opening it. Columns are matched by field id,
//! since a file keeps the names it was written with. A statistic is exact only if it holds of the rows a read sees.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{ScalarValue, Statistics};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::error::Result;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::ParquetMetaData;
use metalake::{Evolve, ScanTask};

use crate::catalog::external;
use metalake::schema::FIELD_ID;

/// The statistics of `task`'s file under the table's `schema`. Its rows are exact: those of the file less those its
/// vector deletes. A vector may have deleted a bound, so with one the columns' statistics are only estimates; a float
/// column's always are, since Parquet leaves NaN out of its bounds. A column the file lacks or holds in an earlier
/// type (TYP-3, TYP-5) has none, but for an identity partition column a file was written without (ENG-3, step 2):
/// every row holds its partition value (PAR-2), so its bounds are that value, which DataFusion reads as a constant
/// instead of the column, and prunes by.
pub(crate) fn of_file(
    task: &ScanTask,
    metadata: Option<&ParquetMetaData>,
    schema: &SchemaRef,
    evolve: &Evolve,
) -> Result<Statistics> {
    let deleted = task.vector.as_ref().map_or(0, |v| v.cardinality);
    let mut statistics = Statistics::new_unknown(schema);
    statistics.num_rows = Precision::Exact((task.record_count - deleted) as usize);
    let Some(metadata) = metadata else {
        // No footer in the catalog yet, as for a file registered in place: its partition values still hold.
        partition_constants(&mut statistics, task, schema, evolve, &[])?;
        return Ok(statistics);
    };
    let file_metadata = metadata.file_metadata();
    let file = parquet_to_arrow_schema(file_metadata.schema_descr(), file_metadata.key_value_metadata())?;
    let file = evolve.file_schema(&file);
    let by_id: HashMap<&String, &Arc<Field>> = file.fields().iter().filter_map(|f| Some((id(f)?, f))).collect();
    let (columns, fields): (Vec<usize>, Vec<Arc<Field>>) = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(at, field)| {
            let found = by_id.get(id(field)?)?;
            (found.data_type() == field.data_type()).then(|| (at, Arc::clone(found)))
        })
        .unzip();
    let found = DFParquetMetadata::statistics_from_parquet_metadata(metadata, &Arc::new(Schema::new(fields)))?;
    for (at, column) in columns.iter().copied().zip(found.column_statistics) {
        let estimate = deleted > 0 || is_float(schema.field(at).data_type());
        statistics.column_statistics[at] = if estimate { column.to_inexact() } else { column };
    }
    partition_constants(&mut statistics, task, schema, evolve, &columns)?;
    Ok(statistics)
}

/// An identity partition column the file lacks, the columns `in_file` not among them, holds its partition value in
/// every row (PAR-2): exact bounds of that value and no nulls, which DataFusion reads as a constant.
fn partition_constants(
    statistics: &mut Statistics,
    task: &ScanTask,
    schema: &SchemaRef,
    evolve: &Evolve,
    in_file: &[usize],
) -> Result<()> {
    let partition = evolve.partition_values(task.spec_id, &task.partition);
    for (at, field) in schema.fields().iter().enumerate() {
        let value = id(field).and_then(|i| partition.get(&i.parse().ok()?));
        if in_file.contains(&at) || value.is_none_or(serde_json::Value::is_null) {
            continue;
        }
        let constant = evolve.column_in(field, None, 1, &partition).map_err(external)?;
        let value = ScalarValue::try_from_array(&constant, 0)?;
        let column = &mut statistics.column_statistics[at];
        column.min_value = Precision::Exact(value.clone());
        column.max_value = Precision::Exact(value);
        column.null_count = Precision::Exact(0);
        column.distinct_count = Precision::Exact(1);
    }
    Ok(())
}

fn id(field: &Field) -> Option<&String> {
    field.metadata().get(FIELD_ID)
}

fn is_float(ty: &DataType) -> bool {
    matches!(ty, DataType::Float16 | DataType::Float32 | DataType::Float64)
}
