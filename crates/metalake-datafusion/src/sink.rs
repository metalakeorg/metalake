//! Writes: each statement is one commit, whatever its rows or the files they make. DataFusion ends a statement in a
//! `DataSink`, with the rows to write as a stream. The bytes reach storage as they come, and nothing exists for a reader
//! until the commit; a refused commit leaves orphans, which their intents name.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::compute::{filter_record_batch, prep_null_mask_filter};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Scan, TableWriter, WriteOptions};

use crate::catalog::external;
use crate::table::MetaTable;

/// An `insert`: the rows of its stream, in new files, in one commit. An `insert overwrite` removes in that commit the
/// files its read saw, and is refused if the table has changed since (CMT-6).
#[derive(Debug)]
pub(crate) struct Insert {
    pub table: MetaTable,
    pub overwrite: bool,
}

impl DisplayAs for Insert {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaLakeInsert: {}", self.table.table.name())
    }
}

#[async_trait]
impl DataSink for Insert {
    fn schema(&self) -> &SchemaRef {
        self.table.table.schema()
    }

    async fn write_all(&self, mut data: SendableRecordBatchStream, _: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable {
            on,
            catalog,
            storage,
            table,
            ..
        } = &self.table;
        let mut writer =
            TableWriter::new(catalog, table, storage.clone(), WriteOptions::default()).map_err(external)?;
        let mut rows = 0;
        while let Some(batch) = data.try_next().await? {
            rows += batch.num_rows() as u64;
            writer.write(&batch).await.map_err(external)?;
        }
        let files = writer.finish().await.map_err(external)?;
        let mut transaction = on.transaction().append(files);
        if self.overwrite {
            transaction = transaction.replaces(table).await.map_err(external)?;
        }
        transaction.commit().await.map_err(external)?;
        Ok(rows)
    }
}

/// The rows a `delete` or an `update` acts on. DataFusion gives those statements no input, so this finds them through the
/// core's scan, which names each by file and position.
#[derive(Debug)]
pub(crate) struct Selection {
    pub table: MetaTable,
    pub scan: Scan,
    /// Over the scan's schema; none matches every row.
    pub filter: Option<Arc<dyn PhysicalExpr>>,
}

impl Selection {
    /// The batches of the rows that match; a filter that is null for a row does not match it.
    async fn rows(&self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let tasks = self.scan.plan().await.map_err(external)?;
        let filter = self.filter.clone();
        let batches = self.scan.read(tasks, &self.table.storage).map(move |batch| {
            let batch = batch.map_err(external)?;
            let Some(filter) = &filter else {
                return Ok(batch);
            };
            let matched = filter.evaluate(&batch)?.into_array(batch.num_rows())?;
            let matched = matched.as_boolean();
            match matched.null_count() {
                0 => Ok(filter_record_batch(&batch, matched)?),
                _ => Ok(filter_record_batch(&batch, &prep_null_mask_filter(matched))?),
            }
        });
        Ok(batches.boxed())
    }
}

/// Marks the rows of `batch` to delete.
pub(crate) fn mark(deletes: &mut Deletes, batch: &RecordBatch) {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .expect("asked of the scan")
            .as_primitive::<Int64Type>()
    };
    for (file, position) in column(FILE_ID).values().iter().zip(column(POSITION).values()) {
        deletes.insert(*file, *position as u64);
    }
}

/// A `delete`: the rows it selects, as vectors, in one commit. It commutes with other deletes (DV-4).
#[derive(Debug)]
pub(crate) struct Delete {
    pub rows: Selection,
}

impl DisplayAs for Delete {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaLakeDelete: {}", self.rows.table.table.name())
    }
}

#[async_trait]
impl DataSink for Delete {
    fn schema(&self) -> &SchemaRef {
        self.rows.scan.schema()
    }

    async fn write_all(&self, _: SendableRecordBatchStream, _: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable { on, storage, table, .. } = &self.rows.table;
        let mut batches = self.rows.rows().await?;
        let mut deletes = Deletes::new();
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await? {
            rows += batch.num_rows() as u64;
            mark(&mut deletes, &batch);
        }
        if !deletes.is_empty() {
            let commit = on.transaction().delete(table, storage, deletes).commit();
            commit.await.map_err(external)?;
        }
        Ok(rows)
    }
}

/// An `update`: a Parquet file does not change, so each row it selects is deleted, as a vector, and written anew, in one
/// commit. It does not commute with a commit that touched one of its files: that vector changed under it (DV-4), or the
/// file went (CMT-7), and the protocol refuses it, for the caller to run again from a new read. Commits on other files
/// go on beside it.
#[derive(Debug)]
pub(crate) struct Update {
    pub rows: Selection,
    /// Each assigned column of the table, by its place, and its new value, over the scan's schema.
    pub assignments: Vec<(usize, Arc<dyn PhysicalExpr>)>,
}

impl Update {
    /// The new version of the rows of `batch`, in the table's columns.
    fn updated(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = self.rows.table.table.schema();
        let mut columns: Vec<ArrayRef> = batch.columns()[..schema.fields().len()].to_vec();
        for (at, value) in &self.assignments {
            let value = value.evaluate(batch)?.into_array(batch.num_rows())?;
            columns[*at] = cast(&value, schema.field(*at).data_type())?;
        }
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

impl DisplayAs for Update {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaLakeUpdate: {}", self.rows.table.table.name())
    }
}

#[async_trait]
impl DataSink for Update {
    fn schema(&self) -> &SchemaRef {
        self.rows.scan.schema()
    }

    async fn write_all(&self, _: SendableRecordBatchStream, _: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable {
            on,
            catalog,
            storage,
            table,
            ..
        } = &self.rows.table;
        let mut batches = self.rows.rows().await?;
        let mut writer =
            TableWriter::new(catalog, table, storage.clone(), WriteOptions::default()).map_err(external)?;
        let mut deletes = Deletes::new();
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            rows += batch.num_rows() as u64;
            mark(&mut deletes, &batch);
            writer.write(&self.updated(&batch)?).await.map_err(external)?;
        }
        let files = writer.finish().await.map_err(external)?;
        if !deletes.is_empty() {
            let commit = on.transaction().append(files).delete(table, storage, deletes).commit();
            commit.await.map_err(external)?;
        }
        Ok(rows)
    }
}
