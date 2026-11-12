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
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Scan, TableWriter, WriteOptions};

use crate::catalog::external;
use crate::table::MetaTable;
use crate::{details, filters};

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

    async fn write_all(&self, mut data: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
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
        details::commit(transaction, context).await.map_err(external)?;
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
    /// The statement's filters, which the scan and the filter are made of: what a process that runs it gets.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub filters: Vec<Expr>,
}

impl Selection {
    /// The rows of `table` that `filters` match: the core's scan, its files pruned by them in Postgres, each row with its
    /// file and position; and their conjunction over that scan, as `state` makes it. The same wherever it is made, in
    /// the process that plans or in one that runs a statement it was sent.
    pub(crate) fn new(table: MetaTable, filters: Vec<Expr>, state: &dyn Session) -> Result<Self> {
        let scan = Scan::new(&table.catalog, &table.table).with_positions();
        let scan = filters::pruned(scan, &table.table, &filters).map_err(external)?;
        let names = DFSchema::try_from(scan.schema().clone())?;
        let filter = conjunction(filters.clone())
            .map(|f| state.create_physical_expr(f, &names))
            .transpose()?;
        Ok(Self {
            table,
            scan,
            filter,
            filters,
        })
    }

    /// The batches of the rows that match; a filter that is null for a row does not match it.
    async fn rows(&self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Taken whole: a stream of them keeps a connection, and what writes needs another while it reads them, so that as
        // many writers as the pool has connections would each hold one and wait for ever.
        let tasks: Vec<_> = self
            .scan
            .plan()
            .await
            .map_err(external)?
            .try_collect()
            .await
            .map_err(external)?;
        let tasks = stream::iter(tasks.into_iter().map(Ok)).boxed();
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

    async fn write_all(&self, _: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable { on, storage, table, .. } = &self.rows.table;
        let mut batches = self.rows.rows().await?;
        let mut deletes = Deletes::new();
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await? {
            rows += batch.num_rows() as u64;
            mark(&mut deletes, &batch);
        }
        if !deletes.is_empty() {
            let transaction = on.transaction().delete(table, storage, deletes);
            details::commit(transaction, context).await.map_err(external)?;
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
    /// The statement's assignments, which those are made of: what a process that runs it gets.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub statement: Vec<(String, Expr)>,
}

impl Update {
    /// The update of `rows` that `assignments` (a column's name, its new value) say, as `state` makes their values.
    pub(crate) fn new(rows: Selection, assignments: Vec<(String, Expr)>, state: &dyn Session) -> Result<Self> {
        let names = DFSchema::try_from(rows.scan.schema().clone())?;
        let schema = rows.table.table.schema().clone();
        let physical = assignments
            .iter()
            .map(|(column, value)| {
                Ok((
                    schema.index_of(column)?,
                    state.create_physical_expr(value.clone(), &names)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            rows,
            assignments: physical,
            statement: assignments,
        })
    }
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

    async fn write_all(&self, _: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
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
            let transaction = on.transaction().append(files).delete(table, storage, deletes);
            details::commit(transaction, context).await.map_err(external)?;
        }
        Ok(rows)
    }
}
