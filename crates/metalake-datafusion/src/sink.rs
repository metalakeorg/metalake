//! Writes: each statement is one commit, whatever its rows or the files they make. DataFusion ends a statement in a
//! `DataSink`, with the rows to write as a stream. The bytes reach storage as they come, and nothing exists for a reader
//! until the commit; a refused commit leaves orphans, which their intents name.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures_util::{StreamExt, TryStreamExt};
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Scan, TableWriter, WriteOptions};

use crate::catalog::external;
use crate::table::MetaTable;

/// An `insert`: the rows of its stream, in new files, in one commit.
#[derive(Debug)]
pub(crate) struct Insert {
    pub table: MetaTable,
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
        } = &self.table;
        let mut writer =
            TableWriter::new(catalog, table, storage.clone(), WriteOptions::default()).map_err(external)?;
        let mut rows = 0;
        while let Some(batch) = data.try_next().await? {
            rows += batch.num_rows() as u64;
            writer.write(&batch).await.map_err(external)?;
        }
        let files = writer.finish().await.map_err(external)?;
        on.transaction().append(files).commit().await.map_err(external)?;
        Ok(rows)
    }
}

/// A `delete`: the rows its filter matches, as vectors, in one commit. DataFusion gives a delete no input, so this finds
/// its rows itself, through the core's scan, which names each by file and position.
#[derive(Debug)]
pub(crate) struct Delete {
    pub table: MetaTable,
    pub scan: Scan,
    /// Over the scan's schema; none matches every row.
    pub filter: Option<Arc<dyn PhysicalExpr>>,
}

impl DisplayAs for Delete {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaLakeDelete: {}", self.table.table.name())
    }
}

#[async_trait]
impl DataSink for Delete {
    fn schema(&self) -> &SchemaRef {
        self.scan.schema()
    }

    async fn write_all(&self, _: SendableRecordBatchStream, _: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable { on, storage, table, .. } = &self.table;
        let tasks = self.scan.plan().await.map_err(external)?;
        let mut batches = self.scan.read(tasks, storage).boxed();
        let mut deletes = Deletes::new();
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await.map_err(external)? {
            let matched = match &self.filter {
                Some(filter) => Some(filter.evaluate(&batch)?.into_array(batch.num_rows())?),
                None => None,
            };
            let matched = matched.as_ref().map(|m| m.as_boolean());
            let column = |name: &str| {
                batch
                    .column_by_name(name)
                    .expect("asked of the scan")
                    .as_primitive::<Int64Type>()
            };
            let (files, positions) = (column(FILE_ID), column(POSITION));
            for row in 0..batch.num_rows() {
                // A filter that is null for a row does not match it.
                if matched.is_none_or(|m| m.is_valid(row) && m.value(row)) {
                    deletes.insert(files.value(row), positions.value(row) as u64);
                    rows += 1;
                }
            }
        }
        if !deletes.is_empty() {
            let commit = on.transaction().delete(table, storage, deletes).commit();
            commit.await.map_err(external)?;
        }
        Ok(rows)
    }
}
