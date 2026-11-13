//! A table: DataFusion's `scan` is where the protocol plans, so that the optimizer sees the files and their statistics
//! before anything runs.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchemaRef, Statistics, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::{InsertOp, MergeIntoClause};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use futures_util::TryStreamExt;
use metalake::{Catalog, Evolve, Ref, Scan, Storage, Table};

use crate::adapter::ByFieldId;
use crate::catalog::external;
use crate::source::MetaLakeSource;
use crate::point::StatementPoint;
use crate::read::{self, TaskReaders};
use crate::sink::{Delete, Insert, Selection, Update};
use crate::written::MetaWriteExec;
use crate::{filters, merge};

/// How many files' vectors are read at once while planning.
const CONCURRENCY: usize = 16;

/// A table of a ref, at the point it was read.
#[derive(Clone)]
pub struct MetaTable {
    pub(crate) on: Ref,
    pub(crate) catalog: Catalog,
    pub(crate) storage: Storage,
    pub(crate) table: Table,
    /// Where a statement of several tables reads them all, instead of where this one was looked up.
    pub(crate) statement: Option<Arc<StatementPoint>>,
}

impl fmt::Debug for MetaTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaTable").field("name", &self.table.name()).finish()
    }
}

impl MetaTable {
    /// The rows `filters` match, for a statement that writes them.
    fn selection(&self, state: &dyn Session, filters: Vec<Expr>) -> DataFusionResult<Selection> {
        self.on.writable().map_err(external)?;
        Selection::new(self.clone(), filters, state)
    }

    /// This table at the point of its statement.
    async fn at_statement(&self) -> metalake::Result<Table> {
        let Some(statement) = &self.statement else {
            return Ok(self.table.clone());
        };
        let points = statement
            .points
            .get_or_try_init(|| self.catalog.latest(&statement.refs));
        let seq = points.await?.seq(&self.table).ok_or_else(|| {
            let name = self.table.name();
            metalake::Error::NotFound(format!("table {name} on {} at this statement", self.on.name()))
        })?;
        match seq == self.table.seq() {
            true => Ok(self.table.clone()),
            false => self.on.table_at(&self.table, seq).await,
        }
    }

    /// The plan that reads what `scan` plans of `table` with DataFusion's reader, the core's bytes and footers, the
    /// vectors as selections and the columns by field id. With `positions`, its schema with them, each row also says
    /// its file and its position, for a statement that goes on to write.
    pub(crate) async fn read(
        &self,
        state: &dyn Session,
        table: &Table,
        scan: Scan,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        positions: Option<&SchemaRef>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let storage = &self.storage;
        let schema = table.schema();
        let planned: Vec<_> = scan
            .plan()
            .await
            .map_err(external)?
            .map_err(external)
            .map_ok(|task| read::file(task, schema, storage, positions.is_some()))
            .try_buffered(CONCURRENCY)
            .try_collect()
            .await?;
        let columns = positions.unwrap_or(schema);
        let (groups, mut statistics) = Self::groups(columns, planned, state.config().target_partitions())?;
        // The other side of a statement's join is a source that knows nothing of its size: told this table's, the
        // optimizer would have the whole table build the join. Each file keeps its own, which is what prunes it.
        if positions.is_some() {
            statistics = Statistics::new_unknown(columns);
        }

        let mut options = state.table_options().parquet.clone();
        // The core's rules read strings as the table has them.
        options.global.schema_force_view_types = false;
        let mut layout = TableSchema::builder(schema.clone());
        if let Some(positions) = positions {
            let at = schema.fields().len();
            layout = layout
                .with_table_partition_cols(vec![positions.fields()[at].clone()])
                .with_virtual_columns(vec![positions.fields()[at + 1].clone()]);
        }
        let source = ParquetSource::new(layout.build())
            .with_table_parquet_options(options)
            .with_parquet_file_reader_factory(Arc::new(TaskReaders {
                storage: storage.clone(),
            }));
        let source = MetaLakeSource::new(Arc::new(source), Arc::new(table.clone()), positions.is_some());
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::parse(storage.base())?, Arc::new(source))
            .with_file_groups(groups)
            .with_statistics(statistics)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_expr_adapter(Some(Arc::new(ByFieldId {
                evolve: Arc::new(Evolve::new(table)),
            })))
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    /// The files in `groups` groups, each a partition of the plan, with the statistics of each group: at least one
    /// group, even an empty one, since a sink needs a partition to drain.
    fn groups(
        schema: &SchemaRef,
        planned: Vec<(PartitionedFile, Statistics)>,
        groups: usize,
    ) -> DataFusionResult<(Vec<FileGroup>, Statistics)> {
        let empty = planned.is_empty();
        let mut files = vec![Vec::new(); groups.clamp(1, planned.len().max(1))];
        let mut statistics = vec![Vec::new(); files.len()];
        let count = files.len();
        for (at, (file, file_statistics)) in planned.into_iter().enumerate() {
            files[at % count].push(file);
            statistics[at % count].push(file_statistics);
        }
        let merged = statistics
            .iter()
            .map(|group| Statistics::try_merge_iter(group, schema))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mut total = Statistics::try_merge_iter(&merged, schema)?;
        if empty {
            total.num_rows = Precision::Exact(0);
        }
        let groups = files
            .into_iter()
            .zip(merged)
            .map(|(files, statistics)| FileGroup::new(files).with_statistics(Arc::new(statistics)))
            .collect();
        Ok((groups, total))
    }
}

#[async_trait]
impl TableProvider for MetaTable {
    fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Every filter is DataFusion's to apply again: pruning leaves out files and row groups, never rows.
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = &self.at_statement().await.map_err(external)?;
        let scan = filters::pruned(Scan::new(&self.catalog, table), table, filters).map_err(external)?;
        self.read(state, table, scan, projection, limit, None).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if insert_op == InsertOp::Replace {
            return not_impl_err!("{insert_op}: a MetaLake table has no key to replace rows by");
        }
        self.on.writable().map_err(external)?;
        let sink = Insert {
            table: self.clone(),
            overwrite: insert_op == InsertOp::Overwrite,
        };
        Ok(Arc::new(MetaWriteExec::new(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))))
    }

    async fn delete_from(&self, state: &dyn Session, filters: Vec<Expr>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let rows = self.selection(state, filters)?;
        Ok(no_input(rows.scan.schema().clone(), Delete { rows }))
    }

    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        combined: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        merge::plan(self, state, source, combined, on, clauses).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let rows = self.selection(state, filters)?;
        let update = Update::new(rows, assignments, state)?;
        Ok(no_input(update.rows.scan.schema().clone(), update))
    }
}

/// A plan of `sink` with nothing to feed it: only DataFusion's way of answering a count, widened into a write's row.
pub(crate) fn no_input(schema: SchemaRef, sink: impl DataSink + 'static) -> Arc<dyn ExecutionPlan> {
    let sink = DataSinkExec::new(Arc::new(EmptyExec::new(schema)), Arc::new(sink), None);
    Arc::new(MetaWriteExec::new(Arc::new(sink)))
}
