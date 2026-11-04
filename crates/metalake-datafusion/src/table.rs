//! A table: DataFusion's `scan` is where the protocol plans, so that the optimizer sees the files and their statistics
//! before anything runs.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, Statistics};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures_util::TryStreamExt;
use metalake::{Catalog, Evolve, Scan, Storage, Table};

use crate::adapter::ByFieldId;
use crate::catalog::external;
use crate::filters;
use crate::read::{self, TaskReaders};

/// How many files' vectors are read at once while planning.
const CONCURRENCY: usize = 16;

/// A table of a ref, at the point it was read.
#[derive(Clone)]
pub struct MetaTable {
    catalog: Catalog,
    storage: Storage,
    table: Table,
}

impl fmt::Debug for MetaTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaTable").field("name", &self.table.name()).finish()
    }
}

impl MetaTable {
    pub(crate) fn new(catalog: Catalog, storage: Storage, table: Table) -> Self {
        Self {
            catalog,
            storage,
            table,
        }
    }

    /// The files in `groups` groups, each a partition of the plan, with the statistics of each group.
    fn groups(
        &self,
        planned: Vec<(PartitionedFile, Statistics)>,
        groups: usize,
    ) -> DataFusionResult<(Vec<FileGroup>, Statistics)> {
        let schema = self.table.schema();
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
            .filter(|(files, _)| !files.is_empty())
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
        let (table, storage) = (&self.table, &self.storage);
        let schema = table.schema();
        let scan = filters::pruned(Scan::new(&self.catalog, table), table, filters).map_err(external)?;
        let planned: Vec<_> = scan
            .plan()
            .await
            .map_err(external)?
            .map_err(external)
            .map_ok(|task| read::file(task, schema, storage))
            .try_buffered(CONCURRENCY)
            .try_collect()
            .await?;
        let (groups, statistics) = self.groups(planned, state.config().target_partitions())?;

        let mut options = state.table_options().parquet.clone();
        // The core's rules read strings as the table has them.
        options.global.schema_force_view_types = false;
        let mut source = ParquetSource::new(schema.clone())
            .with_table_parquet_options(options)
            .with_parquet_file_reader_factory(Arc::new(TaskReaders {
                storage: storage.clone(),
            }));
        if let Some(filter) = conjunction(filters.to_vec()) {
            let names = DFSchema::try_from(schema.clone())?;
            source = source.with_predicate(state.create_physical_expr(filter, &names)?);
        }
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
}
