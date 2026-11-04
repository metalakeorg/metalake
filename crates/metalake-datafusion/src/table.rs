//! A table: DataFusion's `scan` is where the protocol plans, so that the optimizer sees the files and their statistics
//! before anything runs.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures_util::TryStreamExt;
use metalake::{Catalog, Scan, ScanTask, Storage, Table};

use crate::catalog::external;
use crate::stats;

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

    /// Where DataFusion finds a task's file: an inline file has no object, and takes a name only it uses.
    fn file(&self, task: &ScanTask) -> DataFusionResult<PartitionedFile> {
        let size = task.file_size as u64;
        Ok(match &task.path {
            Some(url) => PartitionedFile::new(self.storage.path(url).map_err(external)?.to_string(), size),
            None => PartitionedFile::new(format!("inline/{}", task.file_id), size),
        })
    }

    /// The files in `groups` groups, each a partition of the plan, with the statistics of each group.
    fn groups(&self, tasks: &[ScanTask], groups: usize) -> DataFusionResult<(Vec<FileGroup>, Statistics)> {
        let schema = self.table.schema();
        let mut files = vec![Vec::new(); groups.clamp(1, tasks.len().max(1))];
        let mut statistics = vec![Vec::new(); files.len()];
        let count = files.len();
        for (at, task) in tasks.iter().enumerate() {
            files[at % count].push(self.file(task)?);
            statistics[at % count].push(stats::of_file(task, schema)?);
        }
        let merged = statistics
            .iter()
            .map(|group| Statistics::try_merge_iter(group, schema))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mut total = Statistics::try_merge_iter(&merged, schema)?;
        if tasks.is_empty() {
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let scan = Scan::new(&self.catalog, &self.table);
        let tasks: Vec<ScanTask> = scan
            .plan()
            .await
            .map_err(external)?
            .try_collect()
            .await
            .map_err(external)?;
        let (groups, statistics) = self.groups(&tasks, state.config().target_partitions())?;
        let source = Arc::new(ParquetSource::new(self.table.schema().clone()));
        let url = ObjectStoreUrl::parse(self.storage.base())?;
        let config = FileScanConfigBuilder::new(url, source)
            .with_file_groups(groups)
            .with_statistics(statistics)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}
