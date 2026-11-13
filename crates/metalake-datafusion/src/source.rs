//! A MetaLake table's files as DataFusion reads them. Inside, Parquet's source, which already reads each file through
//! its task (`TaskReaders`) and its columns by field id (`ByFieldId`, on the scan's configuration); around it, what
//! MetaLake's encoding needs besides: the table and whether rows say their file and position.
//!
//! To DataFusion and Ballista the scan is a `DataSourceExec` over a `FileScanConfig` like any other: they plan, push
//! down, split and restrict it with their own rules. To the codec it is MetaLake's: this source does not encode itself,
//! so DataFusion's encoding hands the whole `DataSourceExec` to the extension codec, which keeps what DataFusion's own
//! encoding drops.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion_datasource::morsel::Morselizer;
use metalake::Table;
use object_store::ObjectStore;

/// A MetaLake table's files: Parquet's source, the table, and whether rows say their file and position.
#[derive(Clone)]
pub(crate) struct MetaLakeSource {
    parquet: Arc<dyn FileSource>,
    table: Arc<Table>,
    positions: bool,
}

impl MetaLakeSource {
    pub(crate) fn new(parquet: Arc<dyn FileSource>, table: Arc<Table>, positions: bool) -> Self {
        Self {
            parquet,
            table,
            positions,
        }
    }

    /// The source that reads, Parquet's.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub(crate) fn parquet(&self) -> &Arc<dyn FileSource> {
        &self.parquet
    }

    /// The table read, whose fields match the files' columns by id.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub(crate) fn table(&self) -> &Arc<Table> {
        &self.table
    }

    /// Whether each row also says its file and position, for a statement that goes on to write.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub(crate) fn positions(&self) -> bool {
        self.positions
    }

    /// `parquet`, rewritten by DataFusion, as this source again.
    fn wrap(&self, parquet: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self {
            parquet,
            ..self.clone()
        })
    }
}

impl fmt::Debug for MetaLakeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaLakeSource")
            .field("table", &self.table.name())
            .field("positions", &self.positions)
            .finish()
    }
}

/// The `MetaLakeSource` of a scan's configuration, if it is one.
pub(crate) fn of(config: &FileScanConfig) -> Option<&MetaLakeSource> {
    let source: &dyn std::any::Any = config.file_source().as_ref();
    source.downcast_ref::<MetaLakeSource>()
}

impl FileSource for MetaLakeSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.parquet.create_file_opener(object_store, base_config, partition)
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        self.parquet.create_morselizer(object_store, base_config, partition)
    }

    fn table_schema(&self) -> &TableSchema {
        self.parquet.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        self.wrap(self.parquet.with_batch_size(batch_size))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.parquet.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.parquet.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.parquet.metrics()
    }

    /// The files' format: Parquet's, as DataFusion's metrics of a Parquet scan ask.
    fn file_type(&self) -> &str {
        self.parquet.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ", metalake={}", self.table.name())?;
        self.parquet.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.parquet.supports_repartitioning()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut pushed = self.parquet.try_pushdown_filters(filters, config)?;
        pushed.updated_node = pushed.updated_node.map(|parquet| self.wrap(parquet));
        Ok(pushed)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        let pushed = self.parquet.try_pushdown_sort(order, eq_properties)?;
        Ok(pushed.map(|parquet| self.wrap(parquet)))
    }

    fn reorder_files(&self, files: Vec<datafusion::datasource::listing::PartitionedFile>) -> Vec<datafusion::datasource::listing::PartitionedFile> {
        self.parquet.reorder_files(files)
    }

    fn try_pushdown_projection(&self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>> {
        let pushed = self.parquet.try_pushdown_projection(projection)?;
        Ok(pushed.map(|parquet| self.wrap(parquet)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.parquet.apply_expressions(f)
    }
}
