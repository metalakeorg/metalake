//! The scan of a MetaLake table as a plan node of its own. Inside it is DataFusion's `DataSourceExec`, which reads; around
//! it, a node DataFusion does not know how to serialize, so that a plan which crosses processes is encoded by MetaLake's
//! codec, which keeps what DataFusion's own encoding drops: each file's task, its vector, the reader and the columns by
//! field id. Every rewrite the optimizer asks of the scan is the inner node's, and its result is wrapped again.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult, StatisticsArgs,
};

/// A MetaLake table's scan: DataFusion's `DataSourceExec` inside.
#[derive(Debug, Clone)]
pub(crate) struct MetaScanExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl MetaScanExec {
    pub(crate) fn new(inner: Arc<DataSourceExec>) -> Self {
        Self { inner }
    }

    /// The node that reads.
    pub(crate) fn source(&self) -> &DataSourceExec {
        self.inner
            .downcast_ref::<DataSourceExec>()
            .expect("a scan wraps a DataSourceExec")
    }

    /// `plan`, the inner node rewritten, as a scan again; anything else as it is.
    fn wrap(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        match plan.downcast_ref::<DataSourceExec>() {
            Some(_) => Arc::new(Self { inner: plan }),
            None => plan,
        }
    }
}

impl DisplayAs for MetaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !matches!(t, DisplayFormatType::TreeRender) {
            write!(f, "MetaScanExec: ")?;
        }
        self.source().data_source().fmt_as(t, f)
    }
}

impl ExecutionPlan for MetaScanExec {
    fn name(&self) -> &'static str {
        "MetaScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self.inner.repartitioned(target_partitions, config)?.map(Self::wrap))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit).map(Self::wrap)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn try_swapping_with_projection(&self, projection: &ProjectionExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self.inner.try_swapping_with_projection(projection)?.map(Self::wrap))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let mut result = self
            .inner
            .handle_child_pushdown_result(phase, child_pushdown_result, config)?;
        result.updated_node = result.updated_node.map(Self::wrap);
        Ok(result)
    }

    fn try_pushdown_sort(&self, order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        self.inner
            .try_pushdown_sort(order)?
            .try_map(|plan| Ok(Self::wrap(plan)))
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_preserve_order(preserve_order).map(Self::wrap)
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_new_state(state).map(Self::wrap)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(Arc::clone(&self.inner).reset_state()?))
    }
}
