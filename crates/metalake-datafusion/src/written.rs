//! What a write answers: its count of rows, as DataFusion's, and what came of its commit. The commit happens where the
//! write runs, in the client's process or on a cluster's executor, so what came of it travels back as the write's own
//! row, the way any result does, and not in memory, which stays in the process that holds it.
//!
//! Two pieces. `MetaWriteExec` wraps DataFusion's `DataSinkExec`: it hands the sink, where it runs, the slot its commit
//! fills, and widens the count into the row. `Written`, an analyzer rule, widens the logical plan of a write on a
//! MetaLake table the same way, so that what the plan says it returns is what it does, here and on a cluster.

use std::fmt;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion::datasource::source_as_provider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::WriteOp;
use datafusion::logical_expr::{DmlStatement, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::StreamExt;
use metalake::Outcome;

use crate::details::CommitDetails;
use crate::table::MetaTable;

/// The row a write on a MetaLake table answers.
pub fn written_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // DataFusion's own.
            Field::new("count", DataType::UInt64, false),
            // The sequence of its commit, or of the one that had applied the same writer's key; none when no row
            // changed.
            Field::new("commit", DataType::Int64, true),
            // Whether this write applied it, or found it applied (CMT-8).
            Field::new("applied", DataType::Boolean, false),
            // Of `count`, the rows a merge inserted and those it updated.
            Field::new("inserted", DataType::UInt64, false),
            Field::new("updated", DataType::UInt64, false),
        ]))
    });
    SCHEMA.clone()
}

/// A write as its row says it: how many rows, and what came of its commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrittenRow {
    pub count: u64,
    pub outcome: Option<Outcome>,
    pub inserted: u64,
    pub updated: u64,
}

impl WrittenRow {
    /// The row of `batch`, a write's answer.
    pub fn of(batch: &RecordBatch) -> Result<Self> {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| DataFusionError::Plan(format!("a write's answer has no column {name}")))
        };
        let commit = column("commit")?.as_primitive::<Int64Type>();
        let outcome = (!commit.is_null(0)).then(|| match column("applied").map(|a| a.as_boolean().value(0)) {
            Ok(false) => Outcome::AlreadyApplied(commit.value(0)),
            _ => Outcome::Committed(commit.value(0)),
        });
        Ok(Self {
            count: column("count")?.as_primitive::<UInt64Type>().value(0),
            outcome,
            inserted: column("inserted")?.as_primitive::<UInt64Type>().value(0),
            updated: column("updated")?.as_primitive::<UInt64Type>().value(0),
        })
    }
}

/// A write on a MetaLake table: DataFusion's `DataSinkExec` inside, which writes and commits, and its count widened
/// into the row of `written_schema`.
#[derive(Debug)]
pub(crate) struct MetaWriteExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl MetaWriteExec {
    pub(crate) fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let of = inner.properties();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(written_schema()),
            of.partitioning.clone(),
            of.emission_type,
            of.boundedness,
        );
        Self {
            inner,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for MetaWriteExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaWriteExec")
    }
}

impl ExecutionPlan for MetaWriteExec {
    fn name(&self) -> &'static str {
        "MetaWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    /// The sink, one partition, right under it: a repartition between them would run the sink with the slot of one
    /// partition and send its count out of another, whose slot is empty.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// It holds no expressions.
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // The slot the sink's commit fills, made where it runs.
        let slot = Arc::new(CommitDetails::default());
        let config = context.session_config().clone().with_extension(slot.clone());
        let task = TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        );
        let counted = self.inner.execute(partition, Arc::new(task))?;
        let rows = counted.map(move |batch| widen(&batch?, &slot));
        Ok(Box::pin(RecordBatchStreamAdapter::new(written_schema(), rows)))
    }
}

/// `batch`, DataFusion's count, with what `slot` says of the commit.
fn widen(batch: &RecordBatch, slot: &CommitDetails) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let outcome = slot.outcome();
    let (inserted, updated) = slot.merged();
    let columns: Vec<ArrayRef> = vec![
        batch.column(0).clone(),
        Arc::new(Int64Array::from(vec![outcome.map(Outcome::seq); rows])),
        Arc::new(BooleanArray::from(vec![matches!(outcome, Some(Outcome::Committed(_))); rows])),
        Arc::new(UInt64Array::from(vec![inserted; rows])),
        Arc::new(UInt64Array::from(vec![updated; rows])),
    ];
    Ok(RecordBatch::try_new(written_schema(), columns)?)
}

/// Widens the logical plan of a write on a MetaLake table into the row `MetaWriteExec` answers.
#[derive(Debug, Default)]
pub(crate) struct Written;

impl AnalyzerRule for Written {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::Dml(dml) if writes_metalake(&dml)? => {
                let output_schema: DFSchemaRef = Arc::new(DFSchema::try_from(written_schema())?);
                if dml.output_schema == output_schema {
                    return Ok(Transformed::no(LogicalPlan::Dml(dml)));
                }
                Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement { output_schema, ..dml })))
            }
            plan => Ok(Transformed::no(plan)),
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "metalake_written"
    }
}

/// Whether `dml` is a write MetaLake's table answers with its row.
fn writes_metalake(dml: &DmlStatement) -> Result<bool> {
    let write = matches!(
        dml.op,
        WriteOp::Insert(_) | WriteOp::Delete | WriteOp::Update | WriteOp::MergeInto(_)
    );
    Ok(write && source_as_provider(&dml.target)?.downcast_ref::<MetaTable>().is_some())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::datasource::sink::{DataSink, DataSinkExec};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionConfig;

    use super::*;

    /// A sink that answers three rows, and says a merge inserted one and updated two, in its execution's slot.
    #[derive(Debug)]
    struct Counted(SchemaRef);

    impl DisplayAs for Counted {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Counted")
        }
    }

    #[async_trait]
    impl DataSink for Counted {
        fn schema(&self) -> &SchemaRef {
            &self.0
        }

        async fn write_all(&self, _: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
            crate::details::merged(context, 1, 2);
            Ok(3)
        }
    }

    /// Optimized in a session of many partitions, the sink stays right under the write's row, and the row says what its
    /// slot holds: a repartition between them would send the count out of a partition whose slot is empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn slot_reaches_the_row_in_many_partitions() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let sink = DataSinkExec::new(Arc::new(EmptyExec::new(schema.clone())), Arc::new(Counted(schema)), None);
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(MetaWriteExec::new(Arc::new(sink)));
        let config = SessionConfig::new().with_target_partitions(16);
        let state = SessionStateBuilder::new().with_config(config).with_default_features().build();
        for rule in state.physical_optimizers() {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(plan.children()[0].name(), "DataSinkExec", "{shown}");
        for _ in 0..20 {
            let batches = collect(plan.clone(), Arc::new(TaskContext::from(&state))).await?;
            let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
            let row = WrittenRow::of(batch)?;
            assert_eq!((row.count, row.inserted, row.updated), (3, 1, 2), "{shown}");
        }
        Ok(())
    }
}
