//! Where a statement reads. A catalog's name may carry a point, `"main@1234"` a sequence and
//! `"main@2026-11-04T10:00:00Z"` a time, with no change to the SQL parser, so any client can say it.
//!
//! A statement reads one point (VIS-11), but DataFusion looks its tables up one by one, and each would take its own.
//! This rule runs once on the statement's whole plan and gives the tables that read the latest state one point, which
//! the first of them to plan takes, so that a commit between two tables' lookups is seen by both or by neither.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use metalake::{Error, Ref};
use tokio::sync::OnceCell;

use crate::table::MetaTable;

/// The one point of a statement, taken once.
pub(crate) type StatementPoint = Arc<OnceCell<i64>>;

/// `on`, read at the point `name` carries after its `@`.
pub(crate) fn at(on: Ref, point: &str) -> metalake::Result<Ref> {
    if let Ok(seq) = point.parse() {
        return Ok(on.at(seq));
    }
    let nanos = string_to_timestamp_nanos(point)
        .map_err(|e| Error::Invalid(format!("{point} is neither a sequence nor a time: {e}")))?;
    Ok(on.as_of(UNIX_EPOCH + Duration::from_nanos(nanos.max(0) as u64)))
}

/// Gives a statement's tables one read point.
#[derive(Debug)]
pub(crate) struct OnePoint;

/// The table a node scans, if it is ours and reads the latest state.
fn latest(node: &LogicalPlan) -> Option<MetaTable> {
    let LogicalPlan::TableScan(scan) = node else {
        return None;
    };
    let provider = source_as_provider(&scan.source).ok()?;
    let table = provider.downcast_ref::<MetaTable>()?;
    table.on.is_latest().then(|| table.clone())
}

impl AnalyzerRule for OnePoint {
    fn name(&self) -> &str {
        "metalake_one_point"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let mut tables = 0;
        plan.apply_with_subqueries(|node| {
            tables += usize::from(latest(node).is_some());
            Ok(TreeNodeRecursion::Continue)
        })?;
        if tables < 2 {
            return Ok(plan);
        }
        let point = StatementPoint::default();
        plan.transform_up_with_subqueries(|node| {
            let Some(table) = latest(&node) else {
                return Ok(Transformed::no(node));
            };
            let LogicalPlan::TableScan(mut scan) = node else {
                unreachable!("a table comes from a scan");
            };
            scan.source = provider_as_source(Arc::new(MetaTable {
                statement: Some(point.clone()),
                ..table
            }));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })
        .data()
    }
}
