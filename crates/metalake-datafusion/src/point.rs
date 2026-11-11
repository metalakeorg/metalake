//! Where a statement reads. A catalog's name may carry a point, `"main@1234"` a sequence and
//! `"main@2026-11-04T10:00:00Z"` a time, with no change to the SQL parser, so any client can say it.
//!
//! A statement's tables must agree, but DataFusion looks them up one by one, and a commit could land between two
//! lookups. This rule runs once on the statement's whole plan and shares one [`StatementPoint`] among the tables that
//! read the latest state: the first of them to plan takes all their sequences in one query (`Catalog::latest`), so that
//! a commit is seen by all of them or by none.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use uuid::Uuid;

use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use metalake::{Error, LatestPoints, Ref};
use tokio::sync::OnceCell;

use crate::table::MetaTable;

/// Where a statement reads each of its tables, taken once, by the first of them to plan.
#[derive(Debug)]
pub(crate) struct StatementPoint {
    /// Which statement: the tables of one plan that crosses processes share the point of this id again.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub id: Uuid,
    pub refs: Vec<i32>,
    pub points: OnceCell<LatestPoints>,
}

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
        let mut refs = Vec::new();
        plan.apply_with_subqueries(|node| {
            refs.extend(latest(node).map(|table| table.on.id()));
            Ok(TreeNodeRecursion::Continue)
        })?;
        if refs.len() < 2 {
            return Ok(plan);
        }
        refs.sort_unstable();
        refs.dedup();
        let point = Arc::new(StatementPoint {
            id: Uuid::new_v4(),
            refs,
            points: OnceCell::new(),
        });
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
