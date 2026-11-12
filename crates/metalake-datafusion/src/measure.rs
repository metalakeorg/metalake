//! What a physical plan will cost to run, as far as it is known without running it: the bytes and rows its MetaLake
//! scans read, which the protocol planned, after pruning, exactly; and whether it shuffles, which moves rows between
//! the processes of a cluster. A fact about a plan, for a rule to decide on: this decides nothing.

use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;

use crate::exec::MetaScanExec;

/// What a plan reads, and whether it shuffles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Measure {
    /// The bytes of the files its MetaLake scans read: each file's, or its range's.
    pub bytes: u64,
    /// The rows of those files, when every scan knows them; none when one does not.
    pub rows: Option<u64>,
    /// How many MetaLake scans it reads.
    pub scans: usize,
    /// Whether it repartitions rows by hash: a join or an aggregation that, on a cluster, moves rows between executors.
    pub shuffle: bool,
}

/// What `plan` reads, and whether it shuffles.
pub fn measure(plan: &Arc<dyn ExecutionPlan>) -> Measure {
    let mut measure = Measure {
        rows: Some(0),
        ..Measure::default()
    };
    visit(plan, &mut measure);
    measure
}

fn visit(plan: &Arc<dyn ExecutionPlan>, measure: &mut Measure) {
    if let Some(scan) = plan.downcast_ref::<MetaScanExec>() {
        read(scan, measure);
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>()
        && matches!(repartition.partitioning(), Partitioning::Hash(..))
    {
        measure.shuffle = true;
    }
    for child in plan.children() {
        visit(child, measure);
    }
}

/// Adds what `scan` reads to `measure`.
fn read(scan: &MetaScanExec, measure: &mut Measure) {
    measure.scans += 1;
    let Some(config) = scan.source().data_source().downcast_ref::<FileScanConfig>() else {
        measure.rows = None;
        return;
    };
    for group in &config.file_groups {
        for file in group.iter() {
            measure.bytes += match &file.range {
                Some(range) => u64::try_from(range.end - range.start).unwrap_or(0),
                None => file.object_meta.size,
            };
        }
        let rows = group.file_statistics(None).map(|s| s.num_rows);
        measure.rows = match (measure.rows, rows) {
            (Some(sum), Some(Precision::Exact(n) | Precision::Inexact(n))) => Some(sum + n as u64),
            _ => None,
        };
    }
}
