//! What a physical plan will cost to run, as far as it is known without running it: the bytes and rows its MetaLake
//! reads take, which the protocol planned, after pruning, exactly; and whether it shuffles, which moves rows between
//! the processes of a cluster. A fact about a plan, for a rule to decide on: this decides nothing.
//!
//! A read is a `MetaScanExec` in the plan, or the scan a `delete`, an `update` or a `merge` holds in its sink, outside
//! the plan's children: DataFusion gives those statements no input. Those are planned here, as their sink will.

use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use futures_util::TryStreamExt;
use metalake::Scan;

use crate::catalog::external;
use crate::exec::MetaScanExec;
use crate::merge::Merge;
use crate::sink::{Delete, Update};

/// What a plan reads, and whether it shuffles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Measure {
    /// The bytes of the files its MetaLake reads take: each file's, or its range's.
    pub bytes: u64,
    /// The rows of those files, when every read knows them; none when one does not.
    pub rows: Option<u64>,
    /// How many MetaLake reads it holds.
    pub scans: usize,
    /// Whether it moves rows by key between partitions: a hash repartition, or a merge's join of the table and its
    /// source; on a cluster, between executors.
    pub shuffle: bool,
}

/// What `plan` reads, and whether it shuffles. A write's own reads are planned with the protocol, in Postgres.
pub async fn measure(plan: &Arc<dyn ExecutionPlan>) -> Result<Measure> {
    let mut measure = Measure {
        rows: Some(0),
        ..Measure::default()
    };
    let mut held = Vec::new();
    visit(plan, &mut measure, &mut held);
    for scan in held {
        let tasks: Vec<_> = scan.plan().await.map_err(external)?.try_collect().await.map_err(external)?;
        measure.scans += 1;
        for task in tasks {
            measure.bytes += u64::try_from(task.file_size).unwrap_or(0);
            measure.rows = measure.rows.map(|rows| rows + u64::try_from(task.record_count).unwrap_or(0));
        }
    }
    Ok(measure)
}

/// Adds what `plan` reads to `measure`, and the scans its writes hold, to plan, to `held`.
fn visit(plan: &Arc<dyn ExecutionPlan>, measure: &mut Measure, held: &mut Vec<Scan>) {
    if let Some(scan) = plan.downcast_ref::<MetaScanExec>() {
        read(scan, measure);
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>()
        && matches!(repartition.partitioning(), Partitioning::Hash(..))
    {
        measure.shuffle = true;
    }
    if let Some(sink) = plan.downcast_ref::<DataSinkExec>().map(DataSinkExec::sink) {
        if let Some(delete) = sink.downcast_ref::<Delete>() {
            held.push(delete.rows.scan.clone());
        }
        if let Some(update) = sink.downcast_ref::<Update>() {
            held.push(update.rows.scan.clone());
        }
        if let Some(merge) = sink.downcast_ref::<Merge>() {
            // The table, whole, which its join reads, and the source, a plan of its own.
            held.push(Scan::new(&merge.table.catalog, &merge.table.table));
            measure.shuffle = true;
            visit(&merge.source, measure, held);
        }
    }
    for child in plan.children() {
        visit(child, measure, held);
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
