//! `metalake log`: what happened on a ref. A sequence here is what `"main@1234"` reads and `--from main@1234` starts from.

use serde_json::{Value, json};

use crate::args::Log;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(log: &Log, lake: &Lake, out: &Out) -> Result<(), Failure> {
    let refs = lake.catalog.refs().await?;
    let on = refs
        .iter()
        .find(|r| r.name() == log.on)
        .ok_or_else(|| Failure::failed(format!("no ref {}", log.on)))?;
    let limit = i64::from(log.limit);
    if let Some(table) = &log.table {
        let table = super::table(on, table).await?;
        let snapshots = on.snapshots(&table, limit).await?;
        let rows: Vec<Value> = snapshots
            .iter()
            .map(|s| {
                json!({
                    "snapshot_id": s.snapshot_id,
                    "parent_snapshot_id": s.parent_snapshot_id,
                    "operation": s.operation,
                    "committed_at": s.committed_at,
                })
            })
            .collect();
        let columns = ["snapshot_id", "parent_snapshot_id", "operation", "committed_at"];
        return out.records(&columns, &rows).await;
    }
    let commits = on.history(limit).await?;
    let rows: Vec<Value> = commits
        .iter()
        .map(|c| {
            json!({
                "commit_seq": c.commit_seq,
                "committed_at": c.committed_at,
                "author": c.author,
                "operation": c.operation,
                "message": c.message,
            })
        })
        .collect();
    let columns = ["commit_seq", "committed_at", "author", "operation", "message"];
    out.records(&columns, &rows).await
}
