//! What happened on a ref, and what a lake keeps of it. With no policy, a ref keeps every snapshot (RET-1), and nothing is
//! ever taken away.

use std::time::Duration;

use metalake::{CommitEntry, Retention, SnapshotEntry};

use crate::{Error, Lake, Result, table};

/// A duration, in days, hours, minutes or seconds: `30d`, `12h`, `15m`, `45s`.
pub fn duration(text: &str) -> Result<Duration> {
    let unit = text.chars().last().and_then(|unit| match unit {
        'd' => Some(86_400),
        'h' => Some(3_600),
        'm' => Some(60),
        's' => Some(1),
        _ => None,
    });
    let number = text
        .get(..text.len().saturating_sub(1))
        .and_then(|n| n.parse::<u64>().ok());
    match (number, unit) {
        (Some(number), Some(unit)) => Ok(Duration::from_secs(number * unit)),
        _ => Err(Error::usage(format!("{text}: a number and d, h, m or s"))),
    }
}

impl Lake {
    /// The commits of a ref, the latest first.
    pub async fn commits(&self, on: &str, limit: i64) -> Result<Vec<CommitEntry>> {
        Ok(self.find_ref(on).await?.history(limit).await?)
    }

    /// The snapshots of a table of a ref, by `namespace.table`, the latest first.
    pub async fn snapshots(&self, on: &str, name: &str, limit: i64) -> Result<Vec<SnapshotEntry>> {
        let on = self.find_ref(on).await?;
        let table = table(&on, name).await?;
        Ok(on.snapshots(&table, limit).await?)
    }

    /// Says what a ref keeps, which `maintain` then acts on. It replaces all a ref said before, and a ref that says
    /// nothing keeps everything, so it must say something.
    pub async fn retain(&self, on: &str, retention: &Retention) -> Result<()> {
        if retention.max_age.is_none() && retention.min_snapshots.is_none() && retention.min_seq.is_none() {
            return Err(Error::usage(
                "a ref needs what to keep: an age, a number of snapshots or a sequence",
            ));
        }
        Ok(self.find_ref(on).await?.set_retention(retention).await?)
    }

    /// Says how long an expired object waits before it is deleted, which reads should end within.
    pub async fn set_grace(&self, grace: Duration) -> Result<()> {
        Ok(self.catalog.set_collect_grace(grace).await?)
    }
}
