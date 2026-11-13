//! When the catalog changes, for a face that shows it as it is (ADR 0007, the UI's live catalog). The protocol notifies
//! on its channel `metalake` at each commit and merge (CMT-10); a new ref commits, and so is noticed, but a deleted one
//! notifies nothing, and a notification may be lost while a connection is. So the catalog's [`Mark`] is read again at
//! each notification and at an interval: what is said is that the mark moved, never a notification alone.

use std::time::Duration;

use futures_util::future::Either;
use metalake_pg::Listener;

use crate::{Catalog, Result};

/// The channel the protocol notifies on (CMT-10).
const CHANNEL: &str = "metalake";

/// The catalog's state, as far as telling that it changed needs: its latest commit's sequence, which a commit, a merge
/// and a new ref each move, and how many refs were deleted, which no sequence counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark {
    pub seq: i64,
    pub deleted: i64,
}

/// The catalog's marks as they move ([`Catalog::watch`]).
#[derive(Debug)]
pub struct Watch {
    catalog: Catalog,
    listener: Option<Listener>,
    every: Duration,
    last: Option<Mark>,
}

impl Catalog {
    /// The catalog's mark now.
    pub async fn mark(&self) -> Result<Mark> {
        let (seq, deleted) = self.protocol().mark(&self.session().await?).await?;
        Ok(Mark { seq, deleted })
    }

    /// Its marks as they move: listening on the protocol's channel, on a connection of its own, and reading the mark
    /// again `every` so long, which alone tells where a connection cannot listen (behind a pooler of transactions).
    pub async fn watch(&self, every: Duration) -> Result<Watch> {
        let listener = self.listen().await;
        Ok(Watch {
            catalog: self.clone(),
            listener,
            every,
            last: None,
        })
    }

    async fn listen(&self) -> Option<Listener> {
        self.pool().listen(CHANNEL).await.ok()
    }
}

impl Watch {
    /// The mark now, the first time; after, the next mark that differs from the last said, waiting for it.
    pub async fn next(&mut self) -> Result<Mark> {
        loop {
            if self.last.is_some() {
                self.wait().await;
            }
            let mark = self.catalog.mark().await?;
            if self.last != Some(mark) {
                self.last = Some(mark);
                return Ok(mark);
            }
        }
    }

    /// Whether a notification reaches it now, or the interval alone does.
    pub fn listening(&self) -> bool {
        self.listener.is_some()
    }

    /// Until a notification comes, or the interval passes. A connection lost is opened again at the next interval.
    async fn wait(&mut self) {
        // Whether a notification came, and the connection is still alive; none when the interval passed first.
        let notified = {
            let notified = async {
                match &mut self.listener {
                    Some(listener) => listener.next().await.is_some(),
                    None => std::future::pending().await,
                }
            };
            let interval = tokio::time::sleep(self.every);
            futures_util::pin_mut!(notified, interval);
            match futures_util::future::select(notified, interval).await {
                Either::Left((alive, _)) => Some(alive),
                Either::Right(_) => None,
            }
        };
        match notified {
            Some(true) => {}
            Some(false) => self.listener = None,
            None if self.listener.is_none() => self.listener = self.catalog.listen().await,
            None => {}
        }
    }
}
