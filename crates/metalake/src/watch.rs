//! When the catalog changes, for a face that shows it as it is (ADR 0007, the UI's live catalog). The protocol notifies
//! on its channel `metalake` at each commit and merge (CMT-10), saying its schema; a new ref commits, and so is
//! noticed, but a deleted one notifies nothing, and a notification may be lost while a connection is. So the catalog's
//! [`Mark`] is read again at each notification of its own schema and at an interval that no notification moves: what
//! is said is that the mark moved, never a notification alone.

use std::time::Duration;

use futures_util::future::Either;
use metalake_pg::Listener;
use tokio::time::Instant;

use crate::{Catalog, Result};

/// The channel the protocol notifies on (CMT-10).
const CHANNEL: &str = "metalake";

/// The catalog's state, as far as telling that it changed needs: its latest commit's sequence, which a commit, a merge
/// and a new ref each move; how many commits it has among its latest sequences, which a commit that lands after a
/// later one moves; and how many refs were deleted, which no sequence counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark {
    pub seq: i64,
    pub recent: i64,
    pub deleted: i64,
}

/// The catalog's marks as they move ([`Catalog::watch`]).
#[derive(Debug)]
pub struct Watch {
    catalog: Catalog,
    listener: Option<Listener>,
    every: Duration,
    /// When the mark is read again if nothing of this catalog notified first.
    due: Instant,
    last: Option<Mark>,
}

impl Catalog {
    /// The catalog's mark now.
    pub async fn mark(&self) -> Result<Mark> {
        let (seq, recent, deleted) = self.protocol().mark(&self.session().await?).await?;
        Ok(Mark { seq, recent, deleted })
    }

    /// Its marks as they move: listening on the protocol's channel, on a connection of its own, and reading the mark
    /// again `every` so long, which alone tells where a connection cannot listen (behind a pooler of transactions).
    pub async fn watch(&self, every: Duration) -> Result<Watch> {
        let listener = self.listen().await;
        Ok(Watch {
            catalog: self.clone(),
            listener,
            every,
            due: Instant::now() + every,
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

    /// Until this catalog notifies, or the interval is due. Another catalog's notifications on the same Postgres are
    /// passed over, and none of them puts the interval off: a deleted ref, which notifies nothing, is still read in
    /// time. A connection lost is opened again when the interval is due.
    async fn wait(&mut self) {
        let own = format!("{}:", self.catalog.schema_name());
        loop {
            // A notification's payload, or none when the connection was lost; nothing when the interval came first.
            let heard = {
                let heard = async {
                    match &mut self.listener {
                        Some(listener) => listener.next().await,
                        None => std::future::pending().await,
                    }
                };
                let due = tokio::time::sleep_until(self.due);
                futures_util::pin_mut!(heard, due);
                match futures_util::future::select(heard, due).await {
                    Either::Left((heard, _)) => Some(heard),
                    Either::Right(_) => None,
                }
            };
            match heard {
                Some(Some(payload)) if payload.starts_with(&own) => return,
                Some(Some(_)) => continue,
                Some(None) => {
                    self.listener = None;
                    continue;
                }
                None => {
                    self.due = Instant::now() + self.every;
                    if self.listener.is_none() {
                        self.listener = self.catalog.listen().await;
                    }
                    return;
                }
            }
        }
    }
}
