//! Following the catalog: the change feed of every table (CHG-1), from now, and the lead among the processes that follow
//! it, so that one of them does what each would. A [`crate::TableStream`] follows one table's rows; this follows which
//! commits there are, and leaves what to do with them to its caller.

use std::time::{Duration, SystemTime};

use metalake_pg::records::FeedCommit;
use metalake_pg::{Pool, PooledSession, Protocol};

use crate::catalog::Catalog;
use crate::error::Result;

/// How long taking the lead waits for its connection, which is its own.
const CONNECT: Duration = Duration::from_secs(10);

/// A place in the catalog's change feed: the commits after it are those to take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedCursor {
    /// The feed's own cursor (CHG-1): the time and sequence of the last commit taken.
    at: SystemTime,
    seq: i64,
    /// The commits at or below it were in the state the cursor started from (CHG-4).
    floor: i64,
}

impl FeedCursor {
    /// The cursor past `commit`, one the feed delivered after this one.
    pub fn after(&self, commit: &FeedCommit) -> Self {
        Self {
            at: commit.committed_at,
            seq: commit.commit_seq,
            floor: self.floor,
        }
    }
}

impl Catalog {
    /// A cursor at the catalog as it is now: the feed after it delivers each commit that came after, once, in order,
    /// with no gaps (CHG-1, CHG-4).
    pub async fn feed_start(&self) -> Result<FeedCursor> {
        let (at, seq) = self.stable_point().await?;
        Ok(FeedCursor { at, seq: 0, floor: seq })
    }

    /// The commits after `cursor`, of every table and ref, `at_most` of them, in the feed's order (CHG-1). A cursor past
    /// the last of them takes the rest. Those collected by retention are not delivered (RET-3).
    pub async fn feed(&self, cursor: &FeedCursor, at_most: usize) -> Result<Vec<FeedCommit>> {
        let session = self.session().await?;
        let after = Some((cursor.at, cursor.seq));
        let lim = i32::try_from(at_most).unwrap_or(i32::MAX);
        let mut commits = self.protocol().changes(&session, after, Some(lim), None).await?;
        commits.retain(|c| c.commit_seq > cursor.floor);
        Ok(commits)
    }

    /// The lead named `name` among the processes connected to this catalog, if no other holds it: one of them takes
    /// it, and holds it while its [`Lead`] lives and its connection does. It takes a connection of its own, as the
    /// lock is a session's (Postgres's advisory lock).
    pub async fn lead(&self, name: &str) -> Result<Option<Lead>> {
        let pool = Pool::waiting(self.postgres(), 1, CONNECT)?;
        let session = pool.get().await?;
        let protocol = self.protocol().clone();
        if !protocol.try_lead(&session, name).await? {
            return Ok(None);
        }
        Ok(Some(Lead {
            name: name.to_string(),
            protocol,
            session,
            pool,
        }))
    }
}

/// The lead among a catalog's processes, held while this lives. Dropping it closes its connection, which releases it.
#[derive(Debug)]
pub struct Lead {
    name: String,
    protocol: Protocol,
    session: PooledSession,
    pool: Pool,
}

impl Lead {
    /// Whether it is still held: a connection that was lost lost the lead with it, and another process may have it.
    pub async fn held(&self) -> bool {
        self.protocol
            .holds_lead(&self.session, &self.name)
            .await
            .unwrap_or(false)
    }

    /// Gives it up, for another process to take at once.
    pub async fn release(self) -> Result<()> {
        self.protocol.release_lead(&self.session, &self.name).await?;
        self.pool.close();
        Ok(())
    }
}

impl Drop for Lead {
    fn drop(&mut self) {
        // The connection goes with the pool, and the lock with the connection.
        self.pool.close();
    }
}
