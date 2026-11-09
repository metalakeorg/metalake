//! What the worker that materializes on commit decides, apart from what it does: told which tables are marked, which
//! commits touched which tables, how each load ended and what time it is, it answers which tables to load and how long
//! to wait before looking at the feed again. It holds nothing that outlives the process: a worker that starts again
//! loads every table marked once, and that brings each up to date.
//!
//! - A table is loaded, not a commit: one load generates every snapshot a table is missing (REST-6). A table is marked
//!   on a ref, so a commit of it there makes it due, and one on a ref where it is not marked does not.
//! - A table is loaded once at a time. A commit of it while it loads only says to load it again once that load is done,
//!   however many commits come: what is due is a set, and never grows with the commits.
//! - At most [`AT_ONCE`] tables load at once, and a load that fails is tried again after a wait that doubles.
//! - With no table marked the feed is looked at seldom; with some, every second while commits come, and less often as
//!   reads find none.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use uuid::Uuid;

/// Tables loaded at once at most.
pub(crate) const AT_ONCE: usize = 4;
/// How often the feed is looked at while commits come: it delivers a commit one second after it (CHG-1).
pub(crate) const ACTIVE: Duration = Duration::from_secs(1);
/// How seldom it is looked at, at most, while reads find nothing.
pub(crate) const QUIET: Duration = Duration::from_secs(30);
/// How often a worker with no table marked looks for one.
pub(crate) const UNMARKED: Duration = Duration::from_secs(60);
/// The first wait after a load that failed, which doubles with each failure, up to [`RETRY_MAX`].
pub(crate) const RETRY: Duration = Duration::from_secs(1);
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(60);

/// What a table that is not idle is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Loading; `again` once a commit of it came meanwhile. `failures` before this load, in a row.
    Loading { again: bool, failures: u32 },
    /// Waiting to be tried again after `failures` loads that failed in a row.
    Waiting { until: Instant, failures: u32 },
}

/// The worker's decisions.
#[derive(Debug)]
pub(crate) struct Plan {
    /// Each table marked, and the refs it is marked on.
    marked: BTreeMap<Uuid, BTreeSet<i32>>,
    /// The tables that are not idle.
    states: BTreeMap<Uuid, State>,
    /// The tables to load as soon as there is room, with the failures in a row each comes from.
    due: BTreeMap<Uuid, u32>,
    /// How long to wait before looking at the feed again.
    pause: Duration,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            marked: BTreeMap::new(),
            states: BTreeMap::new(),
            due: BTreeMap::new(),
            pause: UNMARKED,
        }
    }
}

impl Plan {
    /// The refs and tables marked now, as the protocol lists them (`tables_with_property`). A table newly marked is due,
    /// so that what it misses is generated; one no longer marked is left, once a load of it is done.
    pub(crate) fn marked(&mut self, now: impl IntoIterator<Item = (i32, Uuid)>) {
        let mut marked: BTreeMap<Uuid, BTreeSet<i32>> = BTreeMap::new();
        for (ref_id, table) in now {
            marked.entry(table).or_default().insert(ref_id);
        }
        let entered: Vec<Uuid> = marked
            .keys()
            .filter(|t| !self.marked.contains_key(t))
            .copied()
            .collect();
        self.marked = marked;
        for table in entered {
            self.make_due(table);
        }
        let marked = &self.marked;
        self.due.retain(|table, _| marked.contains_key(table));
        self.states
            .retain(|table, state| marked.contains_key(table) || matches!(state, State::Loading { .. }));
        if self.marked.is_empty() {
            self.pause = UNMARKED;
        } else if self.pause == UNMARKED {
            self.pause = ACTIVE;
        }
    }

    /// What a read of the feed found: each commit's ref and a table it made a snapshot of (`commit_tables`). A table
    /// marked on that ref is due. Commits of a table marked keep the feed looked at every second, and reads with none
    /// make it seldom: commits of tables not marked are no reason to look sooner.
    pub(crate) fn committed(&mut self, touched: &[(i32, Uuid)]) {
        let mut any = false;
        for (ref_id, table) in touched {
            if self.marked.get(table).is_some_and(|refs| refs.contains(ref_id)) {
                self.make_due(*table);
                any = true;
            }
        }
        self.pause = match (self.marked.is_empty(), any) {
            (true, _) => UNMARKED,
            (false, true) => ACTIVE,
            (false, false) => (self.pause * 2).min(QUIET),
        };
    }

    /// A table was marked through the service itself: the feed is looked at again at once.
    pub(crate) fn woken(&mut self) {
        self.pause = ACTIVE;
    }

    /// A load of `table` ended, at `now`. Done, it is loaded again if a commit came meanwhile; failed, it is tried again
    /// after a wait.
    pub(crate) fn done(&mut self, table: Uuid, loaded: bool, now: Instant) {
        let Some(State::Loading { again, failures }) = self.states.remove(&table) else {
            return;
        };
        if !self.marked.contains_key(&table) {
            return;
        }
        if loaded {
            if again {
                self.due.insert(table, 0);
            }
            return;
        }
        let failures = failures + 1;
        let wait = RETRY.saturating_mul(1 << (failures - 1).min(16)).min(RETRY_MAX);
        let until = now + wait;
        self.states.insert(table, State::Waiting { until, failures });
    }

    /// The tables to start loading at `now`: those due, and those whose wait is over, while fewer than [`AT_ONCE`] load.
    pub(crate) fn start(&mut self, now: Instant) -> Vec<Uuid> {
        let over: Vec<(Uuid, u32)> = self
            .states
            .iter()
            .filter_map(|(table, state)| match state {
                State::Waiting { until, failures } if *until <= now => Some((*table, *failures)),
                _ => None,
            })
            .collect();
        for (table, failures) in over {
            self.states.remove(&table);
            self.due.insert(table, failures);
        }
        let loading = self.loading();
        let room = AT_ONCE.saturating_sub(loading);
        let started: Vec<(Uuid, u32)> = self.due.iter().take(room).map(|(t, f)| (*t, *f)).collect();
        for (table, failures) in &started {
            self.due.remove(table);
            self.states.insert(
                *table,
                State::Loading {
                    again: false,
                    failures: *failures,
                },
            );
        }
        started.into_iter().map(|(table, _)| table).collect()
    }

    /// How long to wait before looking at the feed again.
    pub(crate) fn pause(&self) -> Duration {
        self.pause
    }

    /// When the first wait after a failure is over, if any, so that the worker wakes for it.
    pub(crate) fn wake_at(&self) -> Option<Instant> {
        self.states
            .values()
            .filter_map(|state| match state {
                State::Waiting { until, .. } => Some(*until),
                State::Loading { .. } => None,
            })
            .min()
    }

    /// Whether any table is marked: a worker with none only looks for one.
    pub(crate) fn any_marked(&self) -> bool {
        !self.marked.is_empty()
    }

    fn loading(&self) -> usize {
        self.states
            .values()
            .filter(|state| matches!(state, State::Loading { .. }))
            .count()
    }

    /// Makes `table` due, or, if it is loading, to be loaded again once done. One waiting after a failure keeps its
    /// wait: its next load takes what came.
    fn make_due(&mut self, table: Uuid) {
        match self.states.get_mut(&table) {
            Some(State::Loading { again, .. }) => *again = true,
            Some(State::Waiting { .. }) => {}
            None => {
                self.due.entry(table).or_insert(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAIN: i32 = 1;
    const DEV: i32 = 2;

    fn table(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// A plan with `tables` marked on main, every one started and loading.
    fn loading(tables: &[Uuid], now: Instant) -> Plan {
        let mut plan = Plan::default();
        plan.marked(tables.iter().map(|t| (MAIN, *t)));
        let started = plan.start(now);
        assert_eq!(started.len(), tables.len().min(AT_ONCE));
        plan
    }

    #[test]
    fn rest6_commits_while_loading_load_once_more() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = loading(&[t], now);
        for _ in 0..50 {
            plan.committed(&[(MAIN, t)]);
            assert!(plan.start(now).is_empty(), "never two loads of a table");
        }
        plan.done(t, true, now);
        assert_eq!(plan.start(now), [t], "once more, for all fifty");
        plan.done(t, true, now);
        assert!(plan.start(now).is_empty(), "and then nothing");
    }

    #[test]
    fn rest6_marking_makes_a_table_due_once() {
        let now = Instant::now();
        let (t, u) = (table(1), table(2));
        let mut plan = Plan::default();
        plan.marked([(MAIN, t)]);
        assert_eq!(plan.start(now), [t], "what it misses is generated");
        plan.done(t, true, now);
        plan.marked([(MAIN, t), (MAIN, u)]);
        assert_eq!(plan.start(now), [u], "only the table newly marked");
    }

    #[test]
    fn rest6_a_commit_on_a_ref_not_marked_is_not_due() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = Plan::default();
        plan.marked([(MAIN, t)]);
        plan.start(now);
        plan.done(t, true, now);
        plan.committed(&[(DEV, t), (MAIN, table(9))]);
        assert!(plan.start(now).is_empty());
        plan.committed(&[(MAIN, t)]);
        assert_eq!(plan.start(now), [t]);
    }

    #[test]
    fn rest6_at_most_at_once() {
        let now = Instant::now();
        let tables: Vec<Uuid> = (1..=10).map(table).collect();
        let mut plan = Plan::default();
        plan.marked(tables.iter().map(|t| (MAIN, *t)));
        let first = plan.start(now);
        assert_eq!(first.len(), AT_ONCE);
        assert!(plan.start(now).is_empty(), "no room");
        plan.done(first[0], true, now);
        assert_eq!(plan.start(now).len(), 1, "room for one");
        let mut loaded = first.len() + 1;
        while loaded < tables.len() {
            let room: Vec<Uuid> = plan.states.keys().copied().collect();
            for t in room {
                plan.done(t, true, now);
            }
            loaded += plan.start(now).len();
        }
        assert_eq!(loaded, tables.len(), "each loaded once");
    }

    #[test]
    fn rest6_a_failure_is_tried_again_after_a_wait_that_doubles() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = loading(&[t], now);
        let mut at = now;
        for wait in [1, 2, 4, 8, 16, 32, 60, 60] {
            plan.done(t, false, at);
            let wait = Duration::from_secs(wait);
            assert_eq!(plan.wake_at(), Some(at + wait));
            assert!(
                plan.start(at + wait - Duration::from_millis(1)).is_empty(),
                "not before its wait"
            );
            at += wait;
            assert_eq!(plan.start(at), [t]);
        }
        plan.done(t, true, at);
        plan.committed(&[(MAIN, t)]);
        plan.start(at);
        plan.done(t, false, at);
        assert_eq!(
            plan.wake_at(),
            Some(at + RETRY),
            "a load that succeeded starts the waits again"
        );
    }

    #[test]
    fn rest6_a_table_unmarked_is_left() {
        let (now, t, u) = (Instant::now(), table(1), table(2));
        let mut plan = loading(&[t, u], now);
        plan.done(u, false, now);
        plan.committed(&[(MAIN, t)]);
        plan.marked([]);
        plan.done(t, true, now);
        assert!(
            plan.start(now + RETRY_MAX).is_empty(),
            "neither loaded again nor tried again"
        );
        assert_eq!(plan.wake_at(), None);
        plan.committed(&[(MAIN, t)]);
        assert!(plan.start(now).is_empty());
    }

    #[test]
    fn rest6_marked_again_while_loading_loads_again() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = loading(&[t], now);
        plan.marked([]);
        plan.marked([(MAIN, t)]);
        plan.done(t, true, now);
        assert_eq!(plan.start(now), [t]);
    }

    #[test]
    fn rest6_the_feed_is_looked_at_as_often_as_commits_come() {
        let t = table(1);
        let mut plan = Plan::default();
        assert_eq!(plan.pause(), UNMARKED, "nothing marked");
        plan.committed(&[(MAIN, t)]);
        assert_eq!(plan.pause(), UNMARKED, "commits of tables not marked");
        plan.marked([(MAIN, t)]);
        assert_eq!(plan.pause(), ACTIVE);
        for wait in [2, 4, 8, 16, 30, 30] {
            plan.committed(&[]);
            assert_eq!(plan.pause(), Duration::from_secs(wait), "reads that find nothing");
        }
        plan.committed(&[(MAIN, table(9)), (DEV, t)]);
        assert_eq!(plan.pause(), QUIET, "commits of tables not marked there");
        plan.committed(&[(MAIN, t)]);
        assert_eq!(plan.pause(), ACTIVE, "a commit of a table marked");
        plan.committed(&[]);
        plan.woken();
        assert_eq!(plan.pause(), ACTIVE, "marked through the service");
        plan.marked([]);
        assert_eq!(plan.pause(), UNMARKED);
        assert!(!plan.any_marked());
    }
}
