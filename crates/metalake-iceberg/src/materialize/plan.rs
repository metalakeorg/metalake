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
//! - With no table marked the feed is looked at seldom; with some, every second while commits come and for [`WARM`]
//!   after the last, as more tend to follow, and then less often as reads find none.
//! - A table's lag is how long it waited, from the first commit a load had not taken to the load that took it: what a
//!   table marked costs a reader that comes meanwhile, and what the worker reports.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use uuid::Uuid;

/// Tables loaded at once at most.
pub(crate) const AT_ONCE: usize = 4;
/// How often the feed is looked at while commits come: it delivers a commit one second after it (CHG-1).
pub(crate) const ACTIVE: Duration = Duration::from_secs(1);
/// How long after a commit of a table marked the feed is still looked at every second.
pub(crate) const WARM: Duration = Duration::from_secs(10);
/// How seldom it is looked at, at most, while reads find nothing.
pub(crate) const QUIET: Duration = Duration::from_secs(30);
/// How often a worker with no table marked looks for one.
pub(crate) const UNMARKED: Duration = Duration::from_secs(60);
/// The first wait after a load that failed, which doubles with each failure, up to [`RETRY_MAX`].
pub(crate) const RETRY: Duration = Duration::from_secs(1);
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(60);

/// Since when a table waits, and after how many loads that failed in a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Due {
    /// When the first commit no load has taken came, or the table was marked.
    since: Instant,
    failures: u32,
}

/// What a table that is not idle is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Loading what was due; `again`, since when, once a commit of it came meanwhile.
    Loading { due: Due, again: Option<Instant> },
    /// Waiting to be tried again after a load that failed.
    Waiting { until: Instant, due: Due },
}

/// The worker's decisions.
#[derive(Debug)]
pub(crate) struct Plan {
    /// Each table marked, and the refs it is marked on.
    marked: BTreeMap<Uuid, BTreeSet<i32>>,
    /// The tables that are not idle.
    states: BTreeMap<Uuid, State>,
    /// The tables to load as soon as there is room.
    due: BTreeMap<Uuid, Due>,
    /// How long to wait before looking at the feed again.
    pause: Duration,
    /// When a commit of a table marked last came, or a table was marked.
    active: Option<Instant>,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            marked: BTreeMap::new(),
            states: BTreeMap::new(),
            due: BTreeMap::new(),
            pause: UNMARKED,
            active: None,
        }
    }
}

impl Plan {
    /// The refs and tables marked at `now`, as the protocol lists them (`tables_with_property`). A table newly marked is
    /// due, so that what it misses is generated; one no longer marked is left, once a load of it is done.
    pub(crate) fn marked(&mut self, marked: impl IntoIterator<Item = (i32, Uuid)>, now: Instant) {
        let mut by_table: BTreeMap<Uuid, BTreeSet<i32>> = BTreeMap::new();
        for (ref_id, table) in marked {
            by_table.entry(table).or_default().insert(ref_id);
        }
        let entered: Vec<Uuid> = by_table
            .keys()
            .filter(|t| !self.marked.contains_key(t))
            .copied()
            .collect();
        self.marked = by_table;
        for table in entered {
            self.make_due(table, now);
        }
        let marked = &self.marked;
        self.due.retain(|table, _| marked.contains_key(table));
        self.states
            .retain(|table, state| marked.contains_key(table) || matches!(state, State::Loading { .. }));
        if self.marked.is_empty() {
            self.pause = UNMARKED;
        } else if self.pause == UNMARKED {
            self.pause = ACTIVE;
            self.active = Some(now);
        }
    }

    /// What a read of the feed found at `now`: each commit's ref and a table it made a snapshot of (`commit_tables`). A
    /// table marked on that ref is due. Commits of a table marked keep the feed looked at every second, and reads with
    /// none make it seldom: commits of tables not marked are no reason to look sooner.
    pub(crate) fn committed(&mut self, touched: &[(i32, Uuid)], now: Instant) {
        let mut any = false;
        for (ref_id, table) in touched {
            if self.marked.get(table).is_some_and(|refs| refs.contains(ref_id)) {
                self.make_due(*table, now);
                any = true;
            }
        }
        if any {
            self.active = Some(now);
        }
        let warm = self.active.is_some_and(|at| now.saturating_duration_since(at) < WARM);
        self.pause = match (self.marked.is_empty(), warm) {
            (true, _) => UNMARKED,
            (false, true) => ACTIVE,
            (false, false) => (self.pause * 2).min(QUIET),
        };
    }

    /// A table was marked through the service itself: the feed is looked at again at once.
    pub(crate) fn woken(&mut self) {
        self.pause = ACTIVE;
    }

    /// A load of `table` ended, at `now`. Done, it answers the table's lag, and the table is loaded again if a commit
    /// came meanwhile; failed, it is tried again after a wait, and still waits since when it did.
    pub(crate) fn done(&mut self, table: Uuid, loaded: bool, now: Instant) -> Option<Duration> {
        let Some(State::Loading { due, again }) = self.states.remove(&table) else {
            return None;
        };
        let lag = now.saturating_duration_since(due.since);
        if !self.marked.contains_key(&table) {
            return loaded.then_some(lag);
        }
        if loaded {
            if let Some(since) = again {
                self.due.insert(table, Due { since, failures: 0 });
            }
            return Some(lag);
        }
        let failures = due.failures + 1;
        let wait = RETRY.saturating_mul(1 << (failures - 1).min(16)).min(RETRY_MAX);
        let due = Due {
            since: due.since,
            failures,
        };
        self.states.insert(table, State::Waiting { until: now + wait, due });
        None
    }

    /// The tables to start loading at `now`: those due, and those whose wait is over, while fewer than [`AT_ONCE`] load.
    pub(crate) fn start(&mut self, now: Instant) -> Vec<Uuid> {
        let over: Vec<(Uuid, Due)> = self
            .states
            .iter()
            .filter_map(|(table, state)| match state {
                State::Waiting { until, due } if *until <= now => Some((*table, *due)),
                _ => None,
            })
            .collect();
        for (table, due) in over {
            self.states.remove(&table);
            self.due.insert(table, due);
        }
        let room = AT_ONCE.saturating_sub(self.loading());
        let started: Vec<(Uuid, Due)> = self.due.iter().take(room).map(|(t, d)| (*t, *d)).collect();
        for (table, due) in &started {
            self.due.remove(table);
            self.states.insert(*table, State::Loading { due: *due, again: None });
        }
        started.into_iter().map(|(table, _)| table).collect()
    }

    /// How long `table` has waited at `now`, if it waits: since the first commit no load has taken.
    pub(crate) fn lag(&self, table: Uuid, now: Instant) -> Option<Duration> {
        let since = match (self.due.get(&table), self.states.get(&table)) {
            (Some(due), _) => due.since,
            (None, Some(State::Loading { due, .. } | State::Waiting { due, .. })) => due.since,
            (None, None) => return None,
        };
        Some(now.saturating_duration_since(since))
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

    /// Makes `table` due at `now`, or, if it is loading, to be loaded again once done. One already due, or waiting
    /// after a failure, keeps since when it waits: its next load takes what came.
    fn make_due(&mut self, table: Uuid, now: Instant) {
        match self.states.get_mut(&table) {
            Some(State::Loading { again, .. }) => {
                again.get_or_insert(now);
            }
            Some(State::Waiting { .. }) => {}
            None => {
                self.due.entry(table).or_insert(Due {
                    since: now,
                    failures: 0,
                });
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
        plan.marked(tables.iter().map(|t| (MAIN, *t)), now);
        let started = plan.start(now);
        assert_eq!(started.len(), tables.len().min(AT_ONCE));
        plan
    }

    #[test]
    fn rest6_commits_during_load_coalesce() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = loading(&[t], now);
        for _ in 0..50 {
            plan.committed(&[(MAIN, t)], now);
            assert!(plan.start(now).is_empty(), "never two loads of a table");
        }
        plan.done(t, true, now);
        assert_eq!(plan.start(now), [t], "once more, for all fifty");
        plan.done(t, true, now);
        assert!(plan.start(now).is_empty(), "and then nothing");
    }

    #[test]
    fn rest6_marked_table_due_once() {
        let now = Instant::now();
        let (t, u) = (table(1), table(2));
        let mut plan = Plan::default();
        plan.marked([(MAIN, t)], now);
        assert_eq!(plan.start(now), [t], "what it misses is generated");
        plan.done(t, true, now);
        plan.marked([(MAIN, t), (MAIN, u)], now);
        assert_eq!(plan.start(now), [u], "only the table newly marked");
    }

    #[test]
    fn rest6_commit_on_unmarked_ref_skips() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = Plan::default();
        plan.marked([(MAIN, t)], now);
        plan.start(now);
        plan.done(t, true, now);
        plan.committed(&[(DEV, t), (MAIN, table(9))], now);
        assert!(plan.start(now).is_empty());
        plan.committed(&[(MAIN, t)], now);
        assert_eq!(plan.start(now), [t]);
    }

    #[test]
    fn rest6_concurrent_loads_bounded() {
        let now = Instant::now();
        let tables: Vec<Uuid> = (1..=10).map(table).collect();
        let mut plan = Plan::default();
        plan.marked(tables.iter().map(|t| (MAIN, *t)), now);
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
    fn rest6_failed_load_retries_with_backoff() {
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
        plan.committed(&[(MAIN, t)], now);
        plan.start(at);
        plan.done(t, false, at);
        assert_eq!(
            plan.wake_at(),
            Some(at + RETRY),
            "a load that succeeded starts the waits again"
        );
    }

    #[test]
    fn rest6_unmarked_table_skips() {
        let (now, t, u) = (Instant::now(), table(1), table(2));
        let mut plan = loading(&[t, u], now);
        plan.done(u, false, now);
        plan.committed(&[(MAIN, t)], now);
        plan.marked([], now);
        plan.done(t, true, now);
        assert!(
            plan.start(now + RETRY_MAX).is_empty(),
            "neither loaded again nor tried again"
        );
        assert_eq!(plan.wake_at(), None);
        plan.committed(&[(MAIN, t)], now);
        assert!(plan.start(now).is_empty());
    }

    #[test]
    fn rest6_marked_again_during_load_reloads() {
        let (now, t) = (Instant::now(), table(1));
        let mut plan = loading(&[t], now);
        plan.marked([], now);
        plan.marked([(MAIN, t)], now);
        plan.done(t, true, now);
        assert_eq!(plan.start(now), [t]);
    }

    #[test]
    fn rest6_feed_poll_interval_adapts() {
        let (t0, t) = (Instant::now(), table(1));
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let mut plan = Plan::default();
        assert_eq!(plan.pause(), UNMARKED, "nothing marked");
        plan.committed(&[(MAIN, t)], at(0));
        assert_eq!(plan.pause(), UNMARKED, "commits of tables not marked");
        plan.marked([(MAIN, t)], at(0));
        assert_eq!(plan.pause(), ACTIVE);
        plan.committed(&[], at(9));
        assert_eq!(
            plan.pause(),
            ACTIVE,
            "reads that find nothing, within WARM of the marking"
        );
        for wait in [2, 4, 8, 16, 30, 30] {
            plan.committed(&[], at(10));
            assert_eq!(
                plan.pause(),
                Duration::from_secs(wait),
                "reads that find nothing, after it"
            );
        }
        plan.committed(&[(MAIN, table(9)), (DEV, t)], at(20));
        assert_eq!(plan.pause(), QUIET, "commits of tables not marked there");
        plan.committed(&[(MAIN, t)], at(30));
        assert_eq!(plan.pause(), ACTIVE, "a commit of a table marked");
        plan.committed(&[], at(35));
        assert_eq!(plan.pause(), ACTIVE, "warm after it");
        plan.committed(&[], at(40));
        assert_eq!(plan.pause(), Duration::from_secs(2), "and then not");
        plan.woken();
        assert_eq!(plan.pause(), ACTIVE, "marked through the service");
        plan.marked([], at(41));
        assert_eq!(plan.pause(), UNMARKED);
        assert!(!plan.any_marked());
    }

    #[test]
    fn rest6_lag_from_first_commit_not_taken() {
        let (t0, t) = (Instant::now(), table(1));
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let mut plan = Plan::default();
        plan.marked([(MAIN, t)], at(0));
        plan.done(t, true, at(0));
        assert_eq!(plan.lag(t, at(0)), Some(Duration::ZERO), "marked: due since then");

        plan.start(at(1));
        plan.committed(&[(MAIN, t)], at(2));
        plan.committed(&[(MAIN, t)], at(3));
        assert_eq!(
            plan.done(t, true, at(4)),
            Some(Duration::from_secs(4)),
            "from the marking"
        );
        assert_eq!(
            plan.lag(t, at(4)),
            Some(Duration::from_secs(2)),
            "again, since the first commit after the load began"
        );

        plan.start(at(5));
        assert_eq!(plan.done(t, false, at(6)), None, "a failure has none");
        assert_eq!(
            plan.lag(t, at(6)),
            Some(Duration::from_secs(4)),
            "and keeps waiting since then"
        );
        plan.start(at(60));
        assert_eq!(plan.done(t, true, at(61)), Some(Duration::from_secs(59)));
        assert_eq!(plan.lag(t, at(61)), None, "up to date");
    }
}
