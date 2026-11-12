//! Where a statement runs: in the client's own process, or on the cluster. The router is the session's query planner,
//! the step that turns a logical plan into one that runs, and it holds both of the lake's: the local one, whose plan
//! runs where it is, and Ballista's, whose plan sends the logical plan to the scheduler. What it returns is the
//! decision carried out: neither DataFusion nor Ballista learns there was one.
//!
//! `metalake.engine` says which: `local` or `cluster` forces it; `auto`, or unset, plans locally, measures what the
//! plan reads, and lets the rule choose. A statement the rule sends to a cluster that does not answer runs locally.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use metalake_datafusion::{Engine, MetaLakeOptions, Measure, measure};
use tokio::net::TcpStream;

/// Bytes read, after pruning, from which a statement that does not shuffle goes to the cluster: where the cluster's
/// fixed cost (sending the plan, splitting it in stages, handing out tasks) pays off. A starting point, not a
/// measurement; `metalake.engine.threshold` sets it.
pub const THRESHOLD: u64 = 1 << 30;

/// The same for a statement that shuffles: one process holds a hash join's or aggregation's state in its memory, a
/// cluster splits it, so the cluster pays off sooner. `metalake.engine.shuffle_threshold` sets it.
pub const SHUFFLE_THRESHOLD: u64 = 256 << 20;

/// How long an answer on whether the scheduler is there holds.
const REACHABLE_FOR: Duration = Duration::from_secs(30);

/// How long the scheduler has to answer.
const CONNECT_WITHIN: Duration = Duration::from_secs(1);

/// Where a statement runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Local,
    Cluster,
}

/// The rule (v1): a statement that reads no MetaLake table runs locally; one that does goes to the cluster from a
/// number of bytes after pruning, lower when it shuffles. Same measure and options, same route. Answers the threshold
/// it applied too.
pub fn route(measure: &Measure, options: &MetaLakeOptions) -> (Route, u64) {
    let threshold = match measure.shuffle {
        true => options.engine_shuffle_threshold.unwrap_or(SHUFFLE_THRESHOLD),
        false => options.engine_threshold.unwrap_or(THRESHOLD),
    };
    let route = match measure.scans > 0 && measure.bytes >= threshold {
        true => Route::Cluster,
        false => Route::Local,
    };
    (route, threshold)
}

/// A session's query planner over the local one and the cluster's.
#[derive(Debug)]
pub struct Router {
    local: Arc<dyn QueryPlanner + Send + Sync>,
    cluster: Arc<dyn QueryPlanner + Send + Sync>,
    scheduler: Scheduler,
}

impl Router {
    /// A router over `local` and `cluster`, whose scheduler is at `scheduler` (`df://host:port`).
    pub fn new(
        local: Arc<dyn QueryPlanner + Send + Sync>,
        cluster: Arc<dyn QueryPlanner + Send + Sync>,
        scheduler: &str,
    ) -> Self {
        Self {
            local,
            cluster,
            scheduler: Scheduler::new(scheduler),
        }
    }

    /// `session`'s MetaLake options.
    fn options(session: &dyn Session) -> MetaLakeOptions {
        let options = session.config_options().extensions.get::<MetaLakeOptions>();
        options.cloned().unwrap_or_default()
    }

    /// The rule's route for `plan`, the cluster's only if it answers.
    async fn decide(&self, plan: &Arc<dyn ExecutionPlan>, options: &MetaLakeOptions) -> Result<Route> {
        let measured = measure(plan).await?;
        let (mut route, threshold) = route(&measured, options);
        if route == Route::Cluster && !self.scheduler.reachable().await {
            log::warn!("metalake: the cluster at {} does not answer; running locally", self.scheduler);
            route = Route::Local;
        }
        log::info!(
            "metalake: {route:?}: {} bytes, {:?} rows, {} scans, shuffle {}, threshold {threshold}",
            measured.bytes,
            measured.rows,
            measured.scans,
            measured.shuffle
        );
        Ok(route)
    }
}

#[async_trait]
impl QueryPlanner for Router {
    async fn create_physical_plan(&self, plan: &LogicalPlan, session: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        let options = Self::options(session);
        match options.engine.unwrap_or(Engine::Auto) {
            Engine::Local => self.local.create_physical_plan(plan, session).await,
            // Forced: a cluster that does not answer is an error, not a local run.
            Engine::Cluster => self.cluster.create_physical_plan(plan, session).await,
            Engine::Auto => {
                let local = self.local.create_physical_plan(plan, session).await?;
                match self.decide(&local, &options).await? {
                    Route::Local => Ok(local),
                    Route::Cluster => self.cluster.create_physical_plan(plan, session).await,
                }
            }
        }
    }
}

/// The scheduler, as far as knowing whether it is there: its port takes a connection. Not whether it is well, or has
/// executors free, which is v2's.
struct Scheduler {
    address: String,
    /// The last answer, and when.
    last: Mutex<Option<(Instant, bool)>>,
}

impl Scheduler {
    fn new(url: &str) -> Self {
        // `df://host:port/…` → `host:port`, Ballista's port when it names none.
        let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
        let authority = rest.split('/').next().unwrap_or(rest);
        let address = match authority.rsplit_once(':') {
            Some((_, port)) if port.parse::<u16>().is_ok() => authority.to_string(),
            _ => format!("{authority}:50050"),
        };
        Self {
            address,
            last: Mutex::default(),
        }
    }

    /// Whether the scheduler takes a connection, asked at most once in `REACHABLE_FOR`.
    async fn reachable(&self) -> bool {
        if let Some((at, answer)) = *self.last.lock().expect("an answer is only replaced")
            && at.elapsed() < REACHABLE_FOR
        {
            return answer;
        }
        let answer = matches!(
            tokio::time::timeout(CONNECT_WITHIN, TcpStream::connect(&self.address)).await,
            Ok(Ok(_))
        );
        *self.last.lock().expect("an answer is only replaced") = Some((Instant::now(), answer));
        answer
    }
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler").field("address", &self.address).finish()
    }
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(bytes: u64, scans: usize, shuffle: bool) -> Measure {
        Measure {
            bytes,
            rows: None,
            scans,
            shuffle,
        }
    }

    #[test]
    fn reads_no_table_runs_locally() {
        let options = MetaLakeOptions {
            engine_threshold: Some(0),
            ..Default::default()
        };
        assert_eq!(route(&measured(0, 0, false), &options).0, Route::Local);
    }

    #[test]
    fn past_the_threshold_goes_to_the_cluster() {
        let options = MetaLakeOptions::default();
        assert_eq!(route(&measured(THRESHOLD - 1, 1, false), &options), (Route::Local, THRESHOLD));
        assert_eq!(route(&measured(THRESHOLD, 1, false), &options), (Route::Cluster, THRESHOLD));
    }

    #[test]
    fn a_shuffle_goes_sooner() {
        let options = MetaLakeOptions::default();
        let bytes = 300 << 20;
        assert_eq!(route(&measured(bytes, 2, false), &options).0, Route::Local);
        assert_eq!(route(&measured(bytes, 2, true), &options), (Route::Cluster, SHUFFLE_THRESHOLD));
    }

    #[test]
    fn options_set_the_thresholds() {
        let options = MetaLakeOptions {
            engine_threshold: Some(10),
            engine_shuffle_threshold: Some(1 << 40),
            ..Default::default()
        };
        assert_eq!(route(&measured(10, 1, false), &options).0, Route::Cluster);
        assert_eq!(route(&measured(1 << 30, 1, true), &options).0, Route::Local);
    }

    #[test]
    fn scheduler_address() {
        assert_eq!(Scheduler::new("df://localhost:50051").address, "localhost:50051");
        assert_eq!(Scheduler::new("df://scheduler").address, "scheduler:50050");
    }
}
