//! Where a statement runs: in the client's own process, or on the cluster. The router is the session's query planner,
//! the step that turns a logical plan into one that runs, and it holds both of the lake's: the local one, whose plan
//! runs where it is, and Ballista's, whose plan sends the logical plan to the scheduler. What it returns is the
//! decision carried out: neither DataFusion nor Ballista learns there was one.
//!
//! `metalake.engine` says which; unset, the cluster, as a session of one has always run.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use metalake_datafusion::{Engine, MetaLakeOptions};

/// A session's query planner over the local one and the cluster's.
#[derive(Debug)]
pub struct Router {
    local: Arc<dyn QueryPlanner + Send + Sync>,
    cluster: Arc<dyn QueryPlanner + Send + Sync>,
}

impl Router {
    pub fn new(local: Arc<dyn QueryPlanner + Send + Sync>, cluster: Arc<dyn QueryPlanner + Send + Sync>) -> Self {
        Self { local, cluster }
    }

    /// Where `session`'s statements run.
    fn engine(session: &dyn Session) -> Engine {
        let options = session.config_options().extensions.get::<MetaLakeOptions>();
        options.and_then(|o| o.engine).unwrap_or(Engine::Cluster)
    }
}

#[async_trait]
impl QueryPlanner for Router {
    async fn create_physical_plan(&self, plan: &LogicalPlan, session: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = match Self::engine(session) {
            Engine::Local => &self.local,
            Engine::Cluster => &self.cluster,
        };
        planner.create_physical_plan(plan, session).await
    }
}
