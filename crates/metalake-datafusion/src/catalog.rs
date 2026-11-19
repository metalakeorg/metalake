//! A lake's catalogs as DataFusion's, on the ref a session is on (I7): a namespace's first level is the catalog, the
//! levels after it the schema (`ventas.eu.orders`), and the ref is the session's, which no name says. DataFusion finds a
//! table asynchronously, so a table is always looked up in the protocol; it lists names synchronously, which Postgres
//! cannot answer, so listings come from a picture taken when [`MetaLake`] is built and on [`MetaLake::refresh`].

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use metalake::path::{DEFAULT_CATALOG, DEFAULT_NAMESPACE, MAIN, NAMESPACE_SEPARATOR, levels};
use metalake::{Catalog, Error, Ref, Result, Storage, TablePath};

use crate::options::MetaLakeOptions;
use crate::point::{self, OnePoint};
use crate::table::MetaTable;
use crate::written::Written;
use crate::{changes, ddl, orc};

/// The catalog of what a statement brings with it, which cannot be a ref's name, since those have no leading dot.
const SOURCES: &str = ".source";
/// The name of the next source.
static NEXT_SOURCE: AtomicU64 = AtomicU64::new(0);

/// Each ref's namespaces, each with its tables.
type Listing = BTreeMap<String, BTreeMap<String, Vec<String>>>;

/// A MetaLake catalog as DataFusion's list of catalogs, one for each ref.
#[derive(Clone)]
pub struct MetaLake {
    shared: Arc<Shared>,
}

struct Shared {
    catalog: Catalog,
    storage: Storage,
    listing: RwLock<Listing>,
    sources: Mutex<BTreeMap<u64, Arc<dyn TableProvider>>>,
}

impl fmt::Debug for MetaLake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaLake").finish_non_exhaustive()
    }
}

impl MetaLake {
    pub async fn new(catalog: Catalog, storage: Storage) -> Result<Self> {
        let lake = Self {
            shared: Arc::new(Shared {
                catalog,
                storage,
                listing: RwLock::default(),
                sources: Mutex::default(),
            }),
        };
        lake.refresh().await?;
        Ok(lake)
    }

    /// The codec of a logical plan that holds this lake's tables, for a plan that crosses processes: each table travels
    /// as a value and is rebuilt with this lake's catalog and storage. `inner` encodes everything else.
    #[cfg(feature = "proto")]
    pub fn logical_codec(
        &self,
        inner: Arc<dyn datafusion_proto::logical_plan::LogicalExtensionCodec>,
    ) -> crate::codec::MetaLakeLogicalCodec {
        crate::codec::MetaLakeLogicalCodec::new(self.shared.catalog.clone(), self.shared.storage.clone(), inner)
    }

    /// The codec of a physical plan that holds this lake's scans and writes, built with this lake's storage and catalog.
    /// An executor that only reads makes one with `MetaLakePhysicalCodec::new`, without a catalog.
    #[cfg(feature = "proto")]
    pub fn physical_codec(
        &self,
        inner: Arc<dyn datafusion_proto::physical_plan::PhysicalExtensionCodec>,
    ) -> crate::codec::MetaLakePhysicalCodec {
        crate::codec::MetaLakePhysicalCodec::new(self.shared.storage.clone(), inner)
            .with_catalog(self.shared.catalog.clone())
    }

    /// Lets statements read `provider` as long as what this answers lives, by the name it gives.
    pub fn source(&self, provider: Arc<dyn TableProvider>) -> Source {
        let name = NEXT_SOURCE.fetch_add(1, Ordering::Relaxed);
        let sources = &self.shared.sources;
        sources
            .lock()
            .expect("a source is only added or taken")
            .insert(name, provider);
        Source {
            shared: self.shared.clone(),
            name,
        }
    }

    /// Takes a new picture of what listings show. A table is found without it.
    ///
    /// Three calls whatever the refs: the view of namespaces computes every ref's before it filters one, so that a call
    /// per ref cost as many times the whole, and a picture is taken after each statement that changes the catalog.
    pub async fn refresh(&self) -> Result<()> {
        let catalog = &self.shared.catalog;
        let refs = catalog.refs().await?;
        // Each ref's namespaces, each with its tables: a catalog or a schema with no table yet too, as `CREATE CATALOG`
        // leaves one (I7). A ref made since `refs` read is in the next picture; one dropped since has no rows.
        let mut listing: Listing = refs.iter().map(|on| (on.name().to_string(), BTreeMap::new())).collect();
        for (on, row) in catalog.namespaces_of_refs().await? {
            if let Some(namespaces) = listing.get_mut(&on) {
                namespaces.entry(row.namespace.join(".")).or_default();
            }
        }
        let ids: Vec<i32> = refs.iter().map(Ref::id).collect();
        for (id, table) in catalog.tables_of_refs(&ids).await? {
            let Some(on) = refs.iter().find(|on| on.id() == id) else {
                continue;
            };
            if let Some(namespaces) = listing.get_mut(on.name()) {
                namespaces
                    .entry(table.namespace.join("."))
                    .or_default()
                    .push(table.name);
            }
        }
        *self.shared.listing.write().expect("a listing is replaced whole") = listing;
        Ok(())
    }

    /// Takes a new picture after a statement that changed the catalog. It committed already: a picture that cannot be
    /// taken now does not fail it, and the next is taken anew, as a table is found without one.
    async fn refreshed(&self) {
        let _ = self.refresh().await;
    }

    /// A session over this catalog, with `information_schema`, on `main` until [`MetaLake::on_ref`] says another, and
    /// reading unqualified tables from `default.default`.
    pub fn context(&self) -> Result<SessionContext, DataFusionError> {
        // The catalogs are the lake's, on the session's ref: DataFusion makes none of its own, here or in a session
        // rebuilt from this one.
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_NAMESPACE)
            .with_create_default_catalog_and_schema(false);
        // `varchar` is a table's string, which has no view.
        config.options_mut().sql_parser.map_string_types_to_utf8view = false;
        let config = MetaLakeOptions::register(config);
        let context = SessionContext::new_with_config(config);
        context.register_catalog_list(Arc::new(SessionLake {
            shared: self.shared.clone(),
            on: Arc::new(RwLock::new(MAIN.to_string())),
        }));
        context.add_analyzer_rule(Arc::new(OnePoint));
        // A write on a table answers its row, what came of its commit with its count.
        context.add_analyzer_rule(Arc::new(Written));
        let url = ObjectStoreUrl::parse(self.shared.storage.base())?;
        context.register_object_store(url.as_ref(), self.shared.storage.store().clone());
        Ok(context)
    }

    /// Has the session of `context` read and write on the ref `name` (or `name@point`), from its next statement: every
    /// table it names is on it (I7).
    pub fn on_ref(context: &SessionContext, name: &str) {
        *session_lake(context).on.write().expect("a ref is replaced whole") = name.to_string();
    }

    /// The ref the session of `context` reads and writes on: `main` until [`MetaLake::on_ref`] says another.
    pub fn current_ref(context: &SessionContext) -> String {
        session_lake(context)
            .on
            .read()
            .expect("a ref is replaced whole")
            .clone()
    }

    /// Runs one statement in a session of this catalog, as `context.sql` would; what creates a namespace or a table as
    /// a commit, which `context.sql` cannot do; and what reads `table_changes(…)`, which it does not know.
    pub async fn execute(&self, context: &SessionContext, mut statement: Statement) -> DataFusionResult<DataFrame> {
        // Every name is on the session's ref, which none says.
        let on = Self::current_ref(context);
        // The ranges it reads live as sources until its plan holds them.
        if let Statement::Statement(inner) = &statement
            && let SqlStatement::AlterTable(alter) = inner.as_ref()
        {
            let defaults = context.state().config().options().catalog.clone();
            let normalize = context.state().config().options().sql_parser.enable_ident_normalization;
            let name = object_name_to_table_reference(alter.name.clone(), normalize)?
                .resolve(&defaults.default_catalog, &defaults.default_schema);
            let path = TablePath::from_parts(&on, Some(&name.catalog), Some(&name.schema), &name.table);
            let on = self.shared.named(&path.on).await?;
            ddl::alter_table(&on, &path, alter, context).await?;
            self.refreshed().await;
            let done = LogicalPlanBuilder::empty(false).build()?;
            return context.execute_logical_plan(done).await;
        }
        let state = context.state();
        let _ranges = self.table_changes(&mut statement, &on).await?;
        let _orc = self.read_orc(&mut statement, &state).await?;
        let plan = state.statement_to_plan(statement).await?;
        let defaults = &state.config().options().catalog;
        match plan {
            // `create schema demo` is `default.demo`; `create schema ventas.eu` is in the catalog `ventas`, made with it
            // if it is not there.
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(create)) => {
                let mut namespace = levels(&create.schema_name);
                if namespace.len() == 1 {
                    namespace.insert(0, DEFAULT_CATALOG.to_string());
                }
                // An Iceberg warehouse puts `@` between a catalog and its ref (`ventas@dev`): no catalog holds one.
                if namespace[0].contains('@') {
                    return Err(external(Error::Invalid(format!(
                        "{}: a catalog's name holds no @, which an Iceberg warehouse puts between a catalog and its ref",
                        namespace[0]
                    ))));
                }
                let on = self.shared.named(&on).await?;
                ddl::create_schema(&on, &namespace, create.if_not_exists).await?;
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let name = create
                    .name
                    .clone()
                    .resolve(&defaults.default_catalog, &defaults.default_schema);
                let path = TablePath::from_parts(&on, Some(&name.catalog), Some(&name.schema), &name.table);
                let on = self.shared.named(&path.on).await?;
                let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
                let writing = ddl::Writing {
                    context,
                    catalog: &self.shared.catalog,
                    storage: &self.shared.storage,
                };
                ddl::create_table(&on, &namespace, &path.name, &create, writing).await?;
            }
            plan => return context.execute_logical_plan(plan).await,
        }
        self.refreshed().await;
        let done = LogicalPlanBuilder::empty(false).build()?;
        context.execute_logical_plan(done).await
    }

    /// Each `read_orc(…)` the statement reads, its directory listed and put in its place as a source.
    async fn read_orc(&self, statement: &mut Statement, state: &SessionState) -> DataFusionResult<Vec<Source>> {
        let Statement::Statement(inner) = statement else {
            return Ok(Vec::new());
        };
        let mut sources = Vec::new();
        for path in orc::calls(inner)? {
            sources.push(self.source(orc::provider(state, &self.shared.storage, &path).await?));
        }
        if !sources.is_empty() {
            let names: Vec<String> = sources.iter().map(Source::name).collect();
            changes::replace(inner, orc::FUNCTION, &names)?;
        }
        Ok(sources)
    }

    /// Each `table_changes(…)` the statement reads, found in the catalog and put in its place as a source. A table
    /// function of DataFusion would have to know its columns without asking Postgres, which only answers when awaited.
    async fn table_changes(&self, statement: &mut Statement, on: &str) -> DataFusionResult<Vec<Source>> {
        let Statement::Statement(inner) = statement else {
            return Ok(Vec::new());
        };
        let calls = changes::calls(inner, on)?;
        let mut sources = Vec::with_capacity(calls.len());
        for call in calls {
            let on = self.shared.named(&call.path.on).await?;
            let namespace: Vec<&str> = call.path.namespace.iter().map(String::as_str).collect();
            let table = on.table(&namespace, &call.path.name).await.map_err(external)?;
            let range = on.changes(&table, call.after, call.until).await.map_err(external)?;
            sources.push(self.source(changes::provider(range, self.shared.storage.clone())?));
        }
        if !sources.is_empty() {
            let names: Vec<String> = sources.iter().map(Source::name).collect();
            changes::replace(inner, changes::FUNCTION, &names)?;
        }
        Ok(sources)
    }
}

impl Shared {
    fn listed<T>(&self, read: impl FnOnce(&Listing) -> T) -> T {
        read(&self.listing.read().expect("a listing is replaced whole"))
    }

    /// The ref of a name a statement must find.
    async fn named(&self, name: &str) -> DataFusionResult<Ref> {
        let found = self.find(name).await.map_err(external)?;
        found.ok_or_else(|| external(Error::NotFound(format!("ref {name}"))))
    }

    /// The ref a catalog's name names, at the point it carries, if any: a ref's own name, `@` included, comes first.
    async fn find(&self, name: &str) -> Result<Option<Ref>> {
        let refs = self.catalog.refs().await?;
        let named = |name: &str| refs.iter().find(|r| r.name() == name).cloned();
        if let Some(on) = named(name) {
            return Ok(Some(on));
        }
        match name.split_once('@') {
            Some((on, at)) => named(on).map(|on| point::at(on, at)).transpose(),
            None => Ok(None),
        }
    }
}

/// A table that lives in memory for as long as this does, however what uses it ends.
pub struct Source {
    shared: Arc<Shared>,
    name: u64,
}

impl Source {
    /// How a statement names it.
    pub fn name(&self) -> String {
        format!("\"{SOURCES}\".\"{DEFAULT_NAMESPACE}\".\"{}\"", self.name)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let sources = &self.shared.sources;
        sources
            .lock()
            .expect("a source is only added or taken")
            .remove(&self.name);
    }
}

/// A session's catalogs: the lake's, on the ref the session is on, which [`MetaLake::on_ref`] changes.
struct SessionLake {
    shared: Arc<Shared>,
    on: Arc<RwLock<String>>,
}

impl fmt::Debug for SessionLake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionLake").finish_non_exhaustive()
    }
}

/// The catalogs of a session a [`MetaLake`] made.
fn session_lake(context: &SessionContext) -> Arc<SessionLake> {
    let list = context.state().catalog_list().clone();
    let any: Arc<dyn Any + Send + Sync> = list;
    any.downcast::<SessionLake>()
        .unwrap_or_else(|_| panic!("a MetaLake session's catalogs are the lake's"))
}

impl SessionLake {
    fn on(&self) -> String {
        self.on.read().expect("a ref is replaced whole").clone()
    }
}

impl CatalogProviderList for SessionLake {
    fn register_catalog(&self, name: String, _: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>> {
        panic!("{name}: the catalogs of a MetaLake session are the lake's")
    }

    /// The first levels of the namespaces the session's ref has tables in.
    fn catalog_names(&self) -> Vec<String> {
        let on = self.on();
        let names: std::collections::BTreeSet<String> = self.shared.listed(|l| {
            l.get(&on)
                .map(|n| n.keys().filter_map(|k| levels(k).into_iter().next()).collect())
                .unwrap_or_default()
        });
        names.into_iter().collect()
    }

    /// Any name, on the session's ref as it is now: whether it holds the table is a question for the protocol.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(LakeCatalog {
            shared: self.shared.clone(),
            on: self.on(),
            name: name.to_string(),
        }))
    }
}

/// A catalog of the lake on a ref: the namespaces whose first level is its name.
struct LakeCatalog {
    shared: Arc<Shared>,
    on: String,
    name: String,
}

impl fmt::Debug for LakeCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LakeCatalog({} on {})", self.name, self.on)
    }
}

impl CatalogProvider for LakeCatalog {
    /// The levels past this catalog of each namespace in it, joined by dots: a schema of several levels is one name.
    fn schema_names(&self) -> Vec<String> {
        let prefix = format!("{}{NAMESPACE_SEPARATOR}", self.name);
        self.shared
            .listed(|l| {
                l.get(&self.on).map(|n| {
                    n.keys()
                        .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(Namespace {
            shared: self.shared.clone(),
            on: self.on.clone(),
            catalog: self.name.clone(),
            name: name.to_string(),
        }))
    }
}

struct Namespace {
    shared: Arc<Shared>,
    on: String,
    catalog: String,
    name: String,
}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({}.{} on {})", self.catalog, self.name, self.on)
    }
}

#[async_trait]
impl SchemaProvider for Namespace {
    fn table_names(&self) -> Vec<String> {
        let namespace = format!("{}{NAMESPACE_SEPARATOR}{}", self.catalog, self.name);
        self.shared
            .listed(|l| l.get(&self.on).and_then(|n| n.get(&namespace)).cloned())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let shared = &self.shared;
        if self.catalog == SOURCES {
            let sources = shared.sources.lock().expect("a source is only added or taken");
            return Ok(name.parse().ok().and_then(|name: u64| sources.get(&name).cloned()));
        }
        let Some(on) = shared.find(&self.on).await.map_err(external)? else {
            return Ok(None);
        };
        let path = TablePath::from_parts(&self.on, Some(&self.catalog), Some(&self.name), name);
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        match on.table(&namespace, &path.name).await {
            Ok(table) => Ok(Some(Arc::new(MetaTable {
                on,
                catalog: shared.catalog.clone(),
                storage: shared.storage.clone(),
                table,
                statement: None,
            }))),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(external(e)),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|t| t == name)
    }
}

pub(crate) fn external(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
