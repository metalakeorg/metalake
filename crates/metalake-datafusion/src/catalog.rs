//! Refs as catalogs, namespaces as schemas (`main.db.events`). DataFusion finds a table asynchronously, so a table is
//! always looked up in the protocol; it lists names synchronously, which Postgres cannot answer, so listings come from
//! a picture taken when [`MetaLake`] is built and on [`MetaLake::refresh`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement;
use metalake::path::{DEFAULT_NAMESPACE, MAIN, NAMESPACE_SEPARATOR};
use metalake::{Catalog, Error, Ref, Result, Storage, TablePath};

use crate::details::CommitOptions;
use crate::point::{self, OnePoint};
use crate::table::MetaTable;
use crate::{changes, ddl};

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

    /// The codec of a physical plan that holds this lake's scans, built with this lake's storage. An executor without a
    /// catalog makes one with `MetaLakePhysicalCodec::new`.
    #[cfg(feature = "proto")]
    pub fn physical_codec(
        &self,
        inner: Arc<dyn datafusion_proto::physical_plan::PhysicalExtensionCodec>,
    ) -> crate::codec::MetaLakePhysicalCodec {
        crate::codec::MetaLakePhysicalCodec::new(self.shared.storage.clone(), inner)
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
    pub async fn refresh(&self) -> Result<()> {
        let mut listing = Listing::new();
        for on in self.shared.catalog.refs().await? {
            let namespaces = listing.entry(on.name().to_string()).or_default();
            for table in on.tables().await? {
                namespaces
                    .entry(table.namespace.join("."))
                    .or_default()
                    .push(table.name);
            }
        }
        *self.shared.listing.write().expect("a listing is replaced whole") = listing;
        Ok(())
    }

    /// A session over this catalog, with `information_schema`, and reading unqualified tables from `main.default`.
    pub fn context(&self) -> Result<SessionContext, DataFusionError> {
        // The catalogs are the refs: DataFusion makes none of its own, here or in a session rebuilt from this one.
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(MAIN, DEFAULT_NAMESPACE)
            .with_create_default_catalog_and_schema(false);
        // `varchar` is a table's string, which has no view.
        config.options_mut().sql_parser.map_string_types_to_utf8view = false;
        let config = CommitOptions::register(config);
        let context = SessionContext::new_with_config(config);
        context.register_catalog_list(Arc::new(self.clone()));
        context.add_analyzer_rule(Arc::new(OnePoint));
        let url = ObjectStoreUrl::parse(self.shared.storage.base())?;
        context.register_object_store(url.as_ref(), self.shared.storage.store().clone());
        Ok(context)
    }

    /// Runs one statement in a session of this catalog, as `context.sql` would; what creates a namespace or a table as
    /// a commit, which `context.sql` cannot do; and what reads `table_changes(…)`, which it does not know.
    pub async fn execute(&self, context: &SessionContext, mut statement: Statement) -> DataFusionResult<DataFrame> {
        // The ranges it reads live as sources until its plan holds them.
        let _ranges = self.table_changes(&mut statement).await?;
        let plan = context.state().statement_to_plan(statement).await?;
        match plan {
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(create)) => {
                let (on, namespace) = match create.schema_name.split_once(NAMESPACE_SEPARATOR) {
                    Some((on, namespace)) => (on, namespace),
                    None => (MAIN, create.schema_name.as_str()),
                };
                let on = self.shared.named(on).await?;
                ddl::create_schema(&on, &[namespace], create.if_not_exists).await?;
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let name = create.name.clone().resolve(MAIN, DEFAULT_NAMESPACE);
                let path = TablePath::from_parts(Some(&name.catalog), Some(&name.schema), &name.table);
                let on = self.shared.named(&path.on).await?;
                let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
                ddl::create_table(&on, &namespace, &path.name, &create).await?;
            }
            plan => return context.execute_logical_plan(plan).await,
        }
        self.refresh().await.map_err(external)?;
        let done = LogicalPlanBuilder::empty(false).build()?;
        context.execute_logical_plan(done).await
    }

    /// Each `table_changes(…)` the statement reads, found in the catalog and put in its place as a source. A table
    /// function of DataFusion would have to know its columns without asking Postgres, which only answers when awaited.
    async fn table_changes(&self, statement: &mut Statement) -> DataFusionResult<Vec<Source>> {
        let Statement::Statement(inner) = statement else {
            return Ok(Vec::new());
        };
        let calls = changes::calls(inner)?;
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
            changes::replace(inner, &names)?;
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

impl CatalogProviderList for MetaLake {
    fn register_catalog(&self, name: String, _: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>> {
        panic!("{name}: the catalogs of a MetaLake session are its refs")
    }

    fn catalog_names(&self) -> Vec<String> {
        self.shared.listed(|l| l.keys().cloned().collect())
    }

    /// Any name: whether it is a ref is a question for the protocol, which a table's lookup asks.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(RefCatalog {
            shared: self.shared.clone(),
            name: name.to_string(),
        }))
    }
}

struct RefCatalog {
    shared: Arc<Shared>,
    name: String,
}

impl fmt::Debug for RefCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefCatalog").field("name", &self.name).finish()
    }
}

impl CatalogProvider for RefCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.shared
            .listed(|l| l.get(&self.name).map(|n| n.keys().cloned().collect()))
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(Namespace {
            shared: self.shared.clone(),
            on: self.name.clone(),
            name: name.to_string(),
        }))
    }
}

struct Namespace {
    shared: Arc<Shared>,
    on: String,
    name: String,
}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({}.{})", self.on, self.name)
    }
}

#[async_trait]
impl SchemaProvider for Namespace {
    fn table_names(&self) -> Vec<String> {
        self.shared
            .listed(|l| l.get(&self.on).and_then(|n| n.get(&self.name)).cloned())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let shared = &self.shared;
        if self.on == SOURCES {
            let sources = shared.sources.lock().expect("a source is only added or taken");
            return Ok(name.parse().ok().and_then(|name: u64| sources.get(&name).cloned()));
        }
        let Some(on) = shared.find(&self.on).await.map_err(external)? else {
            return Ok(None);
        };
        let path = TablePath::from_parts(Some(&self.on), Some(&self.name), name);
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
