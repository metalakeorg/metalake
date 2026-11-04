//! Refs as catalogs, namespaces as schemas (`main.db.events`). DataFusion finds a table asynchronously, so a table is
//! always looked up in the protocol; it lists names synchronously, which Postgres cannot answer, so listings come from
//! a picture taken when [`MetaLake`] is built and on [`MetaLake::refresh`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement;
use metalake::{Catalog, Error, Ref, Result, Storage};

use crate::ddl;
use crate::point::{self, OnePoint};
use crate::table::MetaTable;

/// The ref a session reads unqualified tables from.
const DEFAULT_CATALOG: &str = "main";
/// The namespace a session reads unqualified tables from.
const DEFAULT_SCHEMA: &str = "default";
/// What separates the levels of a nested namespace in a schema's name.
const SEPARATOR: char = '.';

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
            }),
        };
        lake.refresh().await?;
        Ok(lake)
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
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);
        // `varchar` is a table's string, which has no view.
        config.options_mut().sql_parser.map_string_types_to_utf8view = false;
        let context = SessionContext::new_with_config(config);
        context.register_catalog_list(Arc::new(self.clone()));
        context.add_analyzer_rule(Arc::new(OnePoint));
        let url = ObjectStoreUrl::parse(self.shared.storage.base())?;
        context.register_object_store(url.as_ref(), self.shared.storage.store().clone());
        Ok(context)
    }

    /// Runs one statement in a session of this catalog, as `context.sql` would, and what creates a namespace or a
    /// table as a commit, which `context.sql` cannot do.
    pub async fn execute(&self, context: &SessionContext, statement: Statement) -> DataFusionResult<DataFrame> {
        let plan = context.state().statement_to_plan(statement).await?;
        match plan {
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(create)) => {
                let (on, namespace) = match create.schema_name.split_once(SEPARATOR) {
                    Some((on, namespace)) => (on, namespace),
                    None => (DEFAULT_CATALOG, create.schema_name.as_str()),
                };
                let on = self.shared.named(on).await?;
                ddl::create_schema(&on, &[namespace], create.if_not_exists).await?;
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let name = create.name.clone().resolve(DEFAULT_CATALOG, DEFAULT_SCHEMA);
                let on = self.shared.named(&name.catalog).await?;
                let namespace: Vec<&str> = name.schema.split(SEPARATOR).collect();
                ddl::create_table(&on, &namespace, &name.table, &create).await?;
            }
            plan => return context.execute_logical_plan(plan).await,
        }
        let done = LogicalPlanBuilder::empty(false).build()?;
        context.execute_logical_plan(done).await
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
        let Some(on) = shared.find(&self.on).await.map_err(external)? else {
            return Ok(None);
        };
        let namespace: Vec<&str> = self.name.split(SEPARATOR).collect();
        match on.table(&namespace, name).await {
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
