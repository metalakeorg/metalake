//! The MetaLake tables a pipeline's statements name, so that a user writes standard SQL over standard names and never a
//! connector's configuration: what an `INSERT` writes is a sink, and every other table it reads is a source. Names are
//! read by the core's rule ([`TablePath`]), as every other face of MetaLake reads them.

use std::collections::BTreeSet;

use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_rpc::api_types::connections::{ConnectionSchema, SourceField};
use arroyo_rpc::var_str::VarStr;
use datafusion::common::TableReference;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::ast::{Statement, TableObject};
use datafusion::sql::sqlparser::dialect::ArroyoDialect;
use datafusion::sql::sqlparser::parser::Parser;
use metalake::{Catalog, CatalogConfig, TablePath, environment};

use crate::MetaLakeConnector;
use crate::config::{MetaLakeProfile, MetaLakeSink, MetaLakeSource, MetaLakeTable, MetaLakeTableType, StartMode};

/// What a pipeline does with a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Source,
    Sink,
}

/// A table a pipeline's statements name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Named {
    /// The name as the planner looks it up.
    pub reference: TableReference,
    /// Where it is, by MetaLake's rule.
    pub path: TablePath,
    pub role: Role,
}

/// Identifiers are normalized as Arroyo's planner normalizes them, so that a name found here is the one it looks up.
const NORMALIZE: bool = true;

/// The tables `query` names that it does not create itself, each with what the pipeline does with it. A query that does
/// not parse names nothing here: the planner says what is wrong with it. A table both read and written is refused: the
/// pipeline would feed itself, and Arroyo gives a table one role.
pub fn names_of(query: &str) -> anyhow::Result<Vec<Named>> {
    let Ok(statements) = Parser::parse_sql(&ArroyoDialect {}, query) else {
        return Ok(Vec::new());
    };
    let mut created = BTreeSet::new();
    let mut written = BTreeSet::new();
    let mut read = BTreeSet::new();
    for statement in &statements {
        match statement {
            // A table the query declares, with its connector, is the query's own.
            Statement::CreateTable(create) => {
                created.insert(object_name_to_table_reference(create.name.clone(), NORMALIZE)?);
            }
            // What an insert writes is not what it reads: only its query is read.
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &insert.table {
                    written.insert(object_name_to_table_reference(name.clone(), NORMALIZE)?);
                }
                if let Some(source) = &insert.source {
                    read.extend(relations(&Statement::Query(source.clone()))?);
                }
            }
            other => read.extend(relations(other)?),
        }
    }
    if let Some(both) = written.intersection(&read).find(|r| !created.contains(*r)) {
        bail!("{both} is both read and written by this pipeline: it would feed itself");
    }

    let named = |reference: &TableReference, role| Named {
        path: path_of(reference),
        reference: reference.clone(),
        role,
    };
    let sinks = written
        .iter()
        .filter(|r| !created.contains(*r))
        .map(|r| named(r, Role::Sink));
    let sources = read
        .iter()
        .filter(|r| !created.contains(*r))
        .map(|r| named(r, Role::Source));
    Ok(sinks.chain(sources).collect())
}

/// The tables a statement reads, without the names of its CTEs.
fn relations(statement: &Statement) -> anyhow::Result<Vec<TableReference>> {
    let (relations, _ctes) = resolve_table_references(&DFStatement::Statement(Box::new(statement.clone())), NORMALIZE)?;
    Ok(relations)
}

/// MetaLake's name of a table, from the parts the planner split it into.
fn path_of(reference: &TableReference) -> TablePath {
    TablePath::from_parts(reference.catalog(), reference.schema(), reference.table())
}

/// Where the catalog is, as the process that plans a pipeline reaches it: what a client reaches it by, from the same
/// environment. A pipeline's tables carry only the names of these variables, which each worker reads from its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    /// The catalog's connection string, with its password.
    pub postgres: String,
    pub schema: String,
    /// The object store's variables set here, by name.
    pub storage: Vec<String>,
}

impl Environment {
    /// The environment's lake, if it names one: without `METALAKE_POSTGRES`, names are left to the planner.
    pub fn from_env() -> Option<Self> {
        let postgres = std::env::var(environment::POSTGRES).ok()?;
        let password = std::env::var(environment::PASSWORD).ok();
        let mut storage: Vec<String> = std::env::vars_os()
            .filter_map(|(name, _)| name.into_string().ok())
            .filter(|name| environment::is_storage_variable(name))
            .collect();
        storage.sort();
        Some(Self {
            postgres: environment::with_password(&postgres, password.as_deref()),
            schema: std::env::var(environment::SCHEMA).unwrap_or_else(|_| environment::DEFAULT_SCHEMA.to_string()),
            storage,
        })
    }

    /// The profile every resolved table shares: the variables' names, never their values.
    fn profile(&self) -> MetaLakeProfile {
        let placeholder = |name: &str| VarStr::new(format!("{{{{ {name} }}}}"));
        MetaLakeProfile {
            postgres: placeholder(environment::POSTGRES),
            schema: self.schema.clone(),
            storage: self
                .storage
                .iter()
                .map(|name| (name.to_ascii_lowercase(), placeholder(name)))
                .collect(),
        }
    }
}

/// The MetaLake tables `query` names, each as a table of Arroyo's with the `metalake` connector and its role, as a
/// `CREATE TABLE … WITH (connector = 'metalake', …)` would declare it. A name `known` to the planner already, or that
/// no ref or table of the catalog has, is left to it. With nothing to resolve, the catalog is not reached.
pub async fn resolve(query: &str, known: impl Fn(&str) -> bool, env: &Environment) -> anyhow::Result<Vec<Connection>> {
    let wanted: Vec<Named> = names_of(query)?
        .into_iter()
        .filter(|n| !known(&n.reference.to_string()))
        .collect();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let mut config = CatalogConfig::new(env.postgres.clone(), &env.schema);
    config.connections = 1;
    let catalog = Catalog::connect(config).await?;
    let found = tables_of(&catalog, &wanted, env).await;
    catalog.close();
    found
}

async fn tables_of(catalog: &Catalog, wanted: &[Named], env: &Environment) -> anyhow::Result<Vec<Connection>> {
    let mut tables = Vec::new();
    for named in wanted {
        let path = &named.path;
        let on = match catalog.branch(&path.on).await {
            Ok(on) => on,
            Err(metalake::Error::NotFound(_)) => continue,
            Err(e) => return Err(anyhow!("{path}: {e}")),
        };
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        let table = match on.table(&namespace, &path.name).await {
            Ok(table) => table,
            Err(metalake::Error::NotFound(_)) => continue,
            Err(e) => return Err(anyhow!("{path}: {e}")),
        };
        let fields = table
            .schema()
            .fields()
            .iter()
            .map(|field| {
                SourceField::try_from(field.as_ref().clone())
                    .map_err(|e| anyhow!("{path}: column {} has a type Arroyo does not take: {e}", field.name()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let schema = ConnectionSchema::try_new(None, None, None, fields, None, Some(false), Default::default())?;
        let table_type = match named.role {
            Role::Source => MetaLakeTableType::Source(MetaLakeSource {
                start: StartMode::default(),
                from: None,
                poll_interval_ms: None,
            }),
            Role::Sink => MetaLakeTableType::Sink(MetaLakeSink {
                intent_ttl_seconds: None,
                key: Vec::new(),
            }),
        };
        let config = MetaLakeTable {
            branch: path.on.clone(),
            namespace: path.namespace.clone(),
            table: path.name.clone(),
            table_type,
        };
        let name = named.reference.to_string();
        tables.push(MetaLakeConnector {}.from_config(None, &name, env.profile(), config, Some(&schema))?);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roles(query: &str) -> Vec<(String, Role)> {
        let mut found: Vec<(String, Role)> = names_of(query)
            .unwrap()
            .into_iter()
            .map(|n| (n.path.to_string(), n.role))
            .collect();
        found.sort_by(|a, b| a.0.cmp(&b.0));
        found
    }

    #[test]
    fn insert_reads_sources_writes_sink() {
        let query = "INSERT INTO main.shop.totals \
                     SELECT o.customer, count(*), sum(o.amount) FROM main.shop.orders o \
                     JOIN shop.customers c ON o.customer = c.id GROUP BY o.customer";
        assert_eq!(
            roles(query),
            [
                ("main.shop.customers".into(), Role::Source),
                ("main.shop.orders".into(), Role::Source),
                ("main.shop.totals".into(), Role::Sink),
            ]
        );
    }

    #[test]
    fn names_by_metalake_rule() {
        let query = "INSERT INTO totals SELECT * FROM dev.shop.orders UNION ALL SELECT * FROM shop.returns";
        assert_eq!(
            roles(query),
            [
                ("dev.shop.orders".into(), Role::Source),
                ("main.default.totals".into(), Role::Sink),
                ("main.shop.returns".into(), Role::Source),
            ]
        );
        let nested = names_of(r#"INSERT INTO main."a.b".t SELECT 1"#).unwrap();
        assert_eq!(
            nested[0].path.namespace,
            ["a", "b"],
            "a schema of several levels is a nested namespace"
        );
    }

    #[test]
    fn reference_is_what_planner_looks_up() {
        let named = names_of("INSERT INTO shop.totals SELECT * FROM Orders").unwrap();
        let references: BTreeSet<String> = named.iter().map(|n| n.reference.to_string()).collect();
        assert_eq!(
            references,
            BTreeSet::from(["orders".to_string(), "shop.totals".to_string()])
        );
    }

    #[test]
    fn ctes_and_subqueries() {
        let query = "INSERT INTO shop.totals \
                     WITH recent AS (SELECT * FROM shop.orders WHERE amount > 0) \
                     SELECT customer, count(*) FROM recent \
                     WHERE customer IN (SELECT id FROM shop.customers) GROUP BY customer";
        assert_eq!(
            roles(query),
            [
                ("main.shop.customers".into(), Role::Source),
                ("main.shop.orders".into(), Role::Source),
                ("main.shop.totals".into(), Role::Sink),
            ],
            "a CTE's name is not a table"
        );
    }

    #[test]
    fn declared_tables_are_query_own() {
        let query = "CREATE TABLE events (id BIGINT) WITH (connector = 'kafka', topic = 'e', type = 'source', \
                     format = 'json'); \
                     INSERT INTO shop.totals SELECT id, count(*) FROM events GROUP BY id";
        assert_eq!(roles(query), [("main.shop.totals".into(), Role::Sink)]);
    }

    #[test]
    fn several_statements() {
        let query = "INSERT INTO shop.a SELECT * FROM shop.x; INSERT INTO shop.b SELECT * FROM shop.x";
        assert_eq!(
            roles(query),
            [
                ("main.shop.a".into(), Role::Sink),
                ("main.shop.b".into(), Role::Sink),
                ("main.shop.x".into(), Role::Source),
            ]
        );
    }

    #[test]
    fn read_and_written_refused() {
        let refused = names_of("INSERT INTO shop.t SELECT * FROM shop.t").unwrap_err();
        assert!(
            refused.to_string().contains("shop.t is both read and written"),
            "{refused}"
        );
    }

    #[test]
    fn unparsable_names_nothing() {
        assert_eq!(names_of("INSERT INTO WHERE").unwrap(), []);
    }
}
