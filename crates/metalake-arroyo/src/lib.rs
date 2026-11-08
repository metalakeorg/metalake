//! MetaLake's connector for Arroyo: the core runs in Arroyo's worker, and this crate translates between them. It decides
//! nothing about the lake: the core decides what a commit changed and how files are written and committed.
//!
//! A source follows a table as rows inserted and deleted, so its table is updating: the planner unrolls its rows, as it
//! does Debezium's, and every subtask starts from one time, stamped when the pipeline is planned (CHG-5). A sink writes
//! a table's files at each checkpoint and commits them once it is complete.

pub mod changelog;
pub mod config;
pub mod conform;
pub mod envelope;
pub mod epoch;
pub mod inserted;
pub mod lake;
pub mod locate;
pub mod progress;
pub mod schema;
pub mod sink;
pub mod source;
pub mod split;

use anyhow::{anyhow, bail};
use arrow_schema::{Field, Schema};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use chrono::Utc;
use datafusion::common::plan_datafusion_err;
use metalake::{Catalog, CatalogConfig};
use tokio::sync::mpsc::Sender;

use crate::config::{MetaLakeProfile, MetaLakeTable, MetaLakeTableType};
use crate::schema::{check_columns, check_sink_columns, render_schema};
use crate::sink::MetaLakeSinkFunc;
use crate::source::MetaLakeSourceFunc;

const ICON: &str = include_str!("../metalake.svg");

pub struct MetaLakeConnector {}

impl Connector for MetaLakeConnector {
    type ProfileT = MetaLakeProfile;
    type TableT = MetaLakeTable;

    fn name(&self) -> &'static str {
        "metalake"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "metalake".to_string(),
            name: "MetaLake".to_string(),
            icon: ICON.to_string(),
            description: "Follow a MetaLake table's changes, or write to one".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(render_schema::<Self::ProfileT>()),
            table_config: render_schema::<Self::TableT>(),
        }
    }

    fn config_description(&self, profile: Self::ProfileT) -> String {
        format!("schema {}", profile.schema)
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.table_type {
            MetaLakeTableType::Source(_) => ConnectionType::Source,
            MetaLakeTableType::Sink(_) => ConnectionType::Sink,
        }
    }

    fn test(
        &self,
        _: &str,
        profile: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        let declared = schema.map(declared_schema);
        tokio::task::spawn(async move {
            let message = match test_table(&profile, &table, declared).await {
                Ok(()) => TestSourceMessage::done("the table is there, with the columns declared"),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };
            let _ = tx.send(message).await;
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let profile = match profile {
            Some(p) => serde_json::from_value(p.config.clone())
                .map_err(|e| plan_datafusion_err!("invalid config for profile '{}' in database: {}", p.id, e))?,
            None => options.pull_struct()?,
        };
        self.from_config(None, name, profile, options.pull_struct()?, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        profile: Self::ProfileT,
        mut table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .filter(|s| !s.fields.is_empty())
            .cloned()
            .ok_or_else(|| anyhow!("a MetaLake table needs its columns declared"))?;
        if schema.format.is_some() {
            bail!("a MetaLake table takes no 'format': its rows are the table's columns");
        }
        if schema.fields.iter().any(|f| f.metadata_key.is_some()) {
            bail!("a MetaLake table has no metadata fields");
        }

        let (connection_type, kind) = match &mut table.table_type {
            MetaLakeTableType::Source(source) => {
                // planned once for a pipeline, so that all of its subtasks start from the same time
                source.from_time()?;
                source.stamp(Utc::now());
                (ConnectionType::Source, "Source")
            }
            MetaLakeTableType::Sink(sink) => {
                sink.validate()?;
                let mut key: Vec<String> = schema.primary_keys.iter().cloned().collect();
                key.sort();
                sink.key = key;
                (ConnectionType::Sink, "Sink")
            }
        };
        let description = format!("MetaLake{kind}<{}>", table.qualified_name());

        let config = OperatorConfig {
            connection: serde_json::to_value(profile)?,
            table: serde_json::to_value(&table)?,
            format: None,
            bad_data: schema.bad_data.clone(),
            framing: None,
            rate_limit: None,
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type.clone(),
            schema,
            &config,
            description,
        )
        // a source emits rows inserted and deleted, and a sink takes them
        .with_updating(true))
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table.table_type {
            MetaLakeTableType::Source(_) => Ok(ConstructedOperator::from_source(Box::new(MetaLakeSourceFunc::new(
                profile, table,
            )?))),
            MetaLakeTableType::Sink(_) => Ok(ConstructedOperator::from_operator(Box::new(MetaLakeSinkFunc::new(
                profile, table,
            )?))),
        }
    }
}

/// The columns as declared, without Arroyo's own.
fn declared_schema(schema: &ConnectionSchema) -> Schema {
    Schema::new(schema.fields.iter().map(|f| Field::from(f.clone())).collect::<Vec<_>>())
}

/// Finds the table on its branch, and checks the declared columns against its own.
async fn test_table(profile: &MetaLakeProfile, table: &MetaLakeTable, declared: Option<Schema>) -> anyhow::Result<()> {
    let mut config = CatalogConfig::new(profile.postgres.sub_env_vars()?, &profile.schema);
    config.connections = 1;
    let catalog = Catalog::connect(config).await?;
    let namespace: Vec<&str> = table.namespace.iter().map(String::as_str).collect();
    let found = catalog
        .branch(&table.branch)
        .await?
        .table(&namespace, &table.table)
        .await?;
    if let Some(declared) = declared {
        let checked = match table.table_type {
            MetaLakeTableType::Source(_) => check_columns(&declared, found.schema()),
            MetaLakeTableType::Sink(_) => check_sink_columns(&declared, found.schema()),
        };
        checked.map_err(|e| anyhow!("{}: {e}", table.qualified_name()))?;
    }
    catalog.close();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MetaLakeSink, MetaLakeSource, StartMode};
    use arroyo_rpc::api_types::connections::{FieldType, SourceField};
    use arroyo_rpc::formats::{Format, JsonFormat};
    use arroyo_rpc::var_str::VarStr;

    fn profile() -> MetaLakeProfile {
        MetaLakeProfile {
            postgres: VarStr::new("host=pg".to_string()),
            schema: "lake".to_string(),
            storage: Default::default(),
        }
    }

    fn table(table_type: MetaLakeTableType) -> MetaLakeTable {
        MetaLakeTable {
            branch: "main".to_string(),
            namespace: vec!["shop".to_string()],
            table: "orders".to_string(),
            table_type,
        }
    }

    fn source(from: Option<&str>) -> MetaLakeTableType {
        MetaLakeTableType::Source(MetaLakeSource {
            start: StartMode::Snapshot,
            from: from.map(String::from),
            poll_interval_ms: None,
        })
    }

    fn columns(format: Option<Format>) -> ConnectionSchema {
        let field = |name: &str, field_type| SourceField {
            name: name.to_string(),
            field_type,
            required: true,
            sql_name: None,
            metadata_key: None,
        };
        let fields = vec![field("id", FieldType::Int64), field("amount", FieldType::Float64)];
        ConnectionSchema::try_new(format, None, None, fields, None, Some(false), Default::default()).unwrap()
    }

    fn connect(table_type: MetaLakeTableType, schema: Option<&ConnectionSchema>) -> anyhow::Result<Connection> {
        MetaLakeConnector {}.from_config(None, "orders", profile(), table(table_type), schema)
    }

    fn planned(connection: &Connection) -> MetaLakeTable {
        let config: OperatorConfig = serde_json::from_str(&connection.config).unwrap();
        assert_eq!(config.format, None);
        serde_json::from_value(config.table).unwrap()
    }

    #[test]
    fn chg5_source_connection_updating_and_stamped() {
        let schema = columns(None);
        let connection = connect(source(None), Some(&schema)).unwrap();
        assert!(connection.updating);
        assert_eq!(connection.connection_type, ConnectionType::Source);
        assert_eq!(connection.description, "MetaLakeSource<main:shop.orders>");
        let MetaLakeTableType::Source(stamped) = planned(&connection).table_type else {
            panic!("a source")
        };
        assert!(stamped.from_time().unwrap().is_some(), "stamped when planned");

        let given = connect(source(Some("2026-11-08T09:00:00Z")), Some(&schema)).unwrap();
        let MetaLakeTableType::Source(kept) = planned(&given).table_type else {
            panic!("a source")
        };
        assert_eq!(
            kept.from.as_deref(),
            Some("2026-11-08T09:00:00Z"),
            "a time given is kept"
        );
    }

    #[test]
    fn sink_connection_updating() {
        let mut schema = columns(None);
        schema.primary_keys = ["id".to_string()].into_iter().collect();
        let connection = connect(MetaLakeTableType::Sink(MetaLakeSink::default()), Some(&schema)).unwrap();
        assert!(connection.updating);
        let MetaLakeTableType::Sink(sink) = planned(&connection).table_type else {
            panic!("a sink")
        };
        assert_eq!(sink.key, ["id"], "the declared primary key");
        assert_eq!(connection.connection_type, ConnectionType::Sink);
        assert_eq!(connection.description, "MetaLakeSink<main:shop.orders>");
    }

    #[test]
    fn connection_invalid_schema_refused() {
        let error = |result: anyhow::Result<Connection>| result.unwrap_err().to_string();
        assert!(error(connect(source(None), None)).contains("columns declared"));
        let json = Format::Json(JsonFormat::default());
        assert!(error(connect(source(None), Some(&columns(Some(json))))).contains("no 'format'"));
        assert!(error(connect(source(Some("yesterday")), Some(&columns(None)))).contains("RFC 3339"));
    }

    #[test]
    fn connector_metadata() {
        let metadata = MetaLakeConnector {}.metadata();
        assert_eq!((metadata.source, metadata.sink, metadata.hidden), (true, true, false));
        assert!(metadata.icon.starts_with("<svg"));
        let profile: serde_json::Value = serde_json::from_str(&metadata.connection_config.unwrap()).unwrap();
        assert_eq!(profile["title"], "MetaLake catalog");
    }
}
