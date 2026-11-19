//! `read_orc('path')`: the ORC files of a directory of the lake's store, as a table a statement reads, so that a
//! table another system keeps in ORC is brought into a MetaLake table by SQL alone:
//!
//! ```sql
//! CREATE TABLE marts.top_clientes AS SELECT * FROM read_orc('/user/hive/warehouse/marts.db/top_clientes');
//! ```
//!
//! ORC is read, never written: a lake's tables stay Parquet. The path is one in the store, or a URL of it, as
//! `register` takes one. One directory of files is read, its hidden ones (`_SUCCESS`, `.staging`) left out:
//! - a directory with subdirectories, as a partitioned one, is refused, since its partition values would be lost;
//! - a Hive table that is transactional (ACID) is refused, since its rows are its base and its deltas together, and
//!   files read one by one would give rows deleted or old.
//!
//! Found when the statement is prepared, as `table_changes` is: listing the directory is awaited, which a table
//! function of DataFusion cannot do.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Statement, TableFactor, Value, Visit, Visitor,
};
use datafusion_orc::OrcFormat;
use metalake::Storage;

use crate::changes::is_call;

/// The name a statement calls it by.
pub(crate) const FUNCTION: &str = "read_orc";

/// The directories a statement's calls of `read_orc` name, in the order a visit meets them.
pub(crate) fn calls(statement: &Statement) -> Result<Vec<String>, DataFusionError> {
    struct Found(Vec<String>);
    impl Visitor for Found {
        type Break = DataFusionError;
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name, args: Some(args), ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            if !is_call(name, FUNCTION) {
                return ControlFlow::Continue(());
            }
            match args.args.as_slice() {
                [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] => match &value.value {
                    Value::SingleQuotedString(path) => self.0.push(path.clone()),
                    _ => return ControlFlow::Break(usage("the path is a string, as '/user/hive/warehouse/t'")),
                },
                _ => return ControlFlow::Break(usage("it takes one argument, the directory's path")),
            }
            ControlFlow::Continue(())
        }
    }
    let mut found = Found(Vec::new());
    match statement.visit(&mut found) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(found.0),
    }
}

fn usage(why: &str) -> DataFusionError {
    DataFusionError::Plan(format!("{FUNCTION}('path'): {why}"))
}

/// The ORC files of the directory `path` names, as a table: its schema read from their footers.
pub(crate) async fn provider(
    state: &SessionState,
    storage: &Storage,
    path: &str,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let url = storage.url_of(path).map_err(|e| usage(&e.to_string()))?;
    let listed = storage
        .listed(&url)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut files = Vec::new();
    for object in listed {
        let relative = object.url[url.len()..].trim_start_matches('/');
        let parts: Vec<&str> = relative.split('/').collect();
        if parts
            .iter()
            .any(|p| p.starts_with("delta_") || p.starts_with("delete_delta_") || p.starts_with("base_"))
        {
            return Err(usage(&format!(
                "{url} is a transactional (ACID) table of Hive: its rows are its base and its deltas together, which \
                 {FUNCTION} does not merge. Copy it with Hive, or with an engine that reads it through USING"
            )));
        }
        if parts.iter().any(|p| p.starts_with('_') || p.starts_with('.')) {
            continue;
        }
        if parts.len() > 1 {
            return Err(usage(&format!(
                "{url} holds directories, as {relative} lies in one: {FUNCTION} reads one directory of files, and a \
                 partitioned one's values would be lost"
            )));
        }
        files.push(ListingTableUrl::parse(&object.url)?);
    }
    if files.is_empty() {
        return Err(usage(&format!("{url} holds no file to read")));
    }
    files.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    // Hive names its files with no extension, `000000_0`: every file listed is read.
    let options = ListingOptions::new(Arc::new(OrcFormat)).with_file_extension("");
    let config = ListingTableConfig::new_with_multi_paths(files)
        .with_listing_options(options)
        .infer_schema(state)
        .await?;
    Ok(Arc::new(ListingTable::try_new(config)?))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::{DFParser, Statement as DFStatement};

    use super::*;

    fn statement(sql: &str) -> Statement {
        match DFParser::parse_sql(sql).unwrap().pop_front().unwrap() {
            DFStatement::Statement(statement) => *statement,
            other => panic!("{other}"),
        }
    }

    #[test]
    fn read_orc_calls_found() {
        let found = calls(&statement(
            "create table t as select * from read_orc('/a') join READ_ORC('hdfs://nn/b') on true",
        ))
        .unwrap();
        assert_eq!(found, ["/a", "hdfs://nn/b"]);
        assert!(calls(&statement("select * from t")).unwrap().is_empty());
        assert!(calls(&statement("select * from read_orc(1)")).is_err());
        assert!(calls(&statement("select * from read_orc('/a', '/b')")).is_err());
    }
}
