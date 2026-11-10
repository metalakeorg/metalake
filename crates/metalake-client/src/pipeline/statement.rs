//! What a pipeline's query reads and writes, by MetaLake's names. Arroyo reads the same query by the same rule when it
//! plans it (`names_of` in metalake-arroyo); this is the check a client makes first, so that a mistake is said at once,
//! and not later in a pipeline's log.

use datafusion::common::TableReference;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::ast::{Statement, TableObject};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::{Error, Result, TablePath};

/// Identifiers are normalized as Arroyo's planner normalizes them.
const NORMALIZE: bool = true;

/// The tables of the lake a pipeline reads, and those it writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Names {
    pub reads: Vec<TablePath>,
    pub writes: Vec<TablePath>,
}

/// What `query` reads and writes. A pipeline writes into at least one table, with `INSERT INTO t SELECT …`, names only
/// the lake's tables, and never reads one it writes, since it would feed itself.
pub fn names_of(query: &str) -> Result<Names> {
    let statements = Parser::parse_sql(&GenericDialect {}, query)
        .map_err(|e| Error::usage(format!("the pipeline's query does not parse: {e}")))?;
    let mut reads: Vec<TablePath> = Vec::new();
    let mut writes: Vec<TablePath> = Vec::new();
    let add = |into: &mut Vec<TablePath>, path: TablePath| {
        if !into.contains(&path) {
            into.push(path);
        }
    };
    for statement in &statements {
        match statement {
            Statement::Insert(insert) => {
                let TableObject::TableName(name) = &insert.table else {
                    return Err(Error::usage("a pipeline inserts into a table of the lake, by its name"));
                };
                let reference = object_name_to_table_reference(name.clone(), NORMALIZE)?;
                add(&mut writes, path_of(&reference));
                if let Some(source) = &insert.source {
                    for read in relations(&Statement::Query(source.clone()))? {
                        add(&mut reads, read);
                    }
                }
            }
            Statement::CreateTable(_) => {
                return Err(Error::usage(
                    "a pipeline names the lake's tables and declares none: create a table with `sql` first",
                ));
            }
            other => {
                return Err(Error::usage(format!(
                    "a pipeline is made of `INSERT INTO t SELECT …`, not {}",
                    other
                        .to_string()
                        .split_whitespace()
                        .take(2)
                        .collect::<Vec<_>>()
                        .join(" ")
                )));
            }
        }
    }
    if writes.is_empty() {
        return Err(Error::usage(
            "a pipeline writes into a table: `INSERT INTO t SELECT … FROM s`",
        ));
    }
    if let Some(both) = reads.iter().find(|read| writes.contains(read)) {
        return Err(Error::usage(format!(
            "{both} is both read and written by this pipeline: it would feed itself"
        )));
    }
    Ok(Names { reads, writes })
}

/// The tables a statement reads, without the names of its CTEs.
fn relations(statement: &Statement) -> Result<Vec<TablePath>> {
    let (relations, _ctes) = resolve_table_references(&DFStatement::Statement(Box::new(statement.clone())), NORMALIZE)?;
    Ok(relations.iter().map(path_of).collect())
}

fn path_of(reference: &TableReference) -> TablePath {
    TablePath::from_parts(reference.catalog(), reference.schema(), reference.table())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(names: &[&str]) -> Vec<TablePath> {
        names.iter().map(|n| TablePath::parse(n).unwrap()).collect()
    }

    #[test]
    fn reads_and_writes() {
        let names = names_of(
            "INSERT INTO shop.totals SELECT o.customer, count(*) FROM shop.orders o \
             JOIN main.shop.customers c ON o.customer = c.name GROUP BY o.customer",
        )
        .unwrap();
        assert_eq!(names.writes, paths(&["shop.totals"]));
        assert_eq!(names.reads, paths(&["shop.orders", "shop.customers"]));
    }

    #[test]
    fn ctes_are_not_tables() {
        let names = names_of("INSERT INTO t WITH big AS (SELECT * FROM s WHERE x > 1) SELECT * FROM big").unwrap();
        assert_eq!(names.reads, paths(&["s"]));
    }

    #[test]
    fn refusals() {
        for (query, says) in [
            ("SELECT * FROM s", "INSERT INTO"),
            ("INSERT INTO s SELECT * FROM s", "feed itself"),
            ("INSERT INTO main.db.s SELECT * FROM db.s", "feed itself"),
            ("CREATE TABLE x (a int)", "declares none"),
            ("INSERT INTO", "does not parse"),
        ] {
            let refused = names_of(query).unwrap_err();
            assert!(refused.message.contains(says), "{query}: {}", refused.message);
        }
    }
}
