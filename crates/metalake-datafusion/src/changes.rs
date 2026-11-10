//! `table_changes('t', after [, until])`: the rows a range of a table's commits inserted and deleted (CHG-2, CHG-3), as
//! a table a statement reads like any other. Which files to read is found when the statement is prepared; each read
//! opens them again, so that a prepared statement read twice gives the same rows.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, ObjectName, Statement, TableFactor, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures_util::TryStreamExt;
use metalake::{Storage, TableChanges, TablePath};

/// The name a statement calls it by.
pub(crate) const FUNCTION: &str = "table_changes";

/// The rows of a range of commits, read in one partition, commit after commit.
#[derive(Debug)]
struct Partition {
    changes: TableChanges,
    storage: Storage,
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        self.changes.schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let rows = self
            .changes
            .read(&self.storage)
            .map_err(|e| DataFusionError::External(Box::new(e)));
        Box::pin(RecordBatchStreamAdapter::new(self.changes.schema().clone(), rows))
    }
}

/// The range as a table: bounded, so that a statement over it ends.
pub(crate) fn provider(changes: TableChanges, storage: Storage) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let schema = changes.schema().clone();
    let partition = Arc::new(Partition { changes, storage });
    Ok(Arc::new(StreamingTable::try_new(schema, vec![partition])?))
}

/// A call of `table_changes` in a statement: the table it names, by MetaLake's rule, and its range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Call {
    pub path: TablePath,
    pub after: i64,
    pub until: Option<i64>,
}

/// The calls of `table_changes` a statement makes, in the order a visit meets them.
pub(crate) fn calls(statement: &Statement) -> Result<Vec<Call>, DataFusionError> {
    struct Found(Vec<Call>);
    impl Visitor for Found {
        type Break = DataFusionError;
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match call_of(factor) {
                Ok(Some(call)) => self.0.push(call),
                Ok(None) => {}
                Err(e) => return ControlFlow::Break(e),
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

/// Each call of `table_changes`, in the order a visit meets them, replaced by the table `names` give for it.
pub(crate) fn replace(statement: &mut Statement, names: &[String]) -> Result<(), DataFusionError> {
    let names = names
        .iter()
        .map(|name| {
            Parser::new(&GenericDialect {})
                .try_with_sql(name)
                .and_then(|mut p| p.parse_object_name(false))
                .map_err(|e| DataFusionError::Internal(format!("{name}: {e}")))
        })
        .collect::<Result<Vec<ObjectName>, _>>()?;
    struct Replace(std::vec::IntoIter<ObjectName>);
    impl VisitorMut for Replace {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, args, .. } = factor
                && is_call(name)
                && args.is_some()
                && let Some(replaced) = self.0.next()
            {
                *name = replaced;
                *args = None;
            }
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Replace(names.into_iter()));
    Ok(())
}

fn is_call(name: &ObjectName) -> bool {
    name.to_string().eq_ignore_ascii_case(FUNCTION)
}

/// A table factor's call of `table_changes`, if it is one: `('t', after)` or `('t', after, until)`, all literals.
fn call_of(factor: &TableFactor) -> Result<Option<Call>, DataFusionError> {
    let TableFactor::Table {
        name, args: Some(args), ..
    } = factor
    else {
        return Ok(None);
    };
    if !is_call(name) {
        return Ok(None);
    }
    let usage = |why: &str| DataFusionError::Plan(format!("{FUNCTION}(table, after [, until]): {why}"));
    let values: Vec<&Value> = args
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))) => Ok(&v.value),
            _ => Err(usage(
                "its arguments are a table's name and commit sequences, as literals",
            )),
        })
        .collect::<Result<_, _>>()?;
    let sequence = |value: &Value, what: &str| match value {
        Value::Number(n, _) => n
            .parse::<i64>()
            .map_err(|_| usage(&format!("{what} is a commit sequence, not {n}"))),
        other => Err(usage(&format!("{what} is a commit sequence, not {other}"))),
    };
    let (table, after, until) = match values.as_slice() {
        [table, after] => (*table, sequence(after, "after")?, None),
        [table, after, until] => (*table, sequence(after, "after")?, Some(sequence(until, "until")?)),
        _ => return Err(usage("it takes two or three arguments")),
    };
    let Value::SingleQuotedString(table) = table else {
        return Err(usage("the table is named as a string, as 'db.t'"));
    };
    let path = TablePath::parse(table).map_err(|e| usage(&e.to_string()))?;
    if until.is_some_and(|until| until < after) {
        return Err(usage("the end comes before the start"));
    }
    Ok(Some(Call { path, after, until }))
}
