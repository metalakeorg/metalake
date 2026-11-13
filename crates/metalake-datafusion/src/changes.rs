//! `table_changes('t', after [, until])`: the rows a range of a table's commits inserted and deleted (CHG-2, CHG-3), as
//! a table a statement reads like any other. Which files to read is found when the statement is prepared; each read
//! opens them again, so that a prepared statement read twice gives the same rows.

use std::ops::ControlFlow;
use std::sync::Arc;

use std::fmt;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, ObjectName, Query, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
    Value, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures_util::StreamExt;
use metalake::{Storage, TableChanges, TablePath};

/// The name a statement calls it by.
pub(crate) const FUNCTION: &str = "table_changes";

/// The changes of a range of commits as a table: bounded, so that a statement over it ends. A table of its own, not
/// DataFusion's streaming one, so that a plan holding it can name it and send it as its value.
#[derive(Debug)]
pub(crate) struct ChangesTable {
    pub changes: TableChanges,
    pub storage: Storage,
}

#[async_trait]
impl TableProvider for ChangesTable {
    fn schema(&self) -> SchemaRef {
        self.changes.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[datafusion::prelude::Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = ChangesExec::new(self.changes.clone(), self.storage.clone(), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// The read of a range of commits: one partition, commit after commit, each file opened when it is read. It takes no
/// catalog: the tasks name the files, and storage holds them.
#[derive(Debug)]
pub(crate) struct ChangesExec {
    pub changes: TableChanges,
    pub storage: Storage,
    /// The columns read, by place, if not all.
    pub projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl ChangesExec {
    pub(crate) fn new(
        changes: TableChanges,
        storage: Storage,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        let schema = match &projection {
            Some(columns) => Arc::new(changes.schema().project(columns)?),
            None => changes.schema().clone(),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            changes,
            storage,
            projection,
            properties: Arc::new(properties),
        })
    }
}

impl DisplayAs for ChangesExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, files) = (self.changes.table().name(), self.changes.tasks().len());
        write!(f, "ChangesExec: {name}, {files} files")
    }
}

impl ExecutionPlan for ChangesExec {
    fn name(&self) -> &'static str {
        "ChangesExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    /// It holds no expressions.
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream, DataFusionError> {
        let projection = self.projection.clone();
        let rows = self.changes.read(&self.storage).map(move |batch| {
            let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
            match &projection {
                Some(columns) => Ok(batch.project(columns)?),
                None => Ok(batch),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), rows)))
    }
}

/// The range as a table.
pub(crate) fn provider(changes: TableChanges, storage: Storage) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    Ok(Arc::new(ChangesTable { changes, storage }))
}

/// A call of `table_changes` in a statement: the table it names, by MetaLake's rule, and its range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Call {
    pub path: TablePath,
    pub after: i64,
    pub until: Option<i64>,
}

/// The calls of `table_changes` a statement makes, in the order a visit meets them; a table that names no ref is on
/// `on`, the session's default ref, as the statement's other names are.
pub(crate) fn calls(statement: &Statement, on: &str) -> Result<Vec<Call>, DataFusionError> {
    struct Found<'a>(Vec<Call>, &'a str);
    impl Visitor for Found<'_> {
        type Break = DataFusionError;
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match call_of(factor, self.1) {
                Ok(Some(call)) => self.0.push(call),
                Ok(None) => {}
                Err(e) => return ControlFlow::Break(e),
            }
            ControlFlow::Continue(())
        }
    }
    let mut found = Found(Vec::new(), on);
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
    // A call with no alias is qualified by the function's name, `table_changes.k`, when it is the only one in its
    // query's FROM; two in one FROM would share it, and clash, so they keep their sources' names. A UNION's branches
    // and subqueries are scopes of their own.
    struct Scopes;
    impl VisitorMut for Scopes {
        type Break = ();
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            alias_lone_calls(&mut query.body);
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Scopes);
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

/// In each SELECT of a query's body (each branch of a UNION), the function's name as the alias of its call with none,
/// when it is the only one in that FROM.
fn alias_lone_calls(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            let mut calls = Vec::new();
            for from in select.from.iter_mut() {
                unaliased_calls(from, &mut calls);
            }
            if let [call] = calls.as_mut_slice()
                && let TableFactor::Table { name, alias, .. } = &mut **call
                && let Some(called) = name.0.first().and_then(|part| part.as_ident()).cloned()
            {
                *alias = Some(TableAlias {
                    explicit: true,
                    name: called,
                    columns: Vec::new(),
                    at: None,
                });
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            alias_lone_calls(left);
            alias_lone_calls(right);
        }
        _ => {}
    }
}

/// The calls with no alias of one FROM item, its joins and nested joins included; a subquery is a scope of its own.
fn unaliased_calls<'a>(from: &'a mut TableWithJoins, calls: &mut Vec<&'a mut TableFactor>) {
    let TableWithJoins { relation, joins } = from;
    for factor in std::iter::once(relation).chain(joins.iter_mut().map(|join| &mut join.relation)) {
        let lone = matches!(&*factor, TableFactor::Table { name, args: Some(_), alias: None, .. } if is_call(name));
        if lone {
            calls.push(factor);
        } else if let TableFactor::NestedJoin { table_with_joins, .. } = factor {
            unaliased_calls(table_with_joins, calls);
        }
    }
}

/// Whether a name is the function's, by SQL's rule: unquoted in any case, quoted exactly.
fn is_call(name: &ObjectName) -> bool {
    match name.0.as_slice() {
        [part] => part.as_ident().is_some_and(|ident| match ident.quote_style {
            None => ident.value.eq_ignore_ascii_case(FUNCTION),
            Some(_) => ident.value == FUNCTION,
        }),
        _ => false,
    }
}

/// A table factor's call of `table_changes`, if it is one: `('t', after)` or `('t', after, until)`, all literals.
fn call_of(factor: &TableFactor, on: &str) -> Result<Option<Call>, DataFusionError> {
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
    let path = TablePath::parse_on(table, on).map_err(|e| usage(&e.to_string()))?;
    if until.is_some_and(|until| until < after) {
        return Err(usage("the end comes before the start"));
    }
    Ok(Some(Call { path, after, until }))
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
    fn calls_by_sql_rule() {
        for sql in [
            "select * from table_changes('db.t', 1)",
            "select * from TABLE_CHANGES('db.t', 1)",
            r#"select * from "table_changes"('db.t', 1)"#,
        ] {
            assert_eq!(calls(&statement(sql), "main").unwrap().len(), 1, "{sql}");
        }
        for sql in [
            r#"select * from "TABLE_CHANGES"('db.t', 1)"#,
            "select * from main.table_changes",
            "select * from db.table_changes('db.t', 1)",
        ] {
            assert!(calls(&statement(sql), "main").unwrap().is_empty(), "{sql}");
        }
    }

    /// A session on another ref: a table that names none is on it; one that names its ref keeps it.
    #[test]
    fn calls_on_session_ref() {
        let sql = "select * from table_changes('t', 1) a, table_changes('r.n.t', 1) b";
        let found = calls(&statement(sql), "dev").unwrap();
        let on: Vec<_> = found.iter().map(|c| c.path.on.as_str()).collect();
        assert_eq!(on, ["dev", "r"]);
    }

    #[test]
    fn replaced_call_keeps_qualifier() {
        let mut unaliased = statement("select table_changes.k from table_changes('db.t', 1)");
        replace(&mut unaliased, &[r#"".source"."default"."7""#.to_string()]).unwrap();
        assert_eq!(
            unaliased.to_string(),
            r#"SELECT table_changes.k FROM ".source"."default"."7" AS table_changes"#
        );
        let mut aliased = statement("select c.k from table_changes('db.t', 1) c");
        replace(&mut aliased, &[r#"".source"."default"."7""#.to_string()]).unwrap();
        assert_eq!(
            aliased.to_string(),
            r#"SELECT c.k FROM ".source"."default"."7" c"#,
            "an alias given stays as given"
        );
    }
}
