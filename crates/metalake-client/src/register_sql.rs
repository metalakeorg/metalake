//! Registering files in place in SQL: the verbs the command line and Python have, `register` and `register-schema`,
//! read by the client before DataFusion, as a catalog's verbs are ([`crate::catalogs_sql`]). They run on the branch the
//! session is on.
//!
//! ```sql
//! REGISTER TABLE tienda.ventas FROM '/user/hive/warehouse/tienda.db/ventas';   -- created; dt found in its directories
//! REGISTER TABLE tienda.ventas;                                               -- again: what is new, from there
//! REGISTER TABLE t FROM 'raw/pedidos' PARTITIONED BY (year INT, month INT) LOCATION 's3://b/lake/t' ADOPT CHECK;
//! REGISTER SCHEMA curated FROM '/user/hive/warehouse/curated.db';              -- each prefix under it a table
//! ```
//!
//! `FROM` is a path in the lake's store or a URL of it; left out, a table registers again from where it was.
//! `PARTITIONED BY` corrects the partition found in the directories; `CHECK` commits nothing. A table's answer is its
//! report, one row; a run with refused files commits nothing and fails, saying each.
//!
//! `REGISTER SCHEMA` registers each prefix right under `FROM` as a table of the schema, by its name, each in commits of
//! its own. Its answer is a row per table, the refused and failed among them, which leave the others registered.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::tokenizer::Token;
use metalake::register::Report;

use crate::words::Words;
use crate::{Error, Lake, Prepared, RegisterRequest, RegisterSchemaRequest, Result};

const GRAMMAR: &str = "REGISTER TABLE name [FROM 'path'] [PARTITIONED BY (column TYPE, …)] [LOCATION 'url'] [ADOPT] \
                       [CHECK], or REGISTER SCHEMA name FROM 'path' [ADOPT] [CHECK]";

/// `REGISTER TABLE` or `REGISTER SCHEMA`, as a statement says it: the request but its branch, the session's.
#[derive(Debug, Clone)]
pub enum RegisterVerb {
    Table(RegisterRequest),
    Schema(RegisterSchemaRequest),
}

/// The registering a text says, if it says one. `REGISTER` starts no other statement, so a text that starts so is this
/// or is refused.
pub fn register_verb_of(text: &str) -> Result<Option<RegisterVerb>> {
    let Some(mut words) = Words::of(text) else {
        return Ok(None);
    };
    if words.keyword().as_deref() != Some("REGISTER") {
        return Ok(None);
    }
    if words.take("SCHEMA") {
        return schema_of(&mut words).map(|verb| Some(RegisterVerb::Schema(verb)));
    }
    if !words.take("TABLE") {
        return Err(Error::usage(GRAMMAR));
    }
    let mut verb = RegisterRequest {
        table: words.dotted("the table")?,
        ..RegisterRequest::default()
    };
    let mut said: Vec<String> = Vec::new();
    while !words.at_end() {
        let Some(clause) = words.keyword() else {
            return Err(Error::usage(GRAMMAR));
        };
        if said.contains(&clause) {
            return Err(Error::usage(format!("{clause} is said twice: {GRAMMAR}")));
        }
        match clause.as_str() {
            "FROM" => verb.source = Some(words.string("FROM")?),
            "LOCATION" => verb.location = Some(words.string("LOCATION")?),
            "ADOPT" => verb.adopt = true,
            "CHECK" => verb.check = true,
            "PARTITIONED" if words.take("BY") => verb.partition = columns(&mut words)?,
            _ => return Err(Error::usage(GRAMMAR)),
        }
        said.push(clause);
    }
    words.end()?;
    Ok(Some(RegisterVerb::Table(verb)))
}

/// `REGISTER SCHEMA name FROM 'path' [ADOPT] [CHECK]`, past its first two words.
fn schema_of(words: &mut Words) -> Result<RegisterSchemaRequest> {
    let mut verb = RegisterSchemaRequest {
        schema: words.dotted("the schema")?,
        ..RegisterSchemaRequest::default()
    };
    let mut said: Vec<String> = Vec::new();
    while !words.at_end() {
        let Some(clause) = words.keyword() else {
            return Err(Error::usage(GRAMMAR));
        };
        if said.contains(&clause) {
            return Err(Error::usage(format!("{clause} is said twice: {GRAMMAR}")));
        }
        match clause.as_str() {
            "FROM" => verb.source = words.string("FROM")?,
            "ADOPT" => verb.adopt = true,
            "CHECK" => verb.check = true,
            _ => return Err(Error::usage(GRAMMAR)),
        }
        said.push(clause);
    }
    words.end()?;
    if verb.source.is_empty() {
        return Err(Error::usage(
            "REGISTER SCHEMA name FROM 'path': a schema registers from the prefix it is told",
        ));
    }
    Ok(verb)
}

/// `(column TYPE, …)`, each type as SQL names it, as the partition types `register` takes.
fn columns(words: &mut Words) -> Result<Vec<(String, String)>> {
    let usage = || Error::usage("PARTITIONED BY (column TYPE, …), a type of DATE, INT, BIGINT, STRING or BOOLEAN");
    if words.next() != Some(Token::LParen) {
        return Err(usage());
    }
    let mut columns = Vec::new();
    loop {
        let column = words.name("a partition column")?;
        let ty = match words.keyword().as_deref() {
            Some("DATE") => "date",
            Some("INT" | "INTEGER") => "int",
            Some("BIGINT" | "LONG") => "long",
            Some("STRING" | "VARCHAR" | "TEXT") => "string",
            Some("BOOLEAN" | "BOOL") => "boolean",
            _ => return Err(usage()),
        };
        columns.push((column, ty.to_string()));
        match words.next() {
            Some(Token::Comma) => continue,
            Some(Token::RParen) => return Ok(columns),
            _ => return Err(usage()),
        }
    }
}

/// Registers on `on`, the session's branch. A table's report is one row, its refused files failing it, saying each;
/// a schema's is a row per table, those not registered among them, saying why.
pub async fn run(lake: &Lake, verb: RegisterVerb, on: &str) -> Result<Prepared> {
    match verb {
        RegisterVerb::Table(mut verb) => {
            verb.on = Some(on.to_string());
            let report = lake.register(&verb).await?;
            if !report.check && !report.refused.is_empty() {
                return Err(Error::usage(format!(
                    "{} files refused, and nothing registered:\n{}",
                    report.refused.len(),
                    refusals(&report)
                )));
            }
            rows(&[Row::Report(&report)])
        }
        RegisterVerb::Schema(mut verb) => {
            verb.on = Some(on.to_string());
            let report = lake.register_schema(&verb).await?;
            let each: Vec<Row> = report
                .tables
                .iter()
                .map(|t| match (&t.report, &t.error) {
                    (Some(report), _) => Row::Report(report),
                    (None, error) => Row::Failed(&t.name, error.as_deref().unwrap_or("")),
                })
                .collect();
            rows(&each)
        }
    }
}

/// A row of the answer: a table's report, or a table of a schema not registered, and why.
enum Row<'a> {
    Report(&'a Report),
    Failed(&'a str, &'a str),
}

fn refusals(report: &Report) -> String {
    let lines: Vec<String> = report
        .refused
        .iter()
        .map(|r| format!("{} ({}): {}", r.path, r.rule, r.why))
        .collect();
    lines.join("\n")
}

/// The reports as rows, one per table.
fn rows(rows: &[Row]) -> Result<Prepared> {
    let empty = Report::default();
    let report = |row: &Row| -> Report {
        match row {
            Row::Report(report) => (*report).clone(),
            Row::Failed(..) => empty.clone(),
        }
    };
    let reports: Vec<Report> = rows.iter().map(report).collect();
    let text = |name: &str| Field::new(name, DataType::Utf8, true);
    let schema = Arc::new(Schema::new(vec![
        text("table"),
        text("source"),
        text("partition"),
        Field::new("created", DataType::Boolean, false),
        Field::new("check", DataType::Boolean, false),
        Field::new("registered", DataType::UInt64, false),
        Field::new("already", DataType::UInt64, false),
        Field::new("skipped_hidden", DataType::UInt64, false),
        Field::new("rows", DataType::Int64, false),
        Field::new("bytes", DataType::UInt64, false),
        text("commits"),
        text("adopted"),
        text("refused"),
    ]));
    let strings = |values: Vec<Option<String>>| -> ArrayRef { Arc::new(StringArray::from(values)) };
    let counts = |f: fn(&Report) -> usize| -> ArrayRef {
        Arc::new(UInt64Array::from(
            reports.iter().map(|r| f(r) as u64).collect::<Vec<_>>(),
        ))
    };
    let flags = |f: fn(&Report) -> bool| -> ArrayRef {
        Arc::new(BooleanArray::from(reports.iter().map(f).collect::<Vec<_>>()))
    };
    let tables = rows
        .iter()
        .map(|row| match row {
            Row::Report(report) => Some(report.table.clone()),
            Row::Failed(name, _) => Some(name.to_string()),
        })
        .collect();
    let refused = rows
        .iter()
        .map(|row| match row {
            Row::Report(report) => (!report.refused.is_empty()).then(|| refusals(report)),
            Row::Failed(_, why) => Some(format!("not registered: {why}")),
        })
        .collect();
    let columns: Vec<ArrayRef> = vec![
        strings(tables),
        strings(
            reports
                .iter()
                .map(|r| Some(r.source.clone()).filter(|s| !s.is_empty()))
                .collect(),
        ),
        strings(
            reports
                .iter()
                .map(|r| {
                    let partition: Vec<String> = r.partition.iter().map(|(c, t)| format!("{c} {t}")).collect();
                    Some(partition.join(", "))
                })
                .collect(),
        ),
        flags(|r| r.created),
        flags(|r| r.check),
        counts(|r| r.registered),
        counts(|r| r.already),
        counts(|r| r.skipped_hidden),
        Arc::new(Int64Array::from(reports.iter().map(|r| r.rows).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(reports.iter().map(|r| r.bytes).collect::<Vec<_>>())),
        strings(
            reports
                .iter()
                .map(|r| {
                    let commits: Vec<String> = r.commits.iter().map(i64::to_string).collect();
                    Some(commits.join(", "))
                })
                .collect(),
        ),
        strings(reports.iter().map(|r| r.adopted.clone()).collect()),
        strings(refused),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Prepared::from_batches(schema, vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verb(text: &str) -> RegisterRequest {
        match register_verb_of(text).unwrap().unwrap() {
            RegisterVerb::Table(verb) => verb,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn register_table_read() {
        let first = verb("register table tienda.ventas from '/user/hive/warehouse/tienda.db/ventas';");
        assert_eq!(first.table, "tienda.ventas");
        assert_eq!(first.source.as_deref(), Some("/user/hive/warehouse/tienda.db/ventas"));
        assert!(first.partition.is_empty() && !first.adopt && !first.check && first.on.is_none());

        let again = verb("REGISTER TABLE \"Tienda\".ventas");
        assert_eq!((again.table.as_str(), again.source), ("Tienda.ventas", None));

        let all = verb(
            "REGISTER TABLE t FROM 'raw/p' PARTITIONED BY (year INT, month integer, dt DATE, c string, big bigint) \
             LOCATION 's3://b/lake/t' ADOPT CHECK",
        );
        assert_eq!(
            all.partition,
            [
                ("year", "int"),
                ("month", "int"),
                ("dt", "date"),
                ("c", "string"),
                ("big", "long")
            ]
            .map(|(c, t)| (c.to_string(), t.to_string()))
        );
        assert_eq!(all.location.as_deref(), Some("s3://b/lake/t"));
        assert!(all.adopt && all.check);

        // Clauses in any order.
        let any = verb("register table t check from 'x'");
        assert!(any.check && any.source.as_deref() == Some("x"));
    }

    #[test]
    fn register_schema_read() {
        let Some(RegisterVerb::Schema(verb)) =
            register_verb_of("register schema curated from '/user/hive/warehouse/curated.db' adopt check").unwrap()
        else {
            panic!("a schema");
        };
        assert_eq!(
            (verb.schema.as_str(), verb.source.as_str(), verb.adopt, verb.check),
            ("curated", "/user/hive/warehouse/curated.db", true, true)
        );
        for text in [
            "register schema curated",
            "register schema curated from '/x' partitioned by (dt date)",
            "register schema curated from '/x' location '/y'",
            "register schema",
        ] {
            assert!(register_verb_of(text).is_err(), "{text}");
        }
    }

    #[test]
    fn register_table_refused_as_written() {
        for text in [
            "register t from 'x'",
            "register table",
            "register table t from x",
            "register table t partitioned (dt date)",
            "register table t partitioned by (dt timestamp)",
            "register table t partitioned by (dt date",
            "register table t check check",
            "register table t now",
        ] {
            assert!(register_verb_of(text).is_err(), "{text}");
        }
    }

    #[test]
    fn other_statements_are_not_registering() {
        for text in ["select 1", "create table register (a int)", "show tables"] {
            assert!(register_verb_of(text).unwrap().is_none(), "{text}");
        }
    }
}
