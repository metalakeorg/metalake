//! Registering files in place in SQL: the verb the command line and Python have, `register`, read by the client before
//! DataFusion, as a catalog's verbs are ([`crate::catalogs_sql`]). It runs on the branch the session is on.
//!
//! ```sql
//! REGISTER TABLE tienda.ventas FROM '/user/hive/warehouse/tienda.db/ventas';   -- created; dt found in its directories
//! REGISTER TABLE tienda.ventas;                                               -- again: what is new, from there
//! REGISTER TABLE t FROM 'raw/pedidos' PARTITIONED BY (year INT, month INT) LOCATION 's3://b/lake/t' ADOPT CHECK;
//! ```
//!
//! `FROM` is a path in the lake's store or a URL of it; left out, the table registers again from where it was.
//! `PARTITIONED BY` corrects the partition found in the directories; `CHECK` commits nothing. The answer is the
//! report, one row; a run with refused files commits nothing and fails, saying each.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::tokenizer::Token;
use metalake::register::Report;

use crate::words::Words;
use crate::{Error, Lake, Prepared, RegisterRequest, Result};

const GRAMMAR: &str =
    "REGISTER TABLE name [FROM 'path'] [PARTITIONED BY (column TYPE, …)] [LOCATION 'url'] [ADOPT] [CHECK]";

/// `REGISTER TABLE`, as a statement says it: the request but its branch, which is the session's.
pub type RegisterVerb = RegisterRequest;

/// The registering a text says, if it says one. `REGISTER TABLE` starts no other statement, so a text that starts so is
/// this or is refused.
pub fn register_verb_of(text: &str) -> Result<Option<RegisterVerb>> {
    let Some(mut words) = Words::of(text) else {
        return Ok(None);
    };
    if words.keyword().as_deref() != Some("REGISTER") {
        return Ok(None);
    }
    if !words.take("TABLE") {
        return Err(Error::usage(GRAMMAR));
    }
    let mut verb = RegisterVerb {
        table: words.dotted("the table")?,
        ..RegisterVerb::default()
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
    Ok(Some(verb))
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

/// Registers on `on`, the session's branch, and answers the report as one row; refused files fail it, saying each.
pub async fn run(lake: &Lake, mut verb: RegisterVerb, on: &str) -> Result<Prepared> {
    verb.on = Some(on.to_string());
    let report = lake.register(&verb).await?;
    if !report.check && !report.refused.is_empty() {
        return Err(Error::usage(format!(
            "{} files refused, and nothing registered:\n{}",
            report.refused.len(),
            refusals(&report)
        )));
    }
    row(&report)
}

fn refusals(report: &Report) -> String {
    let lines: Vec<String> = report
        .refused
        .iter()
        .map(|r| format!("{} ({}): {}", r.path, r.rule, r.why))
        .collect();
    lines.join("\n")
}

/// The report as a row.
fn row(report: &Report) -> Result<Prepared> {
    let partition: Vec<String> = report.partition.iter().map(|(c, t)| format!("{c} {t}")).collect();
    let commits: Vec<String> = report.commits.iter().map(i64::to_string).collect();
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
    let one = |value: Option<String>| -> ArrayRef { Arc::new(StringArray::from(vec![value])) };
    let count = |value: usize| -> ArrayRef { Arc::new(UInt64Array::from(vec![value as u64])) };
    let columns: Vec<ArrayRef> = vec![
        one(Some(report.table.clone())),
        one(Some(report.source.clone())),
        one(Some(partition.join(", "))),
        Arc::new(BooleanArray::from(vec![report.created])),
        Arc::new(BooleanArray::from(vec![report.check])),
        count(report.registered),
        count(report.already),
        count(report.skipped_hidden),
        Arc::new(Int64Array::from(vec![report.rows])),
        Arc::new(UInt64Array::from(vec![report.bytes])),
        one(Some(commits.join(", "))),
        one(report.adopted.clone()),
        one((!report.refused.is_empty()).then(|| refusals(report))),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Prepared::from_batches(schema, vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verb(text: &str) -> RegisterVerb {
        register_verb_of(text).unwrap().unwrap()
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
