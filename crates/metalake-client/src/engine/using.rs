//! `USING <engine> <statement>`: one statement sent to an engine other than the lake's own, read by the session before
//! DataFusion, which knows nothing of it, as a pipeline's verbs are.
//!
//! ```sql
//! USING duckdb SELECT region, count(*) FROM demo.orders GROUP BY 1;
//! ```
//!
//! The statement is the engine's, in its own SQL, and goes to it as written. No statement of SQL starts with `USING`,
//! so a text that does is this and nothing else.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use super::check_engine_name;
use crate::{Error, Result};

/// A statement for an engine other than the lake's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Using {
    /// The engine, as the statement names it: lowercase, as SQL folds a bare name.
    pub engine: String,
    /// The engine's statement, as written.
    pub sql: String,
}

/// The `USING` a text says, if it says one: none for every other statement. One that starts `USING` and names no
/// engine, or nothing for it to run, is refused.
pub fn using_of(text: &str) -> Result<Option<Using>> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, text).tokenize() else {
        return Ok(None);
    };
    let mut at = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)));
    match at.next() {
        Some((_, Token::Word(w))) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("using") => {}
        _ => return Ok(None),
    }
    let usage = || Error::usage("USING <engine> <statement>, as `USING duckdb SELECT 1`");
    let (named, engine) = match at.next() {
        Some((i, Token::Word(w))) if w.quote_style.is_none() => (i, w.value.to_ascii_lowercase()),
        _ => return Err(usage()),
    };
    check_engine_name(&engine)?;
    let rest = &tokens[named + 1..];
    // Whitespace and comments alone are no statement.
    if rest.iter().all(|t| matches!(t, Token::Whitespace(_))) {
        return Err(usage());
    }
    let sql: String = rest.iter().map(ToString::to_string).collect();
    let sql = sql.trim().to_string();
    Ok(Some(Using { engine, sql }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn using(engine: &str, sql: &str) -> Option<Using> {
        Some(Using {
            engine: engine.into(),
            sql: sql.into(),
        })
    }

    #[test]
    fn using_names_an_engine_and_keeps_its_statement_as_written() {
        assert_eq!(
            using_of("USING duckdb SELECT region, count(*) FROM demo.orders GROUP BY 1").unwrap(),
            using("duckdb", "SELECT region, count(*) FROM demo.orders GROUP BY 1")
        );
        assert_eq!(
            using_of("  -- a sample\n using DuckDB\n  select 'a;b' as s ").unwrap(),
            using("duckdb", "select 'a;b' as s"),
            "a comment before, a keyword in any case, a name folded, lines kept"
        );
    }

    #[test]
    fn any_other_statement_is_not_using() {
        for text in [
            "select 1",
            "select * from t join u using (k)",
            "create table t (a int)",
            "show pipelines",
        ] {
            assert_eq!(using_of(text).unwrap(), None, "{text}");
        }
    }

    #[test]
    fn using_with_no_engine_or_nothing_to_run_is_refused() {
        for text in [
            "using",
            "using duckdb",
            "using duckdb   -- nothing",
            "using 'duckdb' select 1",
            "using 2x select 1",
        ] {
            assert!(using_of(text).is_err(), "{text}");
        }
    }

    /// A text mixes the three: the lake's statements, a pipeline's verbs and another engine's, each a step in order;
    /// the other engine's is not parsed, its SQL being its own.
    #[test]
    fn a_text_mixes_the_lakes_statements_and_another_engines() {
        use crate::{Session, Step};
        let steps = Session::steps(
            "create table t (a int); using duckdb select * from read_parquet('x.parquet') limit 2; show pipelines",
        )
        .unwrap();
        assert!(matches!(steps[0], Step::Statement(_)));
        let Step::Using(using) = &steps[1] else {
            panic!("{:?}", steps[1])
        };
        assert_eq!(using.sql, "select * from read_parquet('x.parquet') limit 2");
        assert!(matches!(steps[2], Step::Pipeline(_)));
        assert!(
            Session::steps("select 1; using duckdb").is_err(),
            "refused before any runs"
        );
    }
}
