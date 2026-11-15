//! `USING`: statements sent to an engine other than the lake's own, read by the session before DataFusion, which knows
//! nothing of them, as a pipeline's verbs are. Two forms:
//!
//! ```sql
//! USING duckdb SELECT count(*) FROM demo.orders;   -- this statement alone, on DuckDB
//!
//! USING duckdb;                                    -- the statements after it, until another USING, on DuckDB
//! INSERT INTO lake.demo.orders VALUES (1);
//! SELECT count(*) FROM demo.orders;
//! USING metalake;                                  -- back to the lake's own engine
//! DELETE FROM demo.orders WHERE id = 1;
//! ```
//!
//! An engine is chosen before the statements it runs, as `USE` chooses a catalog, and for one run of a text alone: the
//! next starts on the lake's own engine. A statement for an engine is in its own SQL, and goes to it as written. No
//! statement of SQL starts with `USING`, so a text that does is this and nothing else. A pipeline's verbs stay the
//! client's whatever engine is chosen: they are MetaLake's, not SQL.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use super::check_engine_name;
use crate::{Error, Result};

/// The lake's own engine, by the name `USING` gives it: `USING metalake` goes back to it.
pub const OWN: &str = "metalake";

/// A statement for an engine other than the lake's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Using {
    /// The engine, as the statement names it: lowercase, as SQL folds a bare name.
    pub engine: String,
    /// The engine's statement, as written.
    pub sql: String,
}

/// What a text that starts `USING` says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsingOf {
    /// `USING <engine> <statement>`: this statement on that engine. On `metalake`, the lake's own.
    Statement { engine: String, sql: String },
    /// `USING <engine>` alone: the statements after it on that engine.
    Switch(String),
}

/// The `USING` a text says, if it says one: none for every other statement. One that starts `USING` and names no
/// engine by a name an engine can take is refused.
pub fn using_of(text: &str) -> Result<Option<UsingOf>> {
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
    let (named, engine) = match at.next() {
        Some((i, Token::Word(w))) if w.quote_style.is_none() => (i, w.value.to_ascii_lowercase()),
        _ => {
            return Err(Error::usage(
                "USING <engine> [statement]: as `USING duckdb SELECT 1`, or `USING duckdb` for the statements after it",
            ));
        }
    };
    // The lake's own and the local engines are named as they are; any other, as a named engine may be.
    if engine != OWN && !super::LOCAL.contains(&engine.as_str()) {
        check_engine_name(&engine)?;
    }
    let rest = &tokens[named + 1..];
    // Whitespace and comments alone are no statement: what is left chooses the engine for what follows.
    if rest.iter().all(|t| matches!(t, Token::Whitespace(_))) {
        return Ok(Some(UsingOf::Switch(engine)));
    }
    let sql: String = rest.iter().map(ToString::to_string).collect();
    Ok(Some(UsingOf::Statement {
        engine,
        sql: sql.trim().to_string(),
    }))
}

/// Where a piece of a text runs, the engine chosen so far given: on the lake's own engine (`None`, the text as it is),
/// on another (`Some(Using)`), or nowhere, a `USING` alone, which only chooses the engine for what follows.
pub(crate) enum Placed {
    Own(String),
    Other(Using),
    Chosen,
}

/// Places a piece of a text, and keeps the engine `chosen` for the pieces after it.
pub(crate) fn place(piece: &str, chosen: &mut Option<String>) -> Result<Placed> {
    match using_of(piece)? {
        Some(UsingOf::Switch(engine)) => {
            *chosen = (engine != OWN).then_some(engine);
            Ok(Placed::Chosen)
        }
        Some(UsingOf::Statement { engine, sql }) if engine == OWN => Ok(Placed::Own(sql)),
        Some(UsingOf::Statement { engine, sql }) => Ok(Placed::Other(Using { engine, sql })),
        // A pipeline's verb stays the client's, whatever engine is chosen.
        None => Ok(match chosen {
            Some(engine) if crate::pipeline::sql::verb_of(piece)?.is_none() => Placed::Other(Using {
                engine: engine.clone(),
                sql: piece.trim().to_string(),
            }),
            _ => Placed::Own(piece.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Session, Step};

    fn statement(engine: &str, sql: &str) -> Option<UsingOf> {
        Some(UsingOf::Statement {
            engine: engine.into(),
            sql: sql.into(),
        })
    }

    #[test]
    fn using_names_an_engine_and_keeps_its_statement_as_written() {
        assert_eq!(
            using_of("USING duckdb SELECT region, count(*) FROM demo.orders GROUP BY 1").unwrap(),
            statement("duckdb", "SELECT region, count(*) FROM demo.orders GROUP BY 1")
        );
        assert_eq!(
            using_of("  -- a sample\n using DuckDB\n  select 'a;b' as s ").unwrap(),
            statement("duckdb", "select 'a;b' as s"),
            "a comment before, a keyword in any case, a name folded, lines kept"
        );
    }

    #[test]
    fn using_alone_chooses_the_engine_for_what_follows() {
        assert_eq!(
            using_of("using duckdb").unwrap(),
            Some(UsingOf::Switch("duckdb".into()))
        );
        assert_eq!(
            using_of(" USING DuckDB  -- for the rest").unwrap(),
            Some(UsingOf::Switch("duckdb".into()))
        );
        assert_eq!(
            using_of("using metalake").unwrap(),
            Some(UsingOf::Switch("metalake".into()))
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
    fn using_with_no_engine_is_refused() {
        for text in [
            "using",
            "using 'duckdb' select 1",
            "using 2x select 1",
            "using -- nothing",
        ] {
            assert!(using_of(text).is_err(), "{text}");
        }
    }

    /// Which engine runs each step of a text: none named, the lake's; a statement's own `USING`, that one; after a
    /// `USING` alone, that one, until another; a pipeline's verbs, always the client's.
    fn engines(text: &str) -> Vec<String> {
        Session::steps(text)
            .unwrap()
            .iter()
            .map(|step| match step {
                Step::Statement(_) => "metalake".to_string(),
                Step::Pipeline(_) => "pipeline".to_string(),
                Step::Using(using) => using.engine.clone(),
            })
            .collect()
    }

    #[test]
    fn each_statement_runs_where_the_text_chose_before_it() {
        assert_eq!(engines("insert into t values (1); select 1"), ["metalake", "metalake"]);
        assert_eq!(
            engines("insert into t values (1); using duckdb select 1; delete from t"),
            ["metalake", "duckdb", "metalake"],
            "a statement's own USING is that statement's alone"
        );
        assert_eq!(
            engines(
                "using duckdb; insert into lake.t values (1); insert into lake.t values (2); select 1; \
                 using metalake; delete from t where a = 1"
            ),
            ["duckdb", "duckdb", "duckdb", "metalake"],
            "a USING alone chooses for what follows, until another; it runs nothing"
        );
        assert_eq!(
            engines("using duckdb; using metalake select 1; select 2; show pipelines"),
            ["metalake", "duckdb", "pipeline"],
            "the lake's own by name, for one statement; a pipeline's verbs stay the client's"
        );
    }

    #[test]
    fn a_statement_for_an_engine_is_kept_as_written_and_not_parsed() {
        let steps = Session::steps("using duckdb; select * from read_parquet('x.parquet') limit 2").unwrap();
        let Step::Using(using) = &steps[0] else {
            panic!("{:?}", steps[0])
        };
        assert_eq!(using.sql, "select * from read_parquet('x.parquet') limit 2");
    }
}
