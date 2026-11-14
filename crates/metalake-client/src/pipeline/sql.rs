//! A pipeline's verbs in SQL, read by the session before DataFusion, which knows nothing of them:
//!
//! ```sql
//! CREATE PIPELINE totals [WITH (parallelism = 2, empty_target = true)] AS INSERT INTO t SELECT … FROM s;
//! SHOW PIPELINES;
//! DESCRIBE PIPELINE totals;
//! ALTER PIPELINE totals SUSPEND;   -- or RESUME
//! DROP PIPELINE [IF EXISTS] totals;
//! ```
//!
//! Each is a text of its own, and answers a table as any statement does. Each is a call of the client's own, which every
//! face makes the same.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampSecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

use super::{Pipeline, PipelineOptions, PipelineState};
use crate::{Error, Lake, Prepared, Result};

/// A pipeline's verb, as a statement says it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verb {
    Create {
        name: String,
        query: String,
        options: PipelineOptions,
    },
    Show,
    Describe(String),
    Suspend(String),
    Resume(String),
    Drop {
        name: String,
        if_exists: bool,
    },
}

/// The statements of a text, each as written, split at the `;` that end them: a `;` in a string or a comment is not
/// one. A text the tokenizer cannot read is one piece, for DataFusion to say what is wrong with it.
pub fn pieces(text: &str) -> Vec<String> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, text).tokenize() else {
        return vec![text.to_string()];
    };
    let mut pieces = Vec::new();
    let mut piece = String::new();
    for token in tokens {
        match token {
            Token::SemiColon => pieces.push(std::mem::take(&mut piece)),
            token => piece.push_str(&token.to_string()),
        }
    }
    pieces.push(piece);
    // A piece of whitespace and comments alone is no statement.
    pieces.retain(|piece| {
        Tokenizer::new(&GenericDialect {}, piece)
            .tokenize()
            .map_or(true, |tokens| tokens.iter().any(|t| !matches!(t, Token::Whitespace(_))))
    });
    pieces
}

/// The pipeline verb a text says, if it says one: none for every other statement.
pub fn verb_of(text: &str) -> Result<Option<Verb>> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, text).tokenize() else {
        return Ok(None);
    };
    let mut words = Words::new(tokens);
    let (first, second) = (words.keyword(), words.keyword());
    let verb = match (first.as_deref(), second.as_deref()) {
        (Some("CREATE"), Some("PIPELINE")) => {
            let name = words.name()?;
            let mut options = PipelineOptions::default();
            if words.peek_keyword().as_deref() == Some("WITH") {
                words.keyword();
                words.options(&mut options)?;
            }
            if words.keyword().as_deref() != Some("AS") {
                return Err(Error::usage("CREATE PIPELINE name [WITH (…)] AS INSERT INTO …"));
            }
            let query = words.rest();
            Verb::Create { name, query, options }
        }
        (Some("SHOW"), Some("PIPELINES")) => Verb::Show,
        (Some("DESCRIBE"), Some("PIPELINE")) => Verb::Describe(words.name()?),
        (Some("ALTER"), Some("PIPELINE")) => {
            let name = words.name()?;
            match words.keyword().as_deref() {
                Some("SUSPEND") => Verb::Suspend(name),
                Some("RESUME") => Verb::Resume(name),
                _ => return Err(Error::usage("ALTER PIPELINE name SUSPEND, or RESUME")),
            }
        }
        (Some("DROP"), Some("PIPELINE")) => {
            let if_exists = words.peek_keyword().as_deref() == Some("IF");
            if if_exists {
                words.keyword();
                if words.keyword().as_deref() != Some("EXISTS") {
                    return Err(Error::usage("DROP PIPELINE [IF EXISTS] name"));
                }
            }
            Verb::Drop {
                name: words.name()?,
                if_exists,
            }
        }
        _ => return Ok(None),
    };
    if !matches!(verb, Verb::Create { .. }) {
        words.end()?;
    }
    Ok(Some(verb))
}

/// The tokens of a statement, past whitespace and comments.
struct Words {
    tokens: Vec<Token>,
    at: usize,
}

impl Words {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, at: 0 }
    }

    fn skip(&mut self) {
        while let Some(Token::Whitespace(_)) = self.tokens.get(self.at) {
            self.at += 1;
        }
    }

    fn next(&mut self) -> Option<Token> {
        self.skip();
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    fn peek_keyword(&mut self) -> Option<String> {
        self.skip();
        match self.tokens.get(self.at) {
            Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
            _ => None,
        }
    }

    fn keyword(&mut self) -> Option<String> {
        match self.next() {
            Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// A pipeline's name: unquoted, it is lowercase as SQL folds it; quoted, as written.
    fn name(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::Word(w)) if w.quote_style.is_none() => Ok(w.value.to_ascii_lowercase()),
            Some(Token::Word(w)) => Ok(w.value),
            _ => Err(Error::usage("a pipeline's name is missing")),
        }
    }

    /// `(name = value, …)`.
    fn options(&mut self, options: &mut PipelineOptions) -> Result<()> {
        if self.next() != Some(Token::LParen) {
            return Err(Error::usage("WITH (parallelism = 2, empty_target = true)"));
        }
        loop {
            let key = self
                .keyword()
                .ok_or_else(|| Error::usage("an option is `name = value`"))?;
            if self.next() != Some(Token::Eq) {
                return Err(Error::usage(format!("{} = value", key.to_ascii_lowercase())));
            }
            let value = match self.next() {
                Some(Token::Number(n, _)) => n,
                Some(Token::Word(w)) => w.value.to_ascii_lowercase(),
                Some(Token::SingleQuotedString(s)) => s.to_ascii_lowercase(),
                _ => return Err(Error::usage(format!("{} has no value", key.to_ascii_lowercase()))),
            };
            match key.as_str() {
                "PARALLELISM" => {
                    options.parallelism = value
                        .parse()
                        .map_err(|_| Error::usage(format!("parallelism is a whole number, not {value}")))?;
                }
                "EMPTY_TARGET" => {
                    options.empty_target = match value.as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(Error::usage(format!("empty_target is true or false, not {value}"))),
                    };
                }
                other => {
                    return Err(Error::usage(format!(
                        "a pipeline has no option {}: parallelism, empty_target",
                        other.to_ascii_lowercase()
                    )));
                }
            }
            match self.next() {
                Some(Token::Comma) => continue,
                Some(Token::RParen) => return Ok(()),
                _ => return Err(Error::usage("options are separated by `,` and closed by `)`")),
            }
        }
    }

    /// What follows, as written, with no `;` at its end.
    fn rest(&mut self) -> String {
        let text: String = self.tokens[self.at.min(self.tokens.len())..]
            .iter()
            .map(ToString::to_string)
            .collect();
        text.trim().trim_end_matches(';').trim().to_string()
    }

    /// Nothing but a `;` and whitespace is left.
    fn end(&mut self) -> Result<()> {
        loop {
            match self.next() {
                None => return Ok(()),
                Some(Token::SemiColon) => continue,
                Some(Token::Whitespace(Whitespace::Newline)) => continue,
                Some(other) => return Err(Error::usage(format!("{other} where the statement ends"))),
            }
        }
    }
}

/// Runs a verb, and answers what a statement answers: the pipelines it is about, as rows.
pub async fn run(lake: &Lake, verb: Verb) -> Result<Prepared> {
    let pipelines = match verb {
        Verb::Create { name, query, options } => vec![lake.create_pipeline(&name, &query, options).await?],
        Verb::Show => lake.pipelines().await?,
        Verb::Describe(name) => vec![lake.pipeline(&name).await?],
        Verb::Suspend(name) => vec![lake.suspend_pipeline(&name).await?],
        Verb::Resume(name) => vec![lake.resume_pipeline(&name).await?],
        Verb::Drop { name, if_exists } => {
            match lake.drop_pipeline(&name).await {
                Err(e) if if_exists && e.message.contains("no pipeline") => {}
                done => done?,
            }
            vec![]
        }
    };
    let (schema, batch) = rows(&pipelines)?;
    Prepared::from_batches(schema, vec![batch])
}

/// A state as a row says it.
pub fn state_name(state: &PipelineState) -> String {
    match state {
        PipelineState::Running => "running".into(),
        PipelineState::Suspended => "suspended".into(),
        PipelineState::Failed { .. } => "failed".into(),
        PipelineState::Failing { .. } => "failing".into(),
        PipelineState::Elsewhere { host } => format!("running on {host}"),
    }
}

/// Pipelines as rows: what `SHOW PIPELINES` and every other verb answer.
pub fn rows(pipelines: &[Pipeline]) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("reads", DataType::Utf8, false),
        Field::new("writes", DataType::Utf8, false),
        Field::new("reached", DataType::Utf8, false),
        Field::new("parallelism", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new("query", DataType::Utf8, false),
        Field::new("log", DataType::Utf8, true),
    ]));
    let joined = |f: &dyn Fn(&Pipeline) -> String| -> ArrayRef {
        Arc::new(pipelines.iter().map(f).map(Some).collect::<StringArray>())
    };
    let list = |paths: &[crate::TablePath]| paths.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
    let columns: Vec<ArrayRef> = vec![
        joined(&|p| p.name.clone()),
        joined(&|p| state_name(&p.state)),
        joined(&|p| list(&p.reads)),
        joined(&|p| list(&p.writes)),
        joined(&|p| {
            p.reached
                .iter()
                .map(|(path, last)| match last {
                    Some(commit) => format!("{path} at {commit}"),
                    None => format!("{path}: nothing yet"),
                })
                .collect::<Vec<_>>()
                .join(", ")
        }),
        Arc::new(pipelines.iter().map(|p| p.parallelism as i64).collect::<Int64Array>()),
        Arc::new(
            pipelines
                .iter()
                .map(|p| {
                    p.created_at
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |d| d.as_secs() as i64)
                })
                .collect::<Vec<i64>>()
                .into_iter()
                .map(Some)
                .collect::<TimestampSecondArray>()
                .with_timezone("UTC"),
        ),
        joined(&|p| p.query.clone()),
        Arc::new(pipelines.iter().map(|p| p.state.log()).collect::<StringArray>()),
    ];
    Ok((schema.clone(), RecordBatch::try_new(schema, columns)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verb(text: &str) -> Verb {
        verb_of(text).unwrap().expect("a pipeline verb")
    }

    #[test]
    fn verbs() {
        assert_eq!(
            verb("create pipeline Totals as insert into t select * from s;"),
            Verb::Create {
                name: "totals".into(),
                query: "insert into t select * from s".into(),
                options: PipelineOptions::default()
            }
        );
        assert_eq!(
            verb("CREATE PIPELINE t WITH (parallelism = 4, empty_target = true)\nAS INSERT INTO x SELECT 'a;b' FROM y"),
            Verb::Create {
                name: "t".into(),
                query: "INSERT INTO x SELECT 'a;b' FROM y".into(),
                options: PipelineOptions {
                    parallelism: 4,
                    empty_target: true
                }
            }
        );
        assert_eq!(verb("show pipelines"), Verb::Show);
        assert_eq!(verb("SHOW PIPELINES;"), Verb::Show);
        assert_eq!(verb("describe pipeline t"), Verb::Describe("t".into()));
        assert_eq!(verb("alter pipeline t suspend"), Verb::Suspend("t".into()));
        assert_eq!(verb("ALTER PIPELINE t RESUME;"), Verb::Resume("t".into()));
        assert_eq!(
            verb("drop pipeline if exists t"),
            Verb::Drop {
                name: "t".into(),
                if_exists: true
            }
        );
        assert_eq!(
            verb("-- a comment\n drop pipeline t"),
            Verb::Drop {
                name: "t".into(),
                if_exists: false
            }
        );
    }

    #[test]
    fn a_batch_in_pieces() {
        assert_eq!(
            pieces("select 1; show pipelines;\n-- the end\n"),
            ["select 1", " show pipelines"]
        );
        assert_eq!(
            pieces("select ';' -- a ; here\n; drop pipeline t"),
            ["select ';' -- a ; here\n", " drop pipeline t"]
        );
        assert!(pieces("  ;\n").is_empty());
        assert_eq!(
            verb_of(&pieces("select 1; show pipelines")[1]).unwrap(),
            Some(Verb::Show)
        );
    }

    #[test]
    fn other_statements_are_not_verbs() {
        for text in [
            "select 1",
            "create table pipeline (a int)",
            "show tables",
            "drop table t",
            "insert into",
        ] {
            assert_eq!(verb_of(text).unwrap(), None, "{text}");
        }
    }

    #[test]
    fn refusals() {
        for (text, says) in [
            ("create pipeline t insert into x select 1", "AS INSERT"),
            (
                "create pipeline t with (speed = 2) as insert into x select 1",
                "no option speed",
            ),
            (
                "create pipeline t with (parallelism = two) as insert into x select 1",
                "whole number",
            ),
            (
                "create pipeline t with (empty_target = maybe) as insert into x select 1",
                "true or false",
            ),
            ("alter pipeline t stop", "SUSPEND"),
            ("drop pipeline", "name is missing"),
            ("show pipelines now", "where the statement ends"),
        ] {
            let refused = verb_of(text).unwrap_err();
            assert!(refused.message.contains(says), "{text}: {}", refused.message);
        }
    }
}
