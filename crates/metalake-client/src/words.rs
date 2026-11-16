//! The words of a statement MetaLake reads before DataFusion does: a pipeline's verbs, a ref's (I5). A cursor over the
//! tokens of one statement, past whitespace and comments, which each grammar reads its own way.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

use crate::{Error, Result};

/// The tokens of a statement, past whitespace and comments.
pub(crate) struct Words {
    tokens: Vec<Token>,
    at: usize,
}

impl Words {
    /// The words of `text`, or none if the tokenizer cannot read it: DataFusion then says what is wrong with it.
    pub(crate) fn of(text: &str) -> Option<Self> {
        let tokens = Tokenizer::new(&GenericDialect {}, text).tokenize().ok()?;
        Some(Self { tokens, at: 0 })
    }

    fn skip(&mut self) {
        while let Some(Token::Whitespace(_)) = self.tokens.get(self.at) {
            self.at += 1;
        }
    }

    /// The next token, past whitespace and comments.
    pub(crate) fn next(&mut self) -> Option<Token> {
        self.skip();
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    /// The next word, if it is a bare one, in capitals, without taking it.
    pub(crate) fn peek_keyword(&mut self) -> Option<String> {
        self.skip();
        match self.tokens.get(self.at) {
            Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// The next word, if it is a bare one, in capitals.
    pub(crate) fn keyword(&mut self) -> Option<String> {
        match self.next() {
            Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// Takes the next word if it is `keyword`, and says whether it was.
    pub(crate) fn take(&mut self, keyword: &str) -> bool {
        let found = self.peek_keyword().as_deref() == Some(keyword);
        if found {
            self.keyword();
        }
        found
    }

    /// A name: unquoted, it is lowercase as SQL folds it; quoted, as written. `what` says whose, when it is missing.
    pub(crate) fn name(&mut self, what: &str) -> Result<String> {
        match self.next() {
            Some(Token::Word(w)) if w.quote_style.is_none() => Ok(w.value.to_ascii_lowercase()),
            Some(Token::Word(w)) => Ok(w.value),
            _ => Err(Error::usage(format!("{what}'s name is missing"))),
        }
    }

    /// What follows, as written, with no `;` at its end.
    pub(crate) fn rest(&mut self) -> String {
        let text: String = self.tokens[self.at.min(self.tokens.len())..]
            .iter()
            .map(ToString::to_string)
            .collect();
        text.trim().trim_end_matches(';').trim().to_string()
    }

    /// Nothing but a `;` and whitespace is left.
    pub(crate) fn end(&mut self) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_past_whitespace_and_comments() {
        let mut words = Words::of("  create -- a comment\n Branch \"My Ref\" from dev; ").unwrap();
        assert_eq!(words.keyword().as_deref(), Some("CREATE"));
        assert!(words.take("BRANCH"));
        assert!(!words.take("TAG"));
        assert_eq!(words.name("a ref").unwrap(), "My Ref", "quoted, as written");
        assert_eq!(words.peek_keyword().as_deref(), Some("FROM"));
        assert!(words.take("FROM"));
        assert_eq!(words.name("a ref").unwrap(), "dev", "bare, folded");
        assert!(words.end().is_ok());
    }

    #[test]
    fn a_missing_name_says_whose() {
        let mut words = Words::of("drop branch").unwrap();
        words.keyword();
        words.keyword();
        assert_eq!(
            words.name("a branch").unwrap_err().message,
            "a branch's name is missing"
        );
    }

    #[test]
    fn what_is_left_is_refused_at_the_end() {
        let mut words = Words::of("show branches now").unwrap();
        words.keyword();
        words.keyword();
        assert!(words.end().is_err());
    }
}
