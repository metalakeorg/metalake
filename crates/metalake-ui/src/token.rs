//! The run's token (ADR 0007 §6.3): the one defense against another process of the machine, which can forge every
//! header but cannot read what the user's browser and terminal hold. A new one each run of `metalake ui`; none is kept
//! in a file.

use std::fmt;

/// The variable that gives the run its token, for a test, a browser driven by one, or Vite's dev server; a new one is
/// generated without it. Like the Iceberg service's, it is read from the environment, which only its user reads.
pub const ENVIRONMENT: &str = "METALAKE_UI_TOKEN";

/// The fewest characters a token given from outside may have.
pub const MIN_LENGTH: usize = 32;

/// The run's secret: 32 bytes of the system's random, as 64 hex characters, or one given. Its `Debug` never shows it.
#[derive(Clone)]
pub struct Token(String);

/// A token given that is too short, or holds what a header or a URL's fragment cannot carry as it is.
#[derive(Debug)]
pub struct Refused(pub String);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

impl Token {
    /// A new token, from the system's source of randomness.
    pub fn generate() -> std::io::Result<Self> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut text = String::with_capacity(64);
        for byte in bytes {
            text.push_str(&format!("{byte:02x}"));
        }
        Ok(Token(text))
    }

    /// A token given, as `METALAKE_UI_TOKEN` gives one to a test or to Vite's dev server: at least [`MIN_LENGTH`]
    /// characters, each a letter, a digit, `-` or `_`.
    pub fn given(text: String) -> Result<Self, Refused> {
        if text.len() < MIN_LENGTH {
            return Err(Refused(format!("a UI token has at least {MIN_LENGTH} characters")));
        }
        if !text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(Refused("a UI token holds letters, digits, `-` and `_` alone".into()));
        }
        Ok(Token(text))
    }

    /// The token [`ENVIRONMENT`] gives, or a new one.
    pub fn from_environment() -> std::io::Result<Self> {
        match std::env::var(ENVIRONMENT) {
            Ok(text) if !text.is_empty() => {
                Token::given(text).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            }
            _ => Token::generate(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `given` is this token, in a time that depends on its length alone.
    pub(crate) fn matches(&self, given: &str) -> bool {
        let (a, b) = (self.0.as_bytes(), given.as_bytes());
        a.len() == b.len() && a.iter().zip(b).fold(0u8, |differ, (x, y)| differ | (x ^ y)) == 0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(…)")
    }
}
