//! The contract of stdout: it holds what was asked for and nothing else, in the format asked for.

use std::fmt::Display;
use std::io::{self, IsTerminal, Write};

use clap::ValueEnum;
use serde::Serialize;

use crate::failure::Failure;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Table,
    Csv,
    Json,
}

/// Where a verb prints.
#[derive(Debug)]
pub struct Out {
    format: Format,
}

impl Out {
    /// The format asked for; failing that a table for a person, and for a pipe what a program reads.
    pub fn new(format: Option<Format>) -> Self {
        let terminal = io::stdout().is_terminal();
        Self {
            format: format.unwrap_or(if terminal { Format::Table } else { Format::Csv }),
        }
    }

    /// What a verb answers: one object per line in `json`, its text otherwise.
    pub fn value<T: Serialize + Display>(&self, value: &T) -> Result<(), Failure> {
        let mut text = match self.format {
            Format::Json => serde_json::to_string(value).map_err(|e| Failure::failed(e.to_string()))?,
            Format::Table | Format::Csv => value.to_string(),
        };
        text.push('\n');
        write(text.as_bytes())
    }
}

/// Writes to stdout. A reader that went away, as `head` does, is not a failure.
fn write(bytes: &[u8]) -> Result<(), Failure> {
    let mut stdout = io::stdout().lock();
    match stdout.write_all(bytes).and_then(|()| stdout.flush()) {
        Err(e) if e.kind() != io::ErrorKind::BrokenPipe => Err(e.into()),
        _ => Ok(()),
    }
}
