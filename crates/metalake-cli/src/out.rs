//! The contract of stdout: it holds what was asked for and nothing else, in the format asked for.

use std::fmt::Display;
use std::io::{self, BufWriter, IsTerminal, Write};

use clap::ValueEnum;
use datafusion::arrow::csv;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{LineDelimited, WriterBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches_with_schema;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::TryStreamExt;
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

    /// What a statement returns, as it arrives: only a table, which sizes its columns by every row, holds it all. A
    /// statement that returns no columns prints nothing.
    pub async fn rows(&self, stream: SendableRecordBatchStream) -> Result<(), Failure> {
        let schema = stream.schema();
        if schema.fields().is_empty() {
            let _: Vec<RecordBatch> = stream.try_collect().await?;
            return Ok(());
        }
        match self.format {
            Format::Table => {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                let text = pretty_format_batches_with_schema(schema, &batches)?;
                write(format!("{text}\n").as_bytes())
            }
            Format::Csv => {
                let mut writer = csv::WriterBuilder::new()
                    .with_header(true)
                    .build(BufWriter::new(io::stdout()));
                // The header, which a writer prints with its first batch, for a result with none.
                ignoring_a_gone_reader(writer.write(&RecordBatch::new_empty(schema)))?;
                each(stream, writer, csv::Writer::write).await
            }
            Format::Json => {
                let writer = WriterBuilder::new().with_explicit_nulls(true);
                let mut writer = writer.build::<_, LineDelimited>(BufWriter::new(io::stdout()));
                let done = each(stream, &mut writer, |w, batch| w.write(batch)).await;
                done.and(ignoring_a_gone_reader(writer.finish()))
            }
        }
    }
}

/// Writes every batch of `stream`.
async fn each<W>(
    mut stream: SendableRecordBatchStream,
    mut writer: W,
    write: impl Fn(&mut W, &RecordBatch) -> Result<(), ArrowError>,
) -> Result<(), Failure> {
    while let Some(batch) = stream.try_next().await? {
        ignoring_a_gone_reader(write(&mut writer, &batch))?;
    }
    Ok(())
}

fn ignoring_a_gone_reader(done: Result<(), ArrowError>) -> Result<(), Failure> {
    match done {
        Err(ArrowError::IoError(_, e)) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        done => Ok(done?),
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
