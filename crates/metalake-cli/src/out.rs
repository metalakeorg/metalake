//! The contract of stdout: it holds what was asked for and nothing else, in the format asked for.

use std::fmt::Display;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::sync::Arc;

use clap::ValueEnum;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{LineDelimited, WriterBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches_with_schema;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::{TryStreamExt, stream};
use serde::Serialize;
use serde_json::Value;

use metalake_client::Error;

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
    pub fn value<T: Serialize + Display>(&self, value: &T) -> Result<(), Error> {
        let mut text = match self.format {
            Format::Json => serde_json::to_string(value).map_err(|e| Error::failed(e.to_string()))?,
            Format::Table | Format::Csv => value.to_string(),
        };
        text.push('\n');
        write(text.as_bytes())
    }

    /// A list of what a verb answers, under `columns`: as its own values in `json`, and otherwise through what prints a
    /// statement's rows, so that there is one way to print a table.
    pub async fn records<T: Serialize>(&self, columns: &[&str], rows: &[T]) -> Result<(), Error> {
        let to_value = |row| serde_json::to_value(row).map_err(|e| Error::failed(e.to_string()));
        let values = rows.iter().map(to_value).collect::<Result<Vec<Value>, Error>>()?;
        if self.format == Format::Json {
            let lines: Vec<String> = values.iter().map(|v| format!("{v}\n")).collect();
            return write(lines.concat().as_bytes());
        }
        let fields: Vec<Field> = columns.iter().map(|c| Field::new(*c, DataType::Utf8, true)).collect();
        let schema = Arc::new(Schema::new(fields));
        let cells = |column: &&str| values.iter().map(|v| cell(&v[*column])).collect::<StringArray>();
        let arrays: Vec<ArrayRef> = columns.iter().map(|c| Arc::new(cells(c)) as ArrayRef).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        let batches = stream::iter([Ok(batch)]);
        self.rows(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
            .await
    }

    /// What a statement returns, as it arrives: only a table, which sizes its columns by every row, holds it all. A
    /// statement that returns no columns prints nothing.
    pub async fn rows(&self, stream: SendableRecordBatchStream) -> Result<(), Error> {
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

/// A cell of a record, in text: a null stays one.
fn cell(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// Writes every batch of `stream`.
async fn each<W>(
    mut stream: SendableRecordBatchStream,
    mut writer: W,
    write: impl Fn(&mut W, &RecordBatch) -> Result<(), ArrowError>,
) -> Result<(), Error> {
    while let Some(batch) = stream.try_next().await? {
        ignoring_a_gone_reader(write(&mut writer, &batch))?;
    }
    Ok(())
}

fn ignoring_a_gone_reader(done: Result<(), ArrowError>) -> Result<(), Error> {
    match done {
        Err(ArrowError::IoError(_, e)) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        done => Ok(done?),
    }
}

/// Writes to stdout. A reader that went away, as `head` does, is not a failure.
fn write(bytes: &[u8]) -> Result<(), Error> {
    let mut stdout = io::stdout().lock();
    match stdout.write_all(bytes).and_then(|()| stdout.flush()) {
        Err(e) if e.kind() != io::ErrorKind::BrokenPipe => Err(e.into()),
        _ => Ok(()),
    }
}
