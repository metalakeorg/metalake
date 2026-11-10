//! `metalake ingest`: rows from stdin into a table as they come, through a producer's writer, which commits on its
//! cadence. Lines are read in batches, by the table's columns: one JSON object per line, or CSV under a header. With a
//! key, a batch's version is its last line's number, or the highest value of a version column, so that a run given the
//! same input again skips what the table already has, row by row, and goes on after it. A quiet input does not hold rows
//! back: what the writer holds is committed each `commit_every`. Ctrl-C and the end of the input commit what it holds,
//! since every batch is whole.

use std::io::{self, BufRead, Cursor};
use std::sync::Arc;
use std::thread;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::json;
use datafusion::arrow::record_batch::RecordBatch;
use metalake_client::{Error, Lake, Mode, TablePath, WriterOptions};
use tokio::sync::mpsc;

use crate::args::{Ingest, Input};

/// What the reader of stdin hands over.
enum Read {
    /// A CSV's first line.
    Header(String),
    /// Lines, the first of them numbered `first` from 1.
    Lines { first: u64, lines: Vec<String> },
}

pub async fn run(args: &Ingest, lake: Arc<Lake>) -> Result<(), Error> {
    let path = TablePath::parse(&args.table)?;
    let (_, table) = lake.table(&path).await?;
    let columns = table.schema().clone();
    let options = WriterOptions {
        app_id: args.app_id.clone(),
        mode: match args.upsert_on.is_empty() {
            true => Mode::Append,
            false => Mode::Upsert(args.upsert_on.clone()),
        },
        commit_every: args.commit_every,
        max_bytes: args.max_bytes,
        author: None,
        message: None,
    };
    let mut writer = lake.writer(&path, options).await?;
    // What the key reached is in the table: rows at or below it are skipped, wherever a batch starts.
    let reached = writer.version();
    let keyed = args.app_id.is_some();

    let (sender, mut reads) = mpsc::channel::<io::Result<Read>>(2);
    let (input, lines) = (args.input, args.batch_lines.max(1));
    thread::spawn(move || read_stdin(input, lines, sender));
    let mut header: Option<SchemaRef> = None;
    let mut ticker = tokio::time::interval(args.commit_every.max(std::time::Duration::from_millis(10)));
    ticker.tick().await;
    loop {
        let read = tokio::select! {
            read = reads.recv() => read,
            _ = ticker.tick() => {
                writer.flush().await?;
                continue;
            }
            _ = tokio::signal::ctrl_c() => break,
        };
        let Some(read) = read else { break };
        match read? {
            Read::Header(line) => header = Some(header_schema(&line, &columns)?),
            Read::Lines { first, lines } => {
                let numbered: Vec<(u64, String)> = (first..).zip(lines).collect();
                // By line: what the key passed is dropped before it is read.
                let numbered: Vec<(u64, String)> = match (keyed, &args.version_column, reached) {
                    (true, None, Some(reached)) => numbered.into_iter().filter(|(n, _)| *n as i64 > reached).collect(),
                    _ => numbered,
                };
                let Some(last) = numbered.last().map(|(n, _)| *n as i64) else {
                    continue;
                };
                let text: Vec<&str> = numbered.iter().map(|(_, line)| line.as_str()).collect();
                let batch = match args.input {
                    Input::Json => parse_json(&text, &columns)?,
                    Input::Csv => parse_csv(
                        &text,
                        header.as_ref().ok_or_else(|| Error::usage("a CSV needs its header"))?,
                    )?,
                };
                let (batch, version) = match (keyed, &args.version_column) {
                    (false, _) => (batch, None),
                    (true, None) => (batch, Some(last)),
                    (true, Some(column)) => match versioned(batch, column, reached)? {
                        Some((batch, version)) => (batch, Some(version)),
                        None => continue,
                    },
                };
                writer.write(batch, version).await?;
            }
        }
    }
    writer.flush().await?;
    if let Some(version) = writer.version() {
        eprintln!("version {version}");
    }
    writer.close().await?;
    Ok(())
}

/// Reads stdin's lines in batches of `lines`, on a thread of its own, since stdin blocks. Empty lines count, and are
/// left out.
fn read_stdin(input: Input, lines: usize, sender: mpsc::Sender<io::Result<Read>>) {
    let stdin = io::stdin().lock();
    let mut batch = Vec::with_capacity(lines);
    let mut first = 0;
    let mut header = input == Input::Csv;
    for (n, line) in (1u64..).zip(stdin.lines()) {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                let _ = sender.blocking_send(Err(e));
                return;
            }
        };
        if header {
            header = false;
            if sender.blocking_send(Ok(Read::Header(line))).is_err() {
                return;
            }
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        if batch.is_empty() {
            first = n;
        }
        batch.push(line);
        if batch.len() == lines {
            let lines = std::mem::replace(&mut batch, Vec::with_capacity(lines));
            if sender.blocking_send(Ok(Read::Lines { first, lines })).is_err() {
                return;
            }
        }
    }
    if !batch.is_empty() {
        let _ = sender.blocking_send(Ok(Read::Lines { first, lines: batch }));
    }
}

/// The columns a CSV's header names, in its order, each with the table's type.
fn header_schema(line: &str, columns: &SchemaRef) -> Result<SchemaRef, Error> {
    let fields = line
        .split(',')
        .map(|name| {
            let name = name.trim().trim_matches('"');
            columns
                .field_with_name(name)
                .cloned()
                .map_err(|_| Error::usage(format!("{name} is not a column of the table")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SchemaRef::new(Schema::new(fields)))
}

fn parse_json(lines: &[&str], columns: &SchemaRef) -> Result<RecordBatch, Error> {
    let reader = json::ReaderBuilder::new(columns.clone())
        .with_batch_size(lines.len())
        .build(Cursor::new(lines.join("\n")))?;
    one(reader.collect::<Result<Vec<_>, _>>()?, columns)
}

fn parse_csv(lines: &[&str], header: &SchemaRef) -> Result<RecordBatch, Error> {
    let reader = csv::ReaderBuilder::new(header.clone())
        .with_header(false)
        .with_batch_size(lines.len())
        .build(Cursor::new(lines.join("\n")))?;
    one(reader.collect::<Result<Vec<_>, _>>()?, header)
}

fn one(batches: Vec<RecordBatch>, schema: &SchemaRef) -> Result<RecordBatch, Error> {
    Ok(datafusion::arrow::compute::concat_batches(schema, &batches)?)
}

/// The rows past what the key reached, by the version column, and the highest version among them.
fn versioned(batch: RecordBatch, column: &str, reached: Option<i64>) -> Result<Option<(RecordBatch, i64)>, Error> {
    let values = batch
        .column_by_name(column)
        .ok_or_else(|| Error::usage(format!("{column} is not a column of the rows")))?;
    let values = cast(values, &DataType::Int64)?;
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::usage(format!("{column} is a version: a whole number")))?;
    if values.null_count() > 0 {
        return Err(Error::usage(format!("{column} is a version, and has rows without one")));
    }
    let past: BooleanArray = values.iter().map(|v| Some(v > reached)).collect();
    let batch = filter_record_batch(&batch, &past)?;
    let highest = values.iter().flatten().filter(|v| Some(*v) > reached).max();
    Ok(highest.map(|version| (batch, version)))
}
