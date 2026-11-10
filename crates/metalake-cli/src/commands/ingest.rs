//! `metalake ingest`: rows from stdin into a table as they come, through a producer's writer, which commits on its
//! cadence. Lines are read by the table's columns: one JSON object per line, or CSV under a header, and given to the
//! writer a batch at a time, a full one or, each `commit_every`, what came since, so that a quiet input holds no row
//! back.
//!
//! With a key, a row's version is its line's number, or the value of a version column, which never goes back: a version
//! that does is refused, not lost. What the key already passed is skipped row by row, so that a run given the same input
//! again goes on after it. By a column, the rows of the highest version seen are held until a higher one comes or the
//! input ends, since more of them may follow: the writer only ever has whole versions, and a commit never splits one.
//! Ctrl-C and the end of the input commit what it holds.

use std::io::{self, BufRead, Cursor};
use std::sync::Arc;
use std::thread;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::compute::{cast, concat_batches, filter_record_batch};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::json;
use datafusion::arrow::record_batch::RecordBatch;
use metalake_client::{Error, Lake, Mode, TablePath, Writer, WriterOptions};
use tokio::sync::mpsc;

use crate::args::{Ingest, Input};

/// What the reader of stdin hands over.
enum Read {
    /// A CSV's first line.
    Header(String),
    /// A line, numbered from 1.
    Line(u64, String),
}

pub async fn run(args: &Ingest, lake: Arc<Lake>) -> Result<(), Error> {
    let path = TablePath::parse(&args.table)?;
    let (_, table) = lake.table(&path).await?;
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
    let writer = lake.writer(&path, options).await?;
    let mut ingestion = Ingestion {
        // What the key reached is in the table: rows at or below it are skipped.
        reached: writer.version(),
        writer,
        args,
        columns: table.schema().clone(),
        header: None,
        lines: Vec::new(),
        highest: None,
        held: None,
    };

    let (sender, mut reads) = mpsc::channel::<io::Result<Read>>(args.batch_lines.max(1));
    let input = args.input;
    thread::spawn(move || read_stdin(input, sender));
    let mut ticker = tokio::time::interval(args.commit_every.max(std::time::Duration::from_millis(10)));
    ticker.tick().await;
    loop {
        tokio::select! {
            read = reads.recv() => match read {
                None => break,
                Some(read) => ingestion.read(read?).await?,
            },
            _ = ticker.tick() => {
                ingestion.give(false).await?;
                ingestion.writer.flush().await?;
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    ingestion.give(true).await?;
    ingestion.writer.flush().await?;
    if let Some(version) = ingestion.writer.version() {
        eprintln!("version {version}");
    }
    ingestion.writer.close().await?;
    Ok(())
}

/// Where an ingestion is: the lines read and not yet given, and, by a version column, the rows of the highest version.
struct Ingestion<'a> {
    writer: Writer,
    args: &'a Ingest,
    columns: SchemaRef,
    header: Option<SchemaRef>,
    reached: Option<i64>,
    lines: Vec<(u64, String)>,
    /// The highest version seen in the input, to refuse one that goes back.
    highest: Option<i64>,
    held: Option<RecordBatch>,
}

impl Ingestion<'_> {
    async fn read(&mut self, read: Read) -> Result<(), Error> {
        match read {
            Read::Header(line) => self.header = Some(header_schema(&line, &self.columns)?),
            Read::Line(n, line) => {
                self.lines.push((n, line));
                if self.lines.len() >= self.args.batch_lines.max(1) {
                    self.give(false).await?;
                }
            }
        }
        Ok(())
    }

    /// Gives the writer the lines read so far, as one batch; at the `end`, what it held back too.
    async fn give(&mut self, end: bool) -> Result<(), Error> {
        let lines = std::mem::take(&mut self.lines);
        let keyed = self.args.app_id.is_some();
        let Some(column) = self.args.version_column.clone().filter(|_| keyed) else {
            // By line: what the key passed is dropped before it is read, and a batch's version is its last line's.
            let reached = self.reached.filter(|_| keyed);
            let lines: Vec<(u64, String)> = lines
                .into_iter()
                .filter(|(n, _)| reached.is_none_or(|reached| *n as i64 > reached))
                .collect();
            let Some(last) = lines.last().map(|(n, _)| *n as i64) else {
                return Ok(());
            };
            let batch = self.parse(&lines)?;
            self.writer.write(batch, keyed.then_some(last)).await?;
            return Ok(());
        };
        let mut rows: Vec<RecordBatch> = self.held.take().into_iter().collect();
        if !lines.is_empty() {
            let batch = self.parse(&lines)?;
            rows.extend(self.past_reached(batch, &column)?);
        }
        let Some(first) = rows.first() else {
            return Ok(());
        };
        let rows = concat_batches(&first.schema(), &rows)?;
        let versions = versions(&rows, &column)?;
        let Some(top) = versions.iter().flatten().max() else {
            return Ok(());
        };
        // The highest version may have more rows to come, unless the input ended: it waits for the next.
        let whole: BooleanArray = versions.iter().map(|v| Some(end || v < Some(top))).collect();
        let waiting: BooleanArray = versions.iter().map(|v| Some(!end && v == Some(top))).collect();
        let given = filter_record_batch(&rows, &whole)?;
        let kept = filter_record_batch(&rows, &waiting)?;
        self.held = (kept.num_rows() > 0).then_some(kept);
        let highest_given = versions.iter().flatten().filter(|v| end || *v < top).max();
        if let Some(version) = highest_given {
            self.writer.write(given, Some(version)).await?;
        }
        Ok(())
    }

    fn parse(&self, lines: &[(u64, String)]) -> Result<RecordBatch, Error> {
        let text: Vec<&str> = lines.iter().map(|(_, line)| line.as_str()).collect();
        match self.args.input {
            Input::Json => parse_json(&text, &self.columns),
            Input::Csv => parse_csv(
                &text,
                self.header
                    .as_ref()
                    .ok_or_else(|| Error::usage("a CSV needs its header"))?,
            ),
        }
    }

    /// The rows past what the key reached, after checking, row by row, that their versions never go back.
    fn past_reached(&mut self, batch: RecordBatch, column: &str) -> Result<Option<RecordBatch>, Error> {
        let versions = versions(&batch, column)?;
        for version in versions.iter().flatten() {
            if let Some(highest) = self.highest.filter(|highest| version < *highest) {
                return Err(Error::usage(format!(
                    "{column} {version} after {highest}: the versions of a producer do not go back"
                )));
            }
            self.highest = Some(version);
        }
        let reached = self.reached;
        let past: BooleanArray = versions.iter().map(|v| Some(v > reached)).collect();
        let past = filter_record_batch(&batch, &past)?;
        Ok((past.num_rows() > 0).then_some(past))
    }
}

/// A batch's versions, by its version column, which every row has.
fn versions(batch: &RecordBatch, column: &str) -> Result<Int64Array, Error> {
    let values = batch
        .column_by_name(column)
        .ok_or_else(|| Error::usage(format!("{column} is not a column of the rows")))?;
    let values = cast(values, &DataType::Int64)?;
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::usage(format!("{column} is a version: a whole number")))?
        .clone();
    if values.null_count() > 0 {
        return Err(Error::usage(format!("{column} is a version, and has rows without one")));
    }
    Ok(values)
}

/// Reads stdin's lines one by one, on a thread of its own, since stdin blocks. Empty lines count, and are left out.
fn read_stdin(input: Input, sender: mpsc::Sender<io::Result<Read>>) {
    let stdin = io::stdin().lock();
    let mut header = input == Input::Csv;
    for (n, line) in (1u64..).zip(stdin.lines()) {
        let read = match line {
            Ok(line) if header => {
                header = false;
                Read::Header(line)
            }
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => Read::Line(n, line),
            Err(e) => {
                let _ = sender.blocking_send(Err(e));
                return;
            }
        };
        if sender.blocking_send(Ok(read)).is_err() {
            return;
        }
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
