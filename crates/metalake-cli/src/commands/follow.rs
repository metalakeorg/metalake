//! `metalake follow`: a table's changes as they come, printed step after step until Ctrl-C, which says the position to
//! go on after. A position file is read at the start and written after each step is printed, whole or not at all, so a
//! follower stopped anywhere goes on with the first commit it had not printed.

use std::io::{self, BufWriter, IsTerminal, Stdout, Write};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::{TryStreamExt, stream};
use metalake_client::{Error, Lake, StartAt, TablePath};

use crate::args::Follow;
use crate::out::{Format, Out};

pub async fn run(args: &Follow, lake: &Lake, format: Option<Format>) -> Result<(), Error> {
    let path = TablePath::parse(&args.table)?;
    let kept = match &args.position_file {
        Some(file) => kept(file)?,
        None => None,
    };
    let start = match (args.after.or(kept), args.latest, &args.since) {
        (Some(after), _, _) => StartAt::After(after),
        (None, true, _) => StartAt::Latest,
        (None, false, Some(since)) => StartAt::Time(time(since)?),
        (None, false, None) => StartAt::Snapshot,
    };
    let mut follower = lake.follow(&path, start).await?;

    // A stream is read by a program in JSON lines, which take new columns as they come; a person reads tables.
    let format = format.unwrap_or(if io::stdout().is_terminal() {
        Format::Table
    } else {
        Format::Json
    });
    let out = Out::new(Some(format));
    let mut csv: Option<(SchemaRef, csv::Writer<BufWriter<Stdout>>)> = None;
    loop {
        let step = tokio::select! {
            step = follower.wait(args.interval) => step?,
            _ = tokio::signal::ctrl_c() => break,
        };
        let schema = follower.schema();
        let batches: Vec<RecordBatch> = step.try_collect().await?;
        if format == Format::Csv {
            let (columns, writer) = csv.get_or_insert_with(|| {
                let writer = csv::WriterBuilder::new()
                    .with_header(true)
                    .build(BufWriter::new(io::stdout()));
                (schema.clone(), writer)
            });
            if *columns != schema {
                let at = follower.position().map_or("its start".to_string(), |p| p.to_string());
                return Err(Error::failed(format!(
                    "{path} changed its columns, which a CSV header cannot: follow it again after {at}, or in json"
                )));
            }
            for batch in &batches {
                writer.write(batch)?;
            }
            let mut stdout = io::stdout();
            stdout.flush()?;
        } else if batches.iter().any(|b| b.num_rows() > 0) {
            let rows = RecordBatchStreamAdapter::new(schema, stream::iter(batches.into_iter().map(Ok)));
            out.rows(Box::pin(rows)).await?;
        }
        if let (Some(file), Some(position)) = (&args.position_file, follower.position()) {
            keep(file, position)?;
        }
    }
    if let Some(position) = follower.position() {
        eprintln!("position {position}");
    }
    Ok(())
}

/// The position a file holds, if it is there.
fn kept(file: &Path) -> Result<Option<i64>, Error> {
    match std::fs::read_to_string(file) {
        Ok(text) => text
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| Error::usage(format!("{}: a position is a commit sequence", file.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::failed(format!("{}: {e}", file.display()))),
    }
}

/// Writes the position whole: beside the file, then in its place.
fn keep(file: &Path, position: i64) -> Result<(), Error> {
    let beside = file.with_extension("partial");
    std::fs::write(&beside, format!("{position}\n"))?;
    std::fs::rename(&beside, file)?;
    Ok(())
}

fn time(text: &str) -> Result<std::time::SystemTime, Error> {
    let nanos =
        string_to_timestamp_nanos(text).map_err(|e| Error::usage(format!("{text}: a time, in RFC 3339: {e}")))?;
    Ok(UNIX_EPOCH + Duration::from_nanos(nanos.max(0) as u64))
}
