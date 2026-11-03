//! The writer: Arrow batches of one table into Parquet files in its storage, each file's intent registered before
//! its upload (CMT-10), and the files a commit adds, with their statistics, when it finishes. A file that ends no
//! larger than the table's inline limit is never uploaded: its bytes travel in the commit (INL-1).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch, make_array};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, SchemaRef};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use metalake_pg::records::FileAdd;
use object_store::{ObjectStoreExt, WriteMultipart};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde_json::json;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::schema::FIELD_ID;
use crate::stats::{self, FileFacts};
use crate::storage::Storage;
use crate::table::Table;

/// The size of an inline file at most, unless the table's property `inline.max-bytes` says otherwise (INL-1).
const INLINE_MAX_BYTES: usize = 65_536;
/// The size a file is closed at, unless the table's property `write.target-file-size-bytes` says otherwise.
const TARGET_FILE_SIZE: usize = 256 * 1024 * 1024;

/// How a writer writes.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// How long a registered intent lives before an unfinished file is an orphan (RET-7).
    pub intent_ttl: Duration,
    /// Rows in a row group at most.
    pub row_group_rows: usize,
    /// Overrides the table's target file size.
    pub target_file_size: Option<usize>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            intent_ttl: Duration::from_secs(3600),
            row_group_rows: 1024 * 1024,
            target_file_size: None,
        }
    }
}

/// The writer of one table, as it was read: its files follow that read's schema and default spec.
pub struct TableWriter {
    catalog: Catalog,
    table: Table,
    storage: Storage,
    options: WriteOptions,
    inline_max: usize,
    target: usize,
    open: Option<OpenFile>,
    done: Vec<FileAdd>,
}

struct OpenFile {
    writer: AsyncArrowWriter<Spill>,
    nan_counts: HashMap<i32, i64>,
}

impl TableWriter {
    /// A writer of `table` into `storage`, the store of its location.
    pub fn new(catalog: &Catalog, table: &Table, storage: Storage, options: WriteOptions) -> Result<Self> {
        let spec = table.spec_id().and_then(|id| table.specs().get(&id));
        if spec.and_then(|s| s.as_array()).is_some_and(|fields| !fields.is_empty()) {
            return Err(Error::Schema(format!(
                "{} is partitioned: partitioned writes come with C4c",
                table.name()
            )));
        }
        let property = |key: &str| table.properties().get(key).and_then(|v| v.parse::<usize>().ok());
        Ok(Self {
            catalog: catalog.clone(),
            table: table.clone(),
            storage,
            inline_max: property("inline.max-bytes").unwrap_or(INLINE_MAX_BYTES),
            target: options
                .target_file_size
                .or_else(|| property("write.target-file-size-bytes"))
                .unwrap_or(TARGET_FILE_SIZE),
            options,
            open: None,
            done: Vec::new(),
        })
    }

    /// Writes a batch: its columns are the table's, by name and in order, of the same types; a required column
    /// holds no null.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = conform(self.table.schema(), batch)?;
        if self.open.is_none() {
            self.open = Some(self.open_file()?);
        }
        let open = self.open.as_mut().expect("a file is open");
        count_nans(self.table.schema(), batch.columns(), &mut open.nan_counts);
        open.writer.write(&batch).await.map_err(parquet_error)?;
        if open.writer.bytes_written() + open.writer.in_progress_size() >= self.target {
            self.close_file().await?;
        }
        Ok(())
    }

    /// Closes the open file, and answers every file written, for one commit.
    pub async fn finish(mut self) -> Result<Vec<FileAdd>> {
        self.close_file().await?;
        Ok(self.done)
    }

    fn open_file(&self) -> Result<OpenFile> {
        let path = format!(
            "{}/data/{}.parquet",
            self.table.location().trim_end_matches('/'),
            Uuid::new_v4().simple()
        );
        let spill = Spill {
            catalog: self.catalog.clone(),
            ref_id: self.table.ref_id(),
            table_id: self.table.id(),
            ttl: self.options.intent_ttl,
            storage: self.storage.clone(),
            path,
            inline_max: self.inline_max,
            buffer: Vec::new(),
            state: State::Inline,
            written: 0,
            tail: Vec::with_capacity(16),
        };
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(self.options.row_group_rows))
            .build();
        let writer =
            AsyncArrowWriter::try_new(spill, self.table.schema().clone(), Some(properties)).map_err(parquet_error)?;
        Ok(OpenFile {
            writer,
            nan_counts: HashMap::new(),
        })
    }

    async fn close_file(&mut self) -> Result<()> {
        let Some(mut open) = self.open.take() else {
            return Ok(());
        };
        let metadata = open.writer.finish().await.map_err(parquet_error)?;
        let spill = open.writer.into_inner();
        // The footer's length is in the file's last 8 bytes: the page indexes come before it, so it cannot be told
        // from what was written before closing.
        let footer_size = stats::footer_size(&spill.tail)
            .ok_or_else(|| Error::Schema("the writer ended a file without a Parquet tail".into()))?;
        let file_size = spill.written as i64;
        let (path, content) = match spill.state {
            State::Inline => (None, Some(spill.buffer)),
            State::Uploading(_) | State::Uploaded => (Some(spill.path), None),
        };
        let statistics = stats::of_footer(&self.table, &metadata, &open.nan_counts);
        self.done.push(statistics.into_file_add(
            self.table.id(),
            FileFacts {
                path,
                content,
                file_size,
                footer_size,
                spec_id: self.table.spec_id().unwrap_or(0),
                partition: json!([]),
                sort_order_id: None,
            },
        ));
        Ok(())
    }
}

/// The destination of one file: in memory while it fits the inline limit; past it, its intent registered, then a
/// multipart upload to its path, which `complete` finishes.
struct Spill {
    catalog: Catalog,
    ref_id: i32,
    table_id: Uuid,
    ttl: Duration,
    storage: Storage,
    path: String,
    inline_max: usize,
    buffer: Vec<u8>,
    state: State,
    written: usize,
    /// The last 8 bytes written, which hold the footer's length.
    tail: Vec<u8>,
}

/// Where a file's bytes are.
enum State {
    /// In `buffer`, within the inline limit.
    Inline,
    /// Being uploaded to its path.
    Uploading(WriteMultipart),
    /// Uploaded to its path.
    Uploaded,
}

impl AsyncFileWriter for Spill {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.written += bs.len();
            self.tail.extend_from_slice(&bs[bs.len().saturating_sub(8)..]);
            let excess = self.tail.len().saturating_sub(8);
            self.tail.drain(..excess);
            if matches!(self.state, State::Inline) && self.buffer.len() + bs.len() <= self.inline_max {
                self.buffer.extend_from_slice(&bs);
                return Ok(());
            }
            if matches!(self.state, State::Inline) {
                let p = self.catalog.protocol();
                let session = self.catalog.session().await.map_err(external)?;
                p.register_intents(
                    &session,
                    self.ref_id,
                    self.table_id,
                    std::slice::from_ref(&self.path),
                    self.ttl,
                )
                .await
                .map_err(|e| external(e.into()))?;
                let location = self.storage.path(&self.path).map_err(external)?;
                let upload = self
                    .storage
                    .store()
                    .put_multipart(&location)
                    .await
                    .map_err(|e| external(e.into()))?;
                let mut upload = WriteMultipart::new(upload);
                upload.write(&std::mem::take(&mut self.buffer));
                self.state = State::Uploading(upload);
            }
            let State::Uploading(upload) = &mut self.state else {
                return Err(ParquetError::General("a file written after it was uploaded".into()));
            };
            upload.wait_for_capacity(8).await.map_err(|e| external(e.into()))?;
            upload.write(&bs);
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            if let State::Uploading(upload) = std::mem::replace(&mut self.state, State::Uploaded) {
                upload.finish().await.map_err(|e| external(e.into()))?;
            } else {
                self.state = State::Inline;
            }
            Ok(())
        })
    }
}

fn external(e: Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

fn parquet_error(e: ParquetError) -> Error {
    match e {
        ParquetError::External(inner) => match inner.downcast::<Error>() {
            Ok(e) => *e,
            Err(other) => Error::Schema(other.to_string()),
        },
        other => Error::Schema(format!("writing Parquet: {other}")),
    }
}

/// The batch with the table's schema, whose fields carry the ids: its columns must be the table's, by name and in
/// order, of the same physical types (a `timestamptz` may come in any zone), and a required column holds no null.
fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let given = batch.schema();
    let names = |s: &arrow_schema::Schema| s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
    if names(&given) != names(schema) {
        return Err(Error::Schema(format!(
            "the batch's columns {:?} are not the table's {:?}",
            names(&given),
            names(schema)
        )));
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            if !field.is_nullable() && column.null_count() > 0 {
                return Err(Error::Schema(format!(
                    "{} is required and the batch has nulls in it",
                    field.name()
                )));
            }
            retype(column, field.data_type()).map_err(|e| Error::Schema(format!("{}: {e}", field.name())))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Schema(e.to_string()))
}

/// The same array under `ty`, which must be the same physical type: only names, ids, nullability and a timestamp's
/// zone may differ, and nested fields are retyped in turn.
fn retype(array: &ArrayRef, ty: &DataType) -> std::result::Result<ArrayRef, ArrowError> {
    if !same_layout(array.data_type(), ty) {
        return Err(ArrowError::SchemaError(format!("{} is not {ty}", array.data_type())));
    }
    let data = array.to_data();
    let children = match ty {
        DataType::Struct(fields) => {
            let s = array.as_struct();
            fields
                .iter()
                .zip(s.columns())
                .map(|(f, c)| retype(c, f.data_type()).map(|a| a.to_data()))
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
        DataType::List(element) => vec![retype(array.as_list::<i32>().values(), element.data_type())?.to_data()],
        DataType::Map(entries, _) => {
            let m = array.as_map();
            let entries_array: ArrayRef = Arc::new(m.entries().clone());
            vec![retype(&entries_array, entries.data_type())?.to_data()]
        }
        _ => data.child_data().to_vec(),
    };
    let data = data.into_builder().data_type(ty.clone()).child_data(children).build()?;
    Ok(make_array(data))
}

fn same_layout(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (Timestamp(u, x), Timestamp(v, y)) => u == v && x.is_some() == y.is_some(),
        (Struct(f), Struct(g)) => {
            f.len() == g.len() && f.iter().zip(g).all(|(f, g)| same_layout(f.data_type(), g.data_type()))
        }
        (List(e), List(f)) => same_layout(e.data_type(), f.data_type()),
        (Map(e, _), Map(f, _)) => same_layout(e.data_type(), f.data_type()),
        (a, b) => a == b,
    }
}

/// Adds the NaN of each `float` or `double` column to its field's count: a footer has none (STA-2).
fn count_nans(schema: &SchemaRef, columns: &[ArrayRef], counts: &mut HashMap<i32, i64>) {
    for (field, column) in schema.fields().iter().zip(columns) {
        count_field(field, column, counts);
    }
}

fn count_field(field: &ArrowField, column: &ArrayRef, counts: &mut HashMap<i32, i64>) {
    let id = field.metadata().get(FIELD_ID).and_then(|v| v.parse::<i32>().ok());
    let nans = match field.data_type() {
        DataType::Float64 => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.iter().flatten().filter(|v| v.is_nan()).count()),
        DataType::Float32 => column
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.iter().flatten().filter(|v| v.is_nan()).count()),
        DataType::Struct(fields) => {
            for (f, c) in fields.iter().zip(column.as_struct().columns()) {
                count_field(f, c, counts);
            }
            None
        }
        DataType::List(element) => {
            let list = column.as_list::<i32>();
            let offsets = list.value_offsets();
            let (start, end) = (offsets[0] as usize, offsets[offsets.len() - 1] as usize);
            count_field(element, &list.values().slice(start, end - start), counts);
            None
        }
        _ => None,
    };
    if let (Some(id), Some(n)) = (id, nans) {
        *counts.entry(id).or_default() += n as i64;
    }
}
