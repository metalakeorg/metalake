//! The writer: Arrow batches of one table into Parquet files in its storage, each file's intent registered before
//! its upload (CMT-10), and the files a commit adds, with their statistics, when it finishes. A file that ends no
//! larger than the table's inline limit is never uploaded: its bytes travel in the commit (INL-1).

use std::collections::HashMap;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type, Time64MicrosecondType,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field as ArrowField, SchemaRef};
use arrow_select::take::take_record_batch;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use metalake_pg::records::FileAdd;
use object_store::{ObjectStoreExt, WriteMultipart};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::partition::{Scalar, Transform};
use crate::schema::{self, FIELD_ID};
use crate::stats::{self, FileFacts};
use crate::storage::Storage;
use crate::table::Table;

/// The size of an inline file at most, unless the table's property `inline.max-bytes` says otherwise (INL-1).
const INLINE_MAX_BYTES: usize = 65_536;
/// The footer a file's commit carries at most, for the catalog to serve it to readers (FTR-1): a larger one is read
/// from the file, its size the hint.
const FOOTER_MAX_BYTES: usize = 65_536;
/// The bytes a file's tail is kept to: its footer at most, then the footer's length and `PAR1`.
const TAIL: usize = FOOTER_MAX_BYTES + 8;
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
    /// Stores every file, however small, as flushing inline files needs (INL-2).
    pub stored: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            intent_ttl: Duration::from_secs(3600),
            row_group_rows: 1024 * 1024,
            target_file_size: None,
            stored: false,
        }
    }
}

/// The size a file of `table` is closed at.
pub(crate) fn target_size(table: &Table) -> usize {
    table
        .properties()
        .get("write.target-file-size-bytes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(TARGET_FILE_SIZE)
}

/// The writer of one table, as it was read: its files follow that read's schema and default spec. A partitioned
/// table's rows go to one open file for each partition value (PAR-2).
pub struct TableWriter {
    catalog: Catalog,
    table: Table,
    storage: Storage,
    options: WriteOptions,
    inline_max: usize,
    target: usize,
    spec_id: i32,
    partitioning: Vec<PartitionField>,
    open: HashMap<String, OpenFile>,
    done: Vec<FileAdd>,
}

/// A field of the default spec: the source field's id and type, and the transform.
struct PartitionField {
    source_id: i32,
    ty: String,
    transform: Transform,
}

struct OpenFile {
    writer: AsyncArrowWriter<Spill>,
    nan_counts: HashMap<i32, i64>,
    partition: Value,
}

impl TableWriter {
    /// A writer of `table` into `storage`, the store of its location.
    pub fn new(catalog: &Catalog, table: &Table, storage: Storage, options: WriteOptions) -> Result<Self> {
        let spec_id = table.spec_id().unwrap_or(0);
        let spec = table
            .specs()
            .get(&spec_id)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let partitioning = spec
            .iter()
            .map(|f| {
                let source_id = f["source_ids"][0]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or_else(|| Error::Schema(format!("a partition field without a source: {f}")))?;
                let ty = table
                    .fields()
                    .iter()
                    .find(|x| x.field_id == source_id)
                    .map(|x| x.ty.clone())
                    .ok_or_else(|| Error::Schema(format!("partition source {source_id} is not a field")))?;
                let transform = Transform::parse(f["transform"].as_str().unwrap_or_default())?;
                Ok(PartitionField {
                    source_id,
                    ty,
                    transform,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let property = |key: &str| table.properties().get(key).and_then(|v| v.parse::<usize>().ok());
        Ok(Self {
            catalog: catalog.clone(),
            table: table.clone(),
            storage,
            inline_max: match options.stored {
                true => 0,
                false => property("inline.max-bytes").unwrap_or(INLINE_MAX_BYTES),
            },
            target: options.target_file_size.unwrap_or_else(|| target_size(table)),
            options,
            spec_id,
            partitioning,
            open: HashMap::new(),
            done: Vec::new(),
        })
    }

    /// Writes a batch: its columns are the table's, by name and in order, of the same types; a required column
    /// holds no null.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = conform(self.table.schema(), batch)?;
        for (partition, rows) in self.split(&batch)? {
            let key = partition.to_string();
            if !self.open.contains_key(&key) {
                let file = self.open_file(partition)?;
                self.open.insert(key.clone(), file);
            }
            let open = self.open.get_mut(&key).expect("a file is open");
            count_nans(self.table.schema(), rows.columns(), &mut open.nan_counts);
            open.writer.write(&rows).await.map_err(parquet_error)?;
            if open.writer.bytes_written() + open.writer.in_progress_size() >= self.target {
                self.close_file(&key).await?;
            }
        }
        Ok(())
    }

    /// Closes the open files, and answers every file written, for one commit.
    pub async fn finish(mut self) -> Result<Vec<FileAdd>> {
        let mut keys: Vec<String> = self.open.keys().cloned().collect();
        keys.sort();
        for key in keys {
            self.close_file(&key).await?;
        }
        Ok(self.done)
    }

    /// The batch's rows by partition value, in the order the values first appear.
    fn split(&self, batch: &RecordBatch) -> Result<Vec<(Value, RecordBatch)>> {
        if self.partitioning.is_empty() {
            return Ok(vec![(json!([]), batch.clone())]);
        }
        let sources = self
            .partitioning
            .iter()
            .map(|f| {
                column_of(self.table.schema().fields(), batch.columns(), f.source_id)
                    .ok_or_else(|| Error::Schema(format!("partition source {} is not a column", f.source_id)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut groups: Vec<(Value, Vec<u32>)> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        for row in 0..batch.num_rows() {
            let values = self
                .partitioning
                .iter()
                .zip(&sources)
                .map(|(f, column)| f.transform.apply(&f.ty, scalar_at(column, &f.ty, row)?))
                .collect::<Result<Vec<_>>>()?;
            let value = Value::Array(values);
            let at = *index.entry(value.to_string()).or_insert_with(|| {
                groups.push((value, Vec::new()));
                groups.len() - 1
            });
            groups[at].1.push(row as u32);
        }
        groups
            .into_iter()
            .map(|(value, rows)| {
                let rows =
                    take_record_batch(batch, &UInt32Array::from(rows)).map_err(|e| Error::Schema(e.to_string()))?;
                Ok((value, rows))
            })
            .collect()
    }

    fn open_file(&self, partition: Value) -> Result<OpenFile> {
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
            tail: Vec::new(),
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
            partition,
        })
    }

    async fn close_file(&mut self, key: &str) -> Result<()> {
        let Some(mut open) = self.open.remove(key) else {
            return Ok(());
        };
        let metadata = open.writer.finish().await.map_err(parquet_error)?;
        let spill = open.writer.into_inner();
        // The footer's length is in the file's last 8 bytes: the page indexes come before it, so it cannot be told
        // from what was written before closing.
        let footer_size = stats::footer_size(&spill.tail)
            .ok_or_else(|| Error::Schema("the writer ended a file without a Parquet tail".into()))?;
        let file_size = spill.written as i64;
        // An inline file's footer is in its content: the commit carries the footer of an uploaded one.
        let (path, content, footer) = match spill.state {
            State::Inline => (None, Some(spill.buffer), None),
            State::Uploading(_) | State::Uploaded => {
                let (n, size) = (spill.tail.len(), footer_size as usize);
                let footer = (size + 8 <= n).then(|| spill.tail[n - 8 - size..n - 8].to_vec());
                (Some(spill.path), None, footer)
            }
        };
        let statistics = stats::of_footer(&self.table, &metadata, &open.nan_counts);
        self.done.push(statistics.into_file_add(
            self.table.id(),
            FileFacts {
                path,
                content,
                file_size,
                footer_size,
                footer,
                spec_id: self.spec_id,
                partition: open.partition,
                sort_order_id: None,
            },
        ));
        Ok(())
    }
}

/// The column of field `id`, top level or inside structs.
fn column_of(fields: &arrow_schema::Fields, columns: &[ArrayRef], id: i32) -> Option<ArrayRef> {
    for (field, column) in fields.iter().zip(columns) {
        if field.metadata().get(FIELD_ID).and_then(|v| v.parse::<i32>().ok()) == Some(id) {
            return Some(column.clone());
        }
        if let DataType::Struct(children) = field.data_type() {
            if let Some(found) = column_of(children, column.as_struct().columns(), id) {
                return Some(found);
            }
        }
    }
    None
}

/// The value at `row` of a column of the specification's type `ty`, as a transform takes it.
fn scalar_at<'a>(column: &'a ArrayRef, ty: &str, row: usize) -> Result<Scalar<'a>> {
    if column.is_null(row) {
        return Ok(Scalar::Null);
    }
    let wrong = || Error::Schema(format!("a {ty} column holds {}", column.data_type()));
    let base = ty.split(['(', '[']).next().unwrap_or(ty);
    Ok(match base {
        "boolean" => Scalar::Boolean(column.as_boolean_opt().ok_or_else(wrong)?.value(row)),
        "int" => Scalar::Long(i64::from(
            column.as_primitive_opt::<Int32Type>().ok_or_else(wrong)?.value(row),
        )),
        "long" => Scalar::Long(column.as_primitive_opt::<Int64Type>().ok_or_else(wrong)?.value(row)),
        "date" => Scalar::Long(i64::from(
            column.as_primitive_opt::<Date32Type>().ok_or_else(wrong)?.value(row),
        )),
        "time" => Scalar::Long(
            column
                .as_primitive_opt::<Time64MicrosecondType>()
                .ok_or_else(wrong)?
                .value(row),
        ),
        "timestamp" | "timestamptz" => Scalar::Long(
            column
                .as_primitive_opt::<TimestampMicrosecondType>()
                .ok_or_else(wrong)?
                .value(row),
        ),
        "timestamp_ns" | "timestamptz_ns" => Scalar::Long(
            column
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(wrong)?
                .value(row),
        ),
        "float" => Scalar::Double(f64::from(
            column.as_primitive_opt::<Float32Type>().ok_or_else(wrong)?.value(row),
        )),
        "double" => Scalar::Double(column.as_primitive_opt::<Float64Type>().ok_or_else(wrong)?.value(row)),
        "decimal" => Scalar::Decimal(
            column
                .as_primitive_opt::<Decimal128Type>()
                .ok_or_else(wrong)?
                .value(row),
        ),
        "string" => Scalar::String(column.as_string_opt::<i32>().ok_or_else(wrong)?.value(row)),
        "uuid" | "fixed" => Scalar::Bytes(column.as_fixed_size_binary_opt().ok_or_else(wrong)?.value(row)),
        "binary" => Scalar::Bytes(column.as_binary_opt::<i32>().ok_or_else(wrong)?.value(row)),
        _ => return Err(wrong()),
    })
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
    /// The last bytes written: the footer, its length and `PAR1`, unless the footer is longer than the catalog keeps.
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
            self.tail.extend_from_slice(&bs[bs.len().saturating_sub(TAIL)..]);
            let excess = self.tail.len().saturating_sub(TAIL);
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
            schema::retype(column, field.data_type()).map_err(|e| Error::Schema(format!("{}: {e}", field.name())))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Schema(e.to_string()))
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
