//! Reading a table: `plan` asks the protocol which files a read opens, pruned in Postgres, and answers each as a
//! `ScanTask`; `open` reads a task as Arrow batches, straight from its storage. A task is a value, so an engine with
//! its own reader, or another process, may open it instead.
//!
//! A file opens with the footer the catalog serves (FTR-1): no request but those for its data. Without one, its
//! size is the hint that fetches the footer in one request, and a size not known (STA-1) a tail that holds most
//! footers; an inline file is read from its bytes, with none.
//! Columns are found by field id, under the table's fields at the read (`evolve`), and a range on a field also
//! skips the row groups its statistics rule out. What
//! comes back may hold rows outside the range (the engine filters them exactly), never fewer than those inside.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use metalake_pg::Prune;
use metalake_pg::records::ListedFile;
use metalake_pg::records::{PlannedFile, PlannedVector};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, RowNumber};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bounds;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::evolve::Evolve;
use crate::partition::Scalar;
use crate::schema::FIELD_ID;
use crate::stats;
use crate::storage::Storage;
use crate::table::{Field, Table};
use crate::vectors::{self, DeletionVector};

/// Rows in a batch at most.
const BATCH_SIZE: usize = 8192;
/// Files read at once.
const CONCURRENCY: usize = 16;
/// The tail fetched for a footer whose size is not known (STA-1): one request for most footers, which the catalog
/// keeps up to this size, and a second for a longer one.
const UNKNOWN_FOOTER_TAIL: usize = 64 * 1024 + 8;

/// The bytes to fetch from a file's end for its footer, its length and `PAR1` (FTR-1): a hint, as a reader fetches more
/// when the footer is longer.
fn footer_tail(footer_size: i32) -> usize {
    match footer_size {
        0 => UNKNOWN_FOOTER_TAIL,
        size => size as usize + 8,
    }
}

/// A read of one table at its read point: which columns, and a range on a field that prunes.
#[derive(Debug, Clone)]
pub struct Scan {
    catalog: Catalog,
    table: Table,
    /// The table's columns the scan reads.
    columns: SchemaRef,
    /// What it answers: those columns, then each row's file and position when asked.
    schema: SchemaRef,
    evolve: Arc<Evolve>,
    filter: Option<Filter>,
    positions: bool,
    batch_size: usize,
    concurrency: usize,
}

/// What reading one file of a scan takes.
struct Reading {
    columns: SchemaRef,
    schema: SchemaRef,
    evolve: Arc<Evolve>,
    filter: Option<Filter>,
    positions: bool,
    batch_size: usize,
    /// Only these rows of the file, whatever its vector says.
    only: Option<DeletionVector>,
}

/// A range on one field, its ends as bounds (STA-5), each inclusive.
#[derive(Debug, Clone)]
struct Filter {
    field: Field,
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
}

/// A file a scan reads, as the protocol planned it: where its bytes are, its footer, and its deletion vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanTask {
    pub file_id: i64,
    /// Its object, or none for an inline file, whose bytes are in `content` (INL-1).
    pub path: Option<String>,
    pub content: Option<Vec<u8>>,
    pub file_size: i64,
    pub footer_size: i32,
    /// Its footer, as the catalog keeps it (FTR-1).
    pub footer: Option<Vec<u8>>,
    pub record_count: i64,
    pub spec_id: i32,
    pub partition: Value,
    pub vector: Option<TaskVector>,
}

/// Where a file's deletion vector is (DV-1): in a Puffin file at `path`, or inline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskVector {
    pub begin_seq: i64,
    pub path: Option<String>,
    pub offset: Option<i64>,
    pub size: i64,
    pub cardinality: i64,
    pub inline: Option<Vec<u8>>,
}

impl ScanTask {
    /// A Parquet reader of its file, for an engine with a reader of its own: the footer comes from the catalog when it
    /// has it, and an inline file's bytes from the task.
    pub fn reader(&self, storage: &Storage) -> Result<TaskReader> {
        Ok(TaskReader {
            source: Source::of(self.content.clone(), self.path.as_deref(), self.file_id, storage)?,
            footer: self.footer.clone(),
            footer_size: self.footer_size,
            file_size: self.file_size,
        })
    }

    /// Its deletion vector: one request if it is in Puffin, none if inline.
    pub async fn vector(&self, storage: &Storage) -> Result<Option<DeletionVector>> {
        vector(self.file_id, self.vector.as_ref(), storage).await
    }

    /// Its file's metadata, if it travels with the task: from the footer the catalog keeps, or an inline file's bytes.
    /// None if only the object has it (FTR-1).
    pub fn metadata(&self) -> Result<Option<ParquetMetaData>> {
        match (&self.footer, &self.content) {
            (Some(footer), _) => ParquetMetaDataReader::decode_metadata(footer).map(Some),
            (None, Some(content)) => ParquetMetaDataReader::new()
                .parse_and_finish(&Bytes::copy_from_slice(content))
                .map(Some),
            (None, None) => Ok(None),
        }
        .map_err(read_error)
    }
}

impl Scan {
    /// A read of every column of `table`, as it was read, from `catalog`.
    pub fn new(catalog: &Catalog, table: &Table) -> Self {
        Self {
            catalog: catalog.clone(),
            table: table.clone(),
            columns: table.schema().clone(),
            schema: table.schema().clone(),
            evolve: Arc::new(Evolve::new(table)),
            filter: None,
            positions: false,
            batch_size: BATCH_SIZE,
            concurrency: CONCURRENCY,
        }
    }

    /// Only these columns, by name and in this order.
    pub fn columns<I, S>(mut self, names: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let all = self.table.schema();
        let fields = names
            .into_iter()
            .map(|name| {
                let name = name.as_ref();
                all.field_with_name(name)
                    .cloned()
                    .map_err(|_| Error::NotFound(format!("column {name} of {}", self.table.name())))
            })
            .collect::<Result<Vec<_>>>()?;
        self.columns = Arc::new(Schema::new(fields));
        self.schema = self.answered();
        Ok(self)
    }

    /// Each row's file and position in it as two more columns, `_file_id` and `_pos`, both `Int64`: what a delete
    /// names a row by (`Deletes`).
    pub fn with_positions(mut self) -> Self {
        self.positions = true;
        self.schema = self.answered();
        self
    }

    fn answered(&self) -> SchemaRef {
        let mut fields: Vec<ArrowField> = self.columns.fields().iter().map(|f| f.as_ref().clone()).collect();
        if self.positions {
            fields.push(ArrowField::new(vectors::FILE_ID, DataType::Int64, false));
            fields.push(ArrowField::new(vectors::POSITION, DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }

    /// Only rows whose `column` is in `[lo, hi]`, an end of none being open: the files and row groups that cannot
    /// hold one are not read.
    pub fn filter(mut self, column: &str, lo: Option<Scalar<'_>>, hi: Option<Scalar<'_>>) -> Result<Self> {
        let field = self
            .table
            .fields()
            .iter()
            .find(|f| f.parent_field_id.is_none() && f.name == column)
            .ok_or_else(|| Error::NotFound(format!("column {column} of {}", self.table.name())))?
            .clone();
        let encode = |v: Option<Scalar<'_>>| match v {
            None => Ok(None),
            Some(v) => bounds::value(&field.ty, v)
                .map(Some)
                .ok_or_else(|| Error::Schema(format!("{v:?} is not a value of {column}, a {}", field.ty))),
        };
        let (lo, hi) = (encode(lo)?, encode(hi)?);
        self.filter = Some(Filter { field, lo, hi });
        Ok(self)
    }

    pub fn batch_size(mut self, rows: usize) -> Self {
        self.batch_size = rows.max(1);
        self
    }

    /// How many files `read` reads at once.
    pub fn concurrency(mut self, files: usize) -> Self {
        self.concurrency = files.max(1);
        self
    }

    /// The schema of the batches the scan answers.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The files the scan reads, as they stream from the protocol.
    pub async fn plan(&self) -> Result<BoxStream<'static, Result<ScanTask>>> {
        let session = self.catalog.session().await?;
        let prune = self.filter.as_ref().map(|f| Prune {
            field_id: f.field.field_id,
            lo: f.lo.clone(),
            hi: f.hi.clone(),
        });
        let t = &self.table;
        let files = self
            .catalog
            .protocol()
            .plan(&session, t.ref_id(), t.id(), Some(t.seq()), prune.as_ref())
            .await?;
        // The session goes with the stream, whose rows are its connection's.
        Ok(files
            .map(move |file| {
                let _ = &session;
                Ok(ScanTask::from(file?))
            })
            .boxed())
    }

    /// The files the scan reads, each with its statistics, as they stream from the protocol.
    pub async fn list(&self) -> Result<BoxStream<'static, Result<ListedFile>>> {
        let session = self.catalog.session().await?;
        let t = &self.table;
        let p = self.catalog.protocol();
        let files = p.listed_files(&session, t.ref_id(), t.id(), Some(t.seq())).await?;
        // The session goes with the stream, whose rows are its connection's.
        Ok(files
            .map(move |file| {
                let _ = &session;
                Ok(file?)
            })
            .boxed())
    }

    /// Every task's batches, `concurrency` files at a time, as they come.
    pub fn read(
        &self,
        tasks: BoxStream<'static, Result<ScanTask>>,
        storage: &Storage,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let (scan, storage) = (self.clone(), storage.clone());
        tasks
            .map_ok(move |task| scan.open(task, &storage))
            .try_flatten_unordered(self.concurrency)
            .boxed()
    }

    /// One task's batches.
    pub fn open(&self, task: ScanTask, storage: &Storage) -> BoxStream<'static, Result<RecordBatch>> {
        self.opened(task, None, storage)
    }

    /// The batches of those rows of one task's file, deleted or not.
    pub(crate) fn open_only(
        &self,
        task: ScanTask,
        rows: DeletionVector,
        storage: &Storage,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        self.opened(task, Some(rows), storage)
    }

    fn opened(
        &self,
        task: ScanTask,
        only: Option<DeletionVector>,
        storage: &Storage,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let reading = Reading {
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            evolve: self.evolve.clone(),
            filter: self.filter.clone(),
            positions: self.positions,
            batch_size: self.batch_size,
            only,
        };
        let storage = storage.clone();
        stream::once(async move { batches(task, &storage, reading).await })
            .try_flatten()
            .boxed()
    }
}

/// The batches of one file, under the scan's schema.
async fn batches(task: ScanTask, storage: &Storage, r: Reading) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let Reading {
        columns,
        schema,
        evolve,
        filter,
        positions,
        batch_size,
        only,
    } = r;
    let file_id = task.file_id;
    let mut source = Source::of(task.content, task.path.as_deref(), file_id, storage)?;
    // The footer and the vector, at once: each a request at most.
    let footer = async {
        match &task.footer {
            Some(footer) => ParquetMetaDataReader::decode_metadata(footer).map_err(read_error),
            None => ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(footer_tail(task.footer_size)))
                .load_and_finish(&mut source, task.file_size as u64)
                .await
                .map_err(read_error),
        }
    };
    let (metadata, vector) = futures_util::try_join!(footer, vector(task.file_id, task.vector.as_ref(), storage))?;
    let metadata = Arc::new(metadata);
    let mut options = ArrowReaderOptions::new();
    if positions {
        let row_number = ArrowField::new(vectors::POSITION, DataType::Int64, false).with_extension_type(RowNumber);
        options = options
            .with_virtual_columns(vec![Arc::new(row_number)])
            .map_err(read_error)?;
    }
    let reader = ArrowReaderMetadata::try_new(metadata.clone(), options).map_err(read_error)?;

    // The file's top-level column of each of the schema's fields, by id, if it has one, and where each comes in
    // the projection's order.
    let in_file = reader.schema();
    let roots: Vec<Option<usize>> = schema
        .fields()
        .iter()
        .map(|f| {
            let id = f.metadata().get(FIELD_ID);
            in_file
                .fields()
                .iter()
                .position(|g| id.is_some() && g.metadata().get(FIELD_ID) == id)
        })
        .collect();
    let mut present: Vec<usize> = roots.iter().flatten().copied().collect();
    present.sort_unstable();
    let order: Vec<Option<usize>> = roots
        .iter()
        .map(|r| r.map(|r| present.binary_search(&r).expect("a root of the projection")))
        .collect();

    let groups = match &filter {
        Some(f) => kept_groups(&metadata, f),
        None => (0..metadata.num_row_groups()).collect(),
    };
    let mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), present);
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(source, reader)
        .with_projection(mask)
        .with_batch_size(batch_size);
    if only.is_some() || vector.is_some() {
        let mut first = 0u64;
        let starts: Vec<u64> = metadata
            .row_groups()
            .iter()
            .map(|g| {
                let at = first;
                first += g.num_rows() as u64;
                at
            })
            .collect();
        let read: Vec<(u64, u64)> = groups
            .iter()
            .map(|g| (starts[*g], metadata.row_group(*g).num_rows() as u64))
            .collect();
        builder = builder.with_row_selection(match (&only, &vector) {
            (Some(only), _) => only.only_these(&read),
            (None, Some(vector)) => vector.row_selection(&read),
            (None, None) => unreachable!("one of them is there"),
        });
    }
    let stream = builder.with_row_groups(groups).build().map_err(read_error)?;
    Ok(stream
        .map(move |batch| {
            let batch = batch.map_err(read_error)?;
            let mut answered = columns
                .fields()
                .iter()
                .zip(&order)
                .map(|(field, at)| evolve.column(field, at.map(|i| batch.column(i)), batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            if positions {
                let position = batch
                    .column_by_name(vectors::POSITION)
                    .ok_or_else(|| Error::Schema("Parquet answered no row numbers".into()))?;
                answered.push(Arc::new(Int64Array::from(vec![file_id; batch.num_rows()])));
                answered.push(position.clone());
            }
            let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(schema.clone(), answered, &rows).map_err(|e| Error::Schema(e.to_string()))
        })
        .boxed())
}

/// The deletion vector of a file as a list gives it, for who serves it.
pub async fn listed_vector(file: &ListedFile, storage: &Storage) -> Result<Option<DeletionVector>> {
    let at = file.vector.as_ref().map(|v| TaskVector {
        begin_seq: v.begin_seq,
        path: v.path.clone(),
        offset: v.offset,
        size: v.size,
        cardinality: v.cardinality,
        inline: v.inline.clone(),
    });
    vector(file.file_id, at.as_ref(), storage).await
}

/// A file's deletion vector, from the catalog's bytes or a range of its Puffin file, checked against the count the
/// catalog keeps.
pub(crate) async fn vector(file_id: i64, at: Option<&TaskVector>, storage: &Storage) -> Result<Option<DeletionVector>> {
    let Some(at) = at else {
        return Ok(None);
    };
    let vector = match (&at.inline, &at.path) {
        (Some(blob), _) => DeletionVector::from_blob(blob)?,
        (None, Some(path)) => {
            let start = at.offset.unwrap_or(0) as u64;
            let blob = storage
                .store()
                .get_range(&storage.path(path)?, start..start + at.size as u64)
                .await?;
            DeletionVector::from_blob(&blob)?
        }
        (None, None) => {
            return Err(Error::Corrupt(format!(
                "the vector of file {file_id} has neither bytes nor a path"
            )));
        }
    };
    if vector.cardinality() != at.cardinality as u64 {
        return Err(Error::Corrupt(format!(
            "the vector of file {file_id} holds {} positions, not {}",
            vector.cardinality(),
            at.cardinality
        )));
    }
    Ok(Some(vector))
}

/// The row groups whose statistics may hold a value of the filter's range: those without bounds too.
fn kept_groups(metadata: &ParquetMetaData, filter: &Filter) -> Vec<usize> {
    let descr = metadata.file_metadata().schema_descr();
    let column = (0..descr.num_columns()).find(|i| {
        let info = descr.column(*i).self_type_ptr();
        let info = info.get_basic_info();
        info.has_id() && info.id() == filter.field.field_id
    });
    (0..metadata.num_row_groups())
        .filter(|g| {
            let Some((lower, upper)) = column.and_then(|c| stats::row_group_bounds(&filter.field, metadata, *g, c))
            else {
                return true;
            };
            !(filter.lo.as_ref().is_some_and(|lo| upper < *lo) || filter.hi.as_ref().is_some_and(|hi| lower > *hi))
        })
        .collect()
}

/// A file's bytes: inline, or an object, each range of which is a request.
enum Source {
    Inline(Bytes),
    Object { store: Arc<dyn ObjectStore>, path: Path },
}

impl Source {
    fn of(content: Option<Vec<u8>>, path: Option<&str>, file_id: i64, storage: &Storage) -> Result<Self> {
        match (content, path) {
            (Some(content), _) => Ok(Self::Inline(Bytes::from(content))),
            (None, Some(path)) => Ok(Self::Object {
                store: storage.store().clone(),
                path: storage.path(path)?,
            }),
            (None, None) => Err(Error::Schema(format!("file {file_id} has neither a path nor content"))),
        }
    }
}

/// A task's file as Parquet reads it, with its metadata.
pub struct TaskReader {
    source: Source,
    footer: Option<Vec<u8>>,
    footer_size: i32,
    file_size: i64,
}

impl AsyncFileReader for TaskReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.source.get_bytes(range)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.source.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async {
            Ok(Arc::new(match &self.footer {
                Some(footer) => ParquetMetaDataReader::decode_metadata(footer)?,
                None => {
                    ParquetMetaDataReader::new()
                        .with_prefetch_hint(Some(footer_tail(self.footer_size)))
                        .load_and_finish(&mut self.source, self.file_size as u64)
                        .await?
                }
            }))
        }
        .boxed()
    }
}

impl AsyncFileReader for Source {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            match self {
                Self::Inline(bytes) => Ok(bytes.slice(range.start as usize..range.end as usize)),
                Self::Object { store, path } => store.get_range(path, range).await.map_err(external),
            }
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            match self {
                Self::Inline(bytes) => Ok(ranges
                    .into_iter()
                    .map(|r| bytes.slice(r.start as usize..r.end as usize))
                    .collect()),
                Self::Object { store, path } => store.get_ranges(path, &ranges).await.map_err(external),
            }
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async {
            Err(ParquetError::General(
                "a task's metadata is read before its reader".into(),
            ))
        }
        .boxed()
    }
}

impl From<PlannedFile> for ScanTask {
    fn from(f: PlannedFile) -> Self {
        Self {
            file_id: f.file_id,
            path: f.path,
            content: f.content,
            file_size: f.file_size,
            footer_size: f.footer_size,
            footer: f.footer,
            record_count: f.record_count,
            spec_id: f.spec_id,
            partition: f.partition,
            vector: f.vector.map(TaskVector::from),
        }
    }
}

impl From<PlannedVector> for TaskVector {
    fn from(v: PlannedVector) -> Self {
        Self {
            begin_seq: v.begin_seq,
            path: v.path,
            offset: v.offset,
            size: v.size,
            cardinality: v.cardinality,
            inline: v.inline,
        }
    }
}

fn external(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

fn read_error(e: ParquetError) -> Error {
    match e {
        ParquetError::External(inner) => match inner.downcast::<object_store::Error>() {
            Ok(e) => Error::Storage(*e),
            Err(other) => Error::Schema(format!("reading Parquet: {other}")),
        },
        other => Error::Schema(format!("reading Parquet: {other}")),
    }
}
