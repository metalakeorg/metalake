//! C5a (ADR 0003 §7): the scan. What is read is what was written, inline and uploaded, under a projection by name;
//! a file opens with the footer the catalog serves and no request but for its data, or one more with only its
//! footer's size; a range prunes files in Postgres and row groups in the reader, keeping every row inside it; and a
//! file with a deletion vector is refused until the core reads vectors.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use metalake::partition::Scalar;
use metalake::{
    Catalog, CatalogConfig, Error, NewTable, Scan, ScanTask, Storage, Table, TableWriter, TaskVector, WriteOptions,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use parquet::file::metadata::ParquetMetaDataReader;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("scan_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A store in memory that counts its GET requests, each range of a read being one, as a remote store's are.
#[derive(Debug, Default)]
struct Counting {
    inner: InMemory,
    gets: AtomicUsize,
}

impl Counting {
    fn take(&self) -> usize {
        self.gets.swap(0, Ordering::SeqCst)
    }
}

impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Rows `keys`: `d` is NaN every 10th row and null every 7th, `s` varied enough not to compress away.
fn batch(keys: Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("d", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let keys: Vec<i64> = keys.collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.clone())),
        Arc::new(Float64Array::from_iter(keys.iter().map(|k| match k {
            k if k % 7 == 0 => None,
            k if k % 10 == 0 => Some(f64::NAN),
            k => Some(*k as f64 / 3.0),
        }))),
        Arc::new(StringArray::from_iter_values(
            keys.iter()
                .map(|k| format!("{:x}-{}", k.wrapping_mul(2_654_435_761), k * 7)),
        )),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(keys.iter().map(|k| 1_700_000_000_000_000 + k))
                .with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn setup() -> Result<(Catalog, Arc<Counting>, Storage), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let main = catalog.branch("main").await?;
    let columns = Schema::new(
        batch(0..1)
            .schema()
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Timestamp(u, Some(_)) => {
                    Field::new(f.name(), DataType::Timestamp(*u, Some("+00:00".into())), true)
                }
                _ => f.as_ref().clone(),
            })
            .collect::<Vec<_>>(),
    );
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "events",
        schema: &columns,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let store = Arc::new(Counting::default());
    let storage = Storage::from_store("memory:///", store.clone())?;
    Ok((catalog, store, storage))
}

async fn table(catalog: &Catalog) -> Result<Table, Error> {
    catalog.branch("main").await?.table(&["db"], "events").await
}

async fn write(
    catalog: &Catalog,
    storage: &Storage,
    batches: Vec<RecordBatch>,
    options: WriteOptions,
) -> Result<(), Error> {
    let t = table(catalog).await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), options)?;
    for b in &batches {
        writer.write(b).await?;
    }
    let files = writer.finish().await?;
    catalog
        .branch("main")
        .await?
        .transaction()
        .append(files)
        .commit()
        .await?;
    Ok(())
}

async fn tasks(scan: &Scan) -> Result<Vec<ScanTask>, Error> {
    scan.plan().await?.try_collect().await
}

fn replay(tasks: &[ScanTask]) -> BoxStream<'static, metalake::Result<ScanTask>> {
    let tasks: Vec<_> = tasks.iter().cloned().map(Ok).collect();
    stream::iter(tasks).boxed()
}

/// The keys in column `column` of the batches, sorted.
fn keys(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(column).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    keys
}

fn row_groups(task: &ScanTask) -> usize {
    ParquetMetaDataReader::decode_metadata(task.footer.as_ref().expect("a footer"))
        .unwrap()
        .num_row_groups()
}

#[tokio::test]
async fn what_is_read_is_what_was_written_with_no_request_for_a_footer() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, store, storage) = setup().await?;
    write(&catalog, &storage, vec![batch(0..10)], WriteOptions::default()).await?;
    let rolled = WriteOptions {
        row_group_rows: 20_000,
        target_file_size: Some(256 * 1024),
        ..Default::default()
    };
    let chunks = (10..200_010).step_by(20_000).map(|s| batch(s..s + 20_000)).collect();
    write(&catalog, &storage, chunks, rolled).await?;
    let t = table(&catalog).await?;
    let scan = Scan::new(&catalog, &t);

    // One inline file, its bytes in its task; uploaded files, each with the footer its commit carried.
    let planned = tasks(&scan).await?;
    let (inline, uploaded): (Vec<_>, Vec<_>) = planned.iter().partition(|t| t.content.is_some());
    assert_eq!(inline.len(), 1);
    assert!(inline[0].path.is_none() && inline[0].footer.is_none());
    assert!(uploaded.len() > 1, "{} files", uploaded.len());
    for task in &uploaded {
        let footer = task.footer.as_ref().expect("the commit carried the footer");
        assert_eq!(footer.len(), task.footer_size as usize);
    }
    let json = serde_json::to_string(&planned[0])?;
    assert_eq!(serde_json::from_str::<ScanTask>(&json)?, planned[0]);

    // Every row, as written; requests only for data: one per row group of an uploaded file.
    store.take();
    let started = Instant::now();
    let mut stream = scan.read(replay(&planned), &storage);
    let first = stream.try_next().await?.expect("a first batch");
    let first_batch = started.elapsed();
    let mut batches = vec![first];
    batches.extend(stream.try_collect::<Vec<_>>().await?);
    let groups: usize = uploaded.iter().map(|t| row_groups(t)).sum();
    assert_eq!(store.take(), groups, "one GET per row group, none for a footer");
    eprintln!(
        "first batch in {first_batch:?}, {} files, {groups} row groups",
        planned.len()
    );
    assert_eq!(batches[0].schema(), *scan.schema());
    assert_eq!(keys(&batches, 0), (0..200_010).collect::<Vec<_>>());
    let all = arrow_select::concat::concat_batches(scan.schema(), &batches)?;
    let expected = batch(0..200_010);
    let by_key: Vec<usize> = all
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .map(|k| *k as usize)
        .collect();
    let d = all.column(1).as_primitive::<Float64Type>();
    let want_d = expected.column(1).as_primitive::<Float64Type>();
    let s = all.column(2).as_string::<i32>();
    let want_s = expected.column(2).as_string::<i32>();
    let at = all.column(3).as_primitive::<TimestampMicrosecondType>();
    let want_at = expected.column(3).as_primitive::<TimestampMicrosecondType>();
    for (row, k) in by_key.iter().enumerate() {
        assert_eq!(d.is_null(row), want_d.is_null(*k), "d of {k}");
        if d.is_valid(row) {
            assert_eq!(d.value(row).to_bits(), want_d.value(*k).to_bits(), "d of {k}");
        }
        assert_eq!(s.value(row), want_s.value(*k), "s of {k}");
        assert_eq!(at.value(row), want_at.value(*k), "at of {k}");
    }

    // Only its footer's size: one more request per uploaded file, the footer's.
    let sized: Vec<ScanTask> = planned
        .iter()
        .cloned()
        .map(|t| ScanTask { footer: None, ..t })
        .collect();
    let rows: usize = scan
        .read(replay(&sized), &storage)
        .try_fold(0, |n, b| async move { Ok(n + b.num_rows()) })
        .await?;
    assert_eq!(rows, 200_010);
    assert_eq!(store.take(), groups + uploaded.len());

    // Two columns, by name, in the order asked.
    let projected = Scan::new(&catalog, &t).columns(["s", "k"])?;
    let names: Vec<&str> = projected.schema().fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["s", "k"]);
    let batches: Vec<RecordBatch> = projected.read(replay(&planned), &storage).try_collect().await?;
    assert_eq!(keys(&batches, 1), (0..200_010).collect::<Vec<_>>());
    for b in &batches {
        let (s, k) = (b.column(0).as_string::<i32>(), b.column(1).as_primitive::<Int64Type>());
        for row in 0..b.num_rows() {
            assert_eq!(s.value(row), want_s.value(k.value(row) as usize));
        }
    }
    assert!(matches!(
        Scan::new(&catalog, &t).columns(["nope"]),
        Err(Error::NotFound(_))
    ));

    // No column at all: batches of rows and nothing else, as `count(*)` reads them.
    let none = Scan::new(&catalog, &t).columns(Vec::<&str>::new())?;
    let counted: usize = none
        .read(replay(&planned), &storage)
        .try_fold(0, |n, b| async move { Ok(n + b.num_rows()) })
        .await?;
    assert_eq!(counted, 200_010);
    Ok(())
}

#[tokio::test]
async fn a_range_reads_only_the_files_and_row_groups_that_may_hold_it() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, store, storage) = setup().await?;
    // One file of 40 row groups of 5,000 rows, then one of 2 far beyond it.
    let groups = WriteOptions {
        row_group_rows: 5_000,
        ..Default::default()
    };
    write(&catalog, &storage, vec![batch(0..200_000)], groups.clone()).await?;
    write(&catalog, &storage, vec![batch(1_000_000..1_010_000)], groups).await?;
    let t = table(&catalog).await?;
    let on = &storage;
    let read = |scan: Scan| async move {
        let planned = tasks(&scan).await?;
        let batches: Vec<RecordBatch> = scan.read(replay(&planned), on).try_collect().await?;
        Ok::<_, Error>((planned, batches))
    };

    // Postgres plans the first file only, and the reader reads its one row group that may hold the range.
    store.take();
    let (planned, batches) =
        read(Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(12_000)), Some(Scalar::Long(13_000)))?).await?;
    assert_eq!(planned.len(), 1);
    assert_eq!(row_groups(&planned[0]), 40);
    assert_eq!(keys(&batches, 0), (10_000..15_000).collect::<Vec<_>>());
    assert_eq!(store.take(), 1);

    // A range open above: the last row group of the first file, and the second file whole.
    let (planned, batches) = read(Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(197_000)), None)?).await?;
    assert_eq!(planned.len(), 2);
    let want: Vec<i64> = (195_000..200_000).chain(1_000_000..1_010_000).collect();
    assert_eq!(keys(&batches, 0), want);
    assert_eq!(store.take(), 3);

    // A range nothing holds: no file.
    let (planned, batches) =
        read(Scan::new(&catalog, &t).filter("k", Some(Scalar::Long(300_000)), Some(Scalar::Long(400_000)))?).await?;
    assert!(planned.is_empty() && batches.is_empty());

    // A value that is not the column's, and a column that is not the table's.
    assert!(matches!(
        Scan::new(&catalog, &t).filter("k", Some(Scalar::String("a")), None),
        Err(Error::Schema(_))
    ));
    assert!(matches!(
        Scan::new(&catalog, &t).filter("nope", None, None),
        Err(Error::NotFound(_))
    ));

    // A file whose d is all null: no bounds, in its footer or the catalog, so a range on d reads it whole, with
    // the first file's one row group that may hold d in [0, 1]; the second file's d is far beyond it.
    let mut nulls = batch(3_000_000..3_010_000);
    let empty = arrow_array::new_null_array(&DataType::Float64, nulls.num_rows());
    let mut columns = nulls.columns().to_vec();
    columns[1] = empty;
    nulls = RecordBatch::try_new(nulls.schema(), columns)?;
    write(
        &catalog,
        &storage,
        vec![nulls],
        WriteOptions {
            row_group_rows: 5_000,
            ..Default::default()
        },
    )
    .await?;
    let t = table(&catalog).await?;
    let (planned, batches) =
        read(Scan::new(&catalog, &t).filter("d", Some(Scalar::Double(0.0)), Some(Scalar::Double(1.0)))?).await?;
    assert_eq!(planned.len(), 2);
    let want: Vec<i64> = (0..5_000).chain(3_000_000..3_010_000).collect();
    assert_eq!(keys(&batches, 0), want);

    // A file with a deletion vector is refused, not read without it.
    let scan = Scan::new(&catalog, &t);
    let mut task = tasks(&scan).await?.remove(0);
    task.vector = Some(TaskVector {
        begin_seq: 1,
        path: None,
        offset: None,
        size: 1,
        cardinality: 1,
        inline: Some(vec![0]),
    });
    let refused = scan.open(task, &storage).try_collect::<Vec<_>>().await;
    assert!(matches!(refused, Err(Error::Unsupported(_))), "{refused:?}");
    Ok(())
}
