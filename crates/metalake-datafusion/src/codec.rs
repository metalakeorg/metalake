//! MetaLake's codecs: a plan that crosses processes (Ballista's client to its scheduler, the scheduler to its executors)
//! keeps MetaLake's tables as values, rebuilt on the other side with that process's catalog and storage. Nothing is
//! asked of Postgres while a plan is decoded: DataFusion decodes synchronously.
//!
//! Each codec wraps an inner one, which encodes whatever is not MetaLake's. MetaLake's bytes start with `MLK` and a
//! version, so that bytes of another kind go to the inner codec and bytes of an unknown version are refused, not misread.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, Statistics, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::physical_plan::parquet::ParquetRowSelection;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use metalake::{Catalog, Evolve, Ref, RefValue, ScanTask, Storage, Table, TaskVector};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::adapter::ByFieldId;
use crate::exec::MetaScanExec;
use crate::point::StatementPoint;
use crate::read::TaskReaders;
use crate::table::MetaTable;

/// What every MetaLake encoding starts with.
const MAGIC: &[u8; 3] = b"MLK";
/// The version of the encoding this build writes and reads.
const VERSION: u8 = 1;

/// What a MetaLake encoding holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Table = 1,
    Scan = 2,
}

/// `value` as MetaLake's bytes of `kind`, appended to `buf`.
fn encode<T: Serialize>(kind: Kind, value: &T, buf: &mut Vec<u8>) -> Result<()> {
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&[VERSION, kind as u8]);
    let bytes = postcard::to_stdvec(value).map_err(|e| DataFusionError::Internal(format!("MetaLake encoding: {e}")))?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// The value of `kind` in `buf`, if `buf` is MetaLake's; none if it is another codec's.
fn decode<T: DeserializeOwned>(kind: Kind, buf: &[u8]) -> Option<Result<T>> {
    let rest = buf.strip_prefix(MAGIC)?;
    Some(match rest {
        [VERSION, found, bytes @ ..] if *found == kind as u8 => postcard::from_bytes(bytes)
            .map_err(|e| DataFusionError::Internal(format!("MetaLake decoding of a {kind:?}: {e}"))),
        [VERSION, found, ..] => Err(DataFusionError::Internal(format!(
            "MetaLake bytes of kind {found} where a {kind:?} was expected"
        ))),
        [version, ..] => Err(DataFusionError::NotImplemented(format!(
            "MetaLake plan bytes of version {version}; this build reads version {VERSION}"
        ))),
        [] => Err(DataFusionError::Internal("MetaLake bytes without a version".into())),
    })
}

/// A `MetaTable` as it travels: its ref, the table as the read saw it, and the statement whose point it shares.
#[derive(Serialize, Deserialize)]
struct TableBytes {
    on: RefValue,
    table: Table,
    statement: Option<StatementBytes>,
}

#[derive(Serialize, Deserialize)]
struct StatementBytes {
    id: Uuid,
    refs: Vec<i32>,
}

/// The statement points of the plans this process decodes, by id: the tables of one statement share one point again.
/// A point lives as long as a plan holds it.
#[derive(Debug, Default)]
pub(crate) struct Points {
    by_id: Mutex<HashMap<Uuid, Weak<StatementPoint>>>,
}

impl Points {
    /// The point of statement `id`: the one a table of it decoded before holds, or a new one.
    pub(crate) fn shared(&self, id: Uuid, refs: Vec<i32>) -> Arc<StatementPoint> {
        let mut by_id = self.by_id.lock().expect("a point is only looked up or added");
        if let Some(point) = by_id.get(&id).and_then(Weak::upgrade) {
            return point;
        }
        by_id.retain(|_, point| point.strong_count() > 0);
        let point = Arc::new(StatementPoint {
            id,
            refs,
            points: OnceCell::new(),
        });
        by_id.insert(id, Arc::downgrade(&point));
        point
    }
}

/// MetaLake's tables in a logical plan: a `MetaTable` travels as its ref, its table and its statement's point, and is
/// rebuilt with this process's catalog and storage. Everything else is the inner codec's.
pub struct MetaLakeLogicalCodec {
    catalog: Catalog,
    storage: Storage,
    inner: Arc<dyn LogicalExtensionCodec>,
    points: Points,
}

impl fmt::Debug for MetaLakeLogicalCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaLakeLogicalCodec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl MetaLakeLogicalCodec {
    pub(crate) fn new(catalog: Catalog, storage: Storage, inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self {
            catalog,
            storage,
            inner,
            points: Points::default(),
        }
    }
}

impl LogicalExtensionCodec for MetaLakeLogicalCodec {
    fn try_decode(&self, buf: &[u8], inputs: &[LogicalPlan], ctx: &TaskContext) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(decoded) = decode::<TableBytes>(Kind::Table, buf) else {
            return self.inner.try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        let TableBytes { on, table, statement } = decoded?;
        if table.schema().fields() != schema.fields() {
            return Err(DataFusionError::Internal(format!(
                "{table_ref}: the plan's schema is not its table's"
            )));
        }
        Ok(Arc::new(MetaTable {
            on: Ref::from_value(&self.catalog, on),
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            table,
            statement: statement.map(|s| self.points.shared(s.id, s.refs)),
        }))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(table) = node.downcast_ref::<MetaTable>() else {
            return self.inner.try_encode_table_provider(table_ref, node, buf);
        };
        let bytes = TableBytes {
            on: table.on.value(),
            table: table.table.clone(),
            statement: table.statement.as_ref().map(|s| StatementBytes {
                id: s.id,
                refs: s.refs.clone(),
            }),
        };
        encode(Kind::Table, &bytes, buf)
    }

    fn try_decode_file_format(&self, buf: &[u8], ctx: &TaskContext) -> Result<Arc<dyn FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(&self, buf: &mut Vec<u8>, node: Arc<dyn FileFormatFactory>) -> Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(&self, name: &str, buf: &[u8]) -> Result<Arc<HigherOrderUDF>> {
        self.inner.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(&self, node: &HigherOrderUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}

/// A `MetaScanExec` as it travels: DataFusion's own encoding of the scan inside, and what that encoding drops.
#[derive(Serialize, Deserialize)]
struct ScanBytes {
    /// The `DataSourceExec`, as DataFusion encodes it: groups and ranges, predicate, projection, limit, statistics.
    standard: Vec<u8>,
    /// The table read, whose fields match the files' columns by id.
    table: Table,
    /// Each file once, however many ranges of it the groups hold.
    files: Vec<FileBytes>,
}

/// What DataFusion's encoding drops of a file: its task, and the rows its vector leaves.
#[derive(Serialize, Deserialize)]
struct FileBytes {
    /// The path DataFusion's encoding keeps, by which the file is found again.
    path: String,
    task: TaskBytes,
    /// The selection of its rows, each run as `(rows, skip)`.
    selection: Option<Vec<(usize, bool)>>,
}

/// A `ScanTask` as it travels: its partition as JSON text, which a format that does not describe itself carries.
#[derive(Serialize, Deserialize)]
struct TaskBytes {
    file_id: i64,
    path: Option<String>,
    content: Option<Vec<u8>>,
    file_size: i64,
    footer_size: i32,
    footer: Option<Vec<u8>>,
    record_count: i64,
    spec_id: i32,
    partition: String,
    vector: Option<TaskVector>,
}

impl From<&ScanTask> for TaskBytes {
    fn from(task: &ScanTask) -> Self {
        Self {
            file_id: task.file_id,
            path: task.path.clone(),
            content: task.content.clone(),
            file_size: task.file_size,
            footer_size: task.footer_size,
            footer: task.footer.clone(),
            record_count: task.record_count,
            spec_id: task.spec_id,
            partition: task.partition.to_string(),
            vector: task.vector.clone(),
        }
    }
}

impl TaskBytes {
    fn task(&self) -> Result<ScanTask> {
        let partition = serde_json::from_str(&self.partition).map_err(|e| {
            DataFusionError::Internal(format!("file {}: a partition that is not JSON: {e}", self.file_id))
        })?;
        Ok(ScanTask {
            file_id: self.file_id,
            path: self.path.clone(),
            content: self.content.clone(),
            file_size: self.file_size,
            footer_size: self.footer_size,
            footer: self.footer.clone(),
            record_count: self.record_count,
            spec_id: self.spec_id,
            partition,
            vector: self.vector.clone(),
        })
    }
}

/// MetaLake's scans in a physical plan: a `MetaScanExec` travels with each file's task and vector, and is rebuilt with
/// this process's storage, MetaLake's reader and the columns by field id. It needs no catalog: an executor reads
/// without Postgres. Everything else is the inner codec's.
pub struct MetaLakePhysicalCodec {
    storage: Storage,
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl fmt::Debug for MetaLakePhysicalCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaLakePhysicalCodec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl MetaLakePhysicalCodec {
    /// The codec of a process that reads with `storage`; `inner` encodes everything else.
    pub fn new(storage: Storage, inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { storage, inner }
    }

    fn encode_scan(&self, scan: &MetaScanExec, buf: &mut Vec<u8>) -> Result<()> {
        if scan.positions() {
            return Err(DataFusionError::NotImplemented(
                "a scan whose rows say their file and position does not cross processes yet".into(),
            ));
        }
        let config = file_scan(scan.source())?;
        let mut seen = HashSet::new();
        let mut files = Vec::new();
        for file in config.file_groups.iter().flat_map(FileGroup::iter) {
            let path = file.path().to_string();
            if !seen.insert(path.clone()) {
                continue;
            }
            let task = file
                .extension::<Arc<ScanTask>>()
                .ok_or_else(|| DataFusionError::Internal(format!("{path}: a file without its task")))?;
            let selection = file
                .extension::<ParquetRowSelection>()
                .map(|s| s.selection().iter().map(|r| (r.row_count, r.skip)).collect());
            files.push(FileBytes {
                path,
                task: TaskBytes::from(task.as_ref()),
                selection,
            });
        }
        let standard = physical_plan_to_bytes_with_extension_codec(Arc::clone(scan.inner()), self.inner.as_ref())?;
        let bytes = ScanBytes {
            standard: standard.to_vec(),
            table: scan.table().as_ref().clone(),
            files,
        };
        encode(Kind::Scan, &bytes, buf)
    }

    fn decode_scan(&self, bytes: ScanBytes, ctx: &TaskContext) -> Result<Arc<dyn ExecutionPlan>> {
        let ScanBytes { standard, table, files } = bytes;
        // DataFusion's decoding of a Parquet scan looks its store up in the runtime before MetaLake's reader replaces
        // its own: a process with storage and nothing registered (an executor) registers the lake's here.
        let url = ObjectStoreUrl::parse(self.storage.base())?;
        let runtime = ctx.runtime_env();
        if runtime.object_store(&url).is_err() {
            runtime.register_object_store(url.as_ref(), self.storage.store().clone());
        }
        let plan = physical_plan_from_bytes_with_extension_codec(&standard, ctx, self.inner.as_ref())?;
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .ok_or_else(|| DataFusionError::Internal("a MetaLake scan whose inside is not a DataSourceExec".into()))?;
        let config = file_scan(source)?.clone();
        let parquet = config
            .file_source()
            .downcast_ref::<ParquetSource>()
            .ok_or_else(|| DataFusionError::Internal("a MetaLake scan whose source is not Parquet".into()))?
            .clone()
            .with_parquet_file_reader_factory(Arc::new(TaskReaders {
                storage: self.storage.clone(),
            }));
        let by_path: HashMap<String, FileBytes> = files.into_iter().map(|f| (f.path.clone(), f)).collect();
        let groups = config
            .file_groups
            .iter()
            .map(|group| {
                let files = group
                    .iter()
                    .map(|file| {
                        let path = file.path().to_string();
                        let found = by_path
                            .get(&path)
                            .ok_or_else(|| DataFusionError::Internal(format!("{path}: a file without its task")))?;
                        let mut file = file.clone();
                        if let Some(runs) = &found.selection {
                            let selectors: Vec<RowSelector> = runs
                                .iter()
                                .map(|&(rows, skip)| match skip {
                                    true => RowSelector::skip(rows),
                                    false => RowSelector::select(rows),
                                })
                                .collect();
                            file = file.with_extension(ParquetRowSelection::new(RowSelection::from(selectors)));
                        }
                        Ok(file.with_extension(Arc::new(found.task.task()?)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                // DataFusion's encoding keeps each file's statistics and drops the group's: merged again, as the read
                // that planned it merged them. A range reads part of its file, whose statistics are the whole file's:
                // a group with one is unknown, as DataFusion leaves the groups it splits.
                let whole = files.iter().all(|f| f.range.is_none());
                let statistics: Option<Vec<Statistics>> = whole
                    .then(|| files.iter().map(|f| f.statistics.as_deref().cloned()).collect())
                    .flatten();
                let rebuilt = FileGroup::new(files);
                Ok(match statistics {
                    Some(statistics) => {
                        let merged = Statistics::try_merge_iter(&statistics, table.schema())?;
                        rebuilt.with_statistics(Arc::new(merged))
                    }
                    None => rebuilt,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let evolve = Arc::new(Evolve::new(&table));
        let config = FileScanConfigBuilder::from(config)
            .with_source(Arc::new(parquet))
            .with_file_groups(groups)
            .with_expr_adapter(Some(Arc::new(ByFieldId { evolve })))
            .build();
        Ok(Arc::new(MetaScanExec::new(
            DataSourceExec::from_data_source(config),
            Arc::new(table),
            false,
        )))
    }
}

/// The file scan a `DataSourceExec` of MetaLake's reads.
fn file_scan(source: &DataSourceExec) -> Result<&FileScanConfig> {
    source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| DataFusionError::Internal("a MetaLake scan that is not a file scan".into()))
}

impl PhysicalExtensionCodec for MetaLakePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match decode::<ScanBytes>(Kind::Scan, buf) {
            Some(bytes) => self.decode_scan(bytes?, ctx),
            None => self.inner.try_decode(buf, inputs, ctx, proto_converter),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        match node.downcast_ref::<MetaScanExec>() {
            Some(scan) => self.encode_scan(scan, buf),
            None => self.inner.try_encode(node, buf, proto_converter),
        }
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(&self, name: &str, buf: &[u8]) -> Result<Arc<HigherOrderUDF>> {
        self.inner.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(&self, node: &HigherOrderUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.try_decode_expr(buf, inputs, ctx)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        self.inner.try_encode_expr(node, buf, ctx)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;

    use super::*;

    #[test]
    fn points_shared_by_id() {
        let points = Points::default();
        let (one, two) = (Uuid::new_v4(), Uuid::new_v4());
        let a = points.shared(one, vec![1]);
        assert!(Arc::ptr_eq(&a, &points.shared(one, vec![1])));
        assert!(!Arc::ptr_eq(&a, &points.shared(two, vec![1])));
        let id = a.id;
        drop(a);
        // No plan holds it: a later plan of the same id starts a point of its own.
        assert_eq!(points.shared(id, vec![1]).id, id);
    }

    #[test]
    fn decode_foreign_bytes_none() {
        assert!(decode::<StatementBytes>(Kind::Table, b"not ours").is_none());
    }

    #[test]
    fn decode_unknown_version_refused() {
        let mut buf = MAGIC.to_vec();
        buf.extend_from_slice(&[VERSION + 1, Kind::Table as u8]);
        let refused = decode::<StatementBytes>(Kind::Table, &buf).unwrap();
        assert!(matches!(refused, Err(DataFusionError::NotImplemented(_))));
    }

    /// A scan whose rows say their file and position, for a statement that goes on to write.
    fn positioned_scan() -> MetaScanExec {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let definition = serde_json::json!({
            "table_id": "6f1c2a3e-1b2c-4d5e-8f90-a1b2c3d4e5f6", "namespace": ["db"], "name": "t",
            "location": "memory:///wh/db/t",
            "fields": [{"field_id": 1, "position": 0, "name": "k", "type": "long", "required": true}],
        });
        let described = serde_json::json!({"ref_id": 1, "seq": 1, "definition": definition.to_string()});
        let table: Table = serde_json::from_value(described).unwrap();
        let source = ParquetSource::new(TableSchema::builder(table.schema().clone()).build());
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source)).build();
        MetaScanExec::new(DataSourceExec::from_data_source(config), Arc::new(table), true)
    }

    #[test]
    fn encode_scan_with_positions_refused() {
        let codec = MetaLakePhysicalCodec::new(Storage::in_memory(), Arc::new(DefaultPhysicalExtensionCodec {}));
        let refused = codec.encode_scan(&positioned_scan(), &mut Vec::new()).unwrap_err();
        assert!(matches!(refused, DataFusionError::NotImplemented(_)), "{refused}");
    }

    #[test]
    fn statement_round_trip() {
        let mut buf = Vec::new();
        let id = Uuid::new_v4();
        encode(Kind::Table, &StatementBytes { id, refs: vec![1, 2] }, &mut buf).unwrap();
        let back = decode::<StatementBytes>(Kind::Table, &buf).unwrap().unwrap();
        assert_eq!((back.id, back.refs), (id, vec![1, 2]));
    }
}
