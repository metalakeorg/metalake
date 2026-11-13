//! MetaLake's codecs: a plan that crosses processes (Ballista's client to its scheduler, the scheduler to its executors)
//! keeps MetaLake's tables as values, rebuilt on the other side with that process's catalog and storage. Nothing is
//! asked of Postgres while a plan is decoded: DataFusion decodes synchronously.
//!
//! Each codec wraps an inner one, which encodes whatever is not MetaLake's. MetaLake's bytes start with `MLK` and a
//! version, so that bytes of another kind go to the inner codec and bytes of an unknown version are refused, not misread.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, DataFusionError, Result, Statistics, TableReference, internal_err};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::physical_plan::parquet::ParquetRowSelection;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::{AggregateUDF, Expr, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    Serializeable, physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::DfSchema;
use metalake::{
    Catalog, Evolve, Ref, RefValue, RowChange, ScanTask, Storage, StreamTask, Table, TableChanges, TaskRows, TaskVector,
};
use prost::Message;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::adapter::ByFieldId;
use crate::changes::{ChangesExec, ChangesTable};
use crate::source::{self, MetaLakeSource};
use crate::merge::Merge;
use crate::point::StatementPoint;
use crate::read::TaskReaders;
use crate::sink::{Delete, Insert, Selection, Update, session};
use crate::table::MetaTable;
use crate::written::MetaWriteExec;

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
    Insert = 3,
    Delete = 4,
    Update = 5,
    Merge = 6,
    Changes = 7,
    ChangesScan = 8,
    /// A write's row around its sink, which is encoded as its child.
    Written = 9,
}

/// `value` as MetaLake's bytes of `kind`, appended to `buf`.
fn encode<T: Serialize>(kind: Kind, value: &T, buf: &mut Vec<u8>) -> Result<()> {
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&[VERSION, kind as u8]);
    let bytes = postcard::to_stdvec(value).map_err(|e| DataFusionError::Internal(format!("MetaLake encoding: {e}")))?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// The kind `buf` holds, if it is MetaLake's bytes; none if it is another codec's.
fn kind_of(buf: &[u8]) -> Option<u8> {
    buf.strip_prefix(MAGIC)?.get(1).copied()
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

/// A `MetaTable` again, in the process that decoded it: its catalog and storage.
fn rebuilt(
    catalog: &Catalog,
    storage: &Storage,
    on: RefValue,
    table: Table,
    statement: Option<Arc<StatementPoint>>,
) -> MetaTable {
    MetaTable {
        on: Ref::from_value(catalog, on),
        catalog: catalog.clone(),
        storage: storage.clone(),
        table,
        statement,
    }
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
        if kind_of(buf) == Some(Kind::Changes as u8) {
            let bytes = decode::<ChangesBytes>(Kind::Changes, buf).expect("MetaLake's bytes")?;
            let changes = ChangesTable {
                changes: bytes.changes()?,
                storage: self.storage.clone(),
            };
            return Ok(Arc::new(changes));
        }
        let Some(decoded) = decode::<TableBytes>(Kind::Table, buf) else {
            return self.inner.try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        let TableBytes { on, table, statement } = decoded?;
        if table.schema().fields() != schema.fields() {
            return Err(DataFusionError::Internal(format!(
                "{table_ref}: the plan's schema is not its table's"
            )));
        }
        let statement = statement.map(|s| self.points.shared(s.id, s.refs));
        Ok(Arc::new(rebuilt(&self.catalog, &self.storage, on, table, statement)))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(changes) = node.downcast_ref::<ChangesTable>() {
            return encode(Kind::Changes, &ChangesBytes::of(&changes.changes), buf);
        }
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

/// An `insert`'s sink as it travels: the table it writes and whether it overwrites. Its rows are its input's, which
/// DataFusion encodes as the node's child.
#[derive(Serialize, Deserialize)]
struct InsertBytes {
    on: RefValue,
    table: Table,
    overwrite: bool,
}

/// The rows a `delete` or an `update` acts on, as they travel: the table, and the statement's filters, from which the
/// process that runs it makes the core's scan, its pruning and its filter again, as the one that planned it did.
#[derive(Serialize, Deserialize)]
struct RowsBytes {
    on: RefValue,
    table: Table,
    /// Each a logical expression, in DataFusion's encoding.
    filters: Vec<Vec<u8>>,
}

/// An `update` as it travels: its rows, and each assigned column's name and new value, a logical expression.
#[derive(Serialize, Deserialize)]
struct UpdateBytes {
    rows: RowsBytes,
    assignments: Vec<(String, Vec<u8>)>,
}

/// A `merge into` as it travels: its statement, from which the process that runs it plans the join and makes its
/// clauses, as the one that planned it did. The source is a plan, in this codec's encoding, so that MetaLake's scans in it
/// keep their tasks; the combined schema is DataFusion's protobuf; expressions are logical, in DataFusion's encoding.
#[derive(Serialize, Deserialize)]
struct MergeBytes {
    on: RefValue,
    table: Table,
    source: Vec<u8>,
    combined: Vec<u8>,
    join_on: Vec<u8>,
    clauses: Vec<ClauseBytes>,
}

#[derive(Serialize, Deserialize)]
struct ClauseBytes {
    kind: ClauseKind,
    predicate: Option<Vec<u8>>,
    action: ActionBytes,
}

#[derive(Serialize, Deserialize)]
enum ClauseKind {
    Matched,
    NotMatched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

#[derive(Serialize, Deserialize)]
enum ActionBytes {
    Update(Vec<(String, Vec<u8>)>),
    Insert { columns: Vec<String>, values: Vec<Vec<u8>> },
    Delete,
}

fn expr_bytes(expr: &Expr) -> Result<Vec<u8>> {
    Ok(expr.to_bytes()?.to_vec())
}

impl ClauseBytes {
    fn of(clause: &MergeIntoClause) -> Result<Self> {
        let kind = match clause.kind {
            MergeIntoClauseKind::Matched => ClauseKind::Matched,
            MergeIntoClauseKind::NotMatched => ClauseKind::NotMatched,
            MergeIntoClauseKind::NotMatchedByTarget => ClauseKind::NotMatchedByTarget,
            MergeIntoClauseKind::NotMatchedBySource => ClauseKind::NotMatchedBySource,
        };
        let action = match &clause.action {
            MergeIntoAction::Update(assignments) => ActionBytes::Update(
                assignments
                    .iter()
                    .map(|(column, value)| Ok((column.clone(), expr_bytes(value)?)))
                    .collect::<Result<_>>()?,
            ),
            MergeIntoAction::Insert { columns, values } => ActionBytes::Insert {
                columns: columns.clone(),
                values: values.iter().map(expr_bytes).collect::<Result<_>>()?,
            },
            MergeIntoAction::Delete => ActionBytes::Delete,
        };
        Ok(Self {
            kind,
            predicate: clause.predicate.as_ref().map(expr_bytes).transpose()?,
            action,
        })
    }

    fn clause(&self, ctx: &TaskContext) -> Result<MergeIntoClause> {
        let expr = |bytes: &Vec<u8>| Expr::from_bytes_with_ctx(bytes, ctx);
        let kind = match self.kind {
            ClauseKind::Matched => MergeIntoClauseKind::Matched,
            ClauseKind::NotMatched => MergeIntoClauseKind::NotMatched,
            ClauseKind::NotMatchedByTarget => MergeIntoClauseKind::NotMatchedByTarget,
            ClauseKind::NotMatchedBySource => MergeIntoClauseKind::NotMatchedBySource,
        };
        let action = match &self.action {
            ActionBytes::Update(assignments) => MergeIntoAction::Update(
                assignments
                    .iter()
                    .map(|(column, value)| Ok((column.clone(), expr(value)?)))
                    .collect::<Result<_>>()?,
            ),
            ActionBytes::Insert { columns, values } => MergeIntoAction::Insert {
                columns: columns.clone(),
                values: values.iter().map(expr).collect::<Result<_>>()?,
            },
            ActionBytes::Delete => MergeIntoAction::Delete,
        };
        Ok(MergeIntoClause {
            kind,
            predicate: self.predicate.as_ref().map(expr).transpose()?,
            action,
        })
    }
}

/// The changes of a range of commits as they travel: the table and the tasks, all that reading them takes.
#[derive(Serialize, Deserialize)]
struct ChangesBytes {
    table: Table,
    tasks: Vec<StreamTaskBytes>,
}

/// A `StreamTask` as it travels: its file a `TaskBytes`, whose partition is JSON text.
#[derive(Serialize, Deserialize)]
struct StreamTaskBytes {
    seq: i64,
    committed_at: SystemTime,
    change: RowChange,
    rows: TaskRows,
    file: TaskBytes,
}

impl ChangesBytes {
    fn of(changes: &TableChanges) -> Self {
        Self {
            table: changes.table().clone(),
            tasks: changes
                .tasks()
                .iter()
                .map(|task| StreamTaskBytes {
                    seq: task.seq,
                    committed_at: task.committed_at,
                    change: task.change,
                    rows: task.rows.clone(),
                    file: TaskBytes::from(&task.file),
                })
                .collect(),
        }
    }

    fn changes(self) -> Result<TableChanges> {
        let tasks = self
            .tasks
            .into_iter()
            .map(|task| {
                Ok(StreamTask {
                    seq: task.seq,
                    committed_at: task.committed_at,
                    change: task.change,
                    rows: task.rows,
                    file: task.file.task()?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(TableChanges::new(self.table, tasks))
    }
}

/// A `ChangesExec` as it travels: its changes, and the columns it reads.
#[derive(Serialize, Deserialize)]
struct ChangesScanBytes {
    changes: ChangesBytes,
    projection: Option<Vec<usize>>,
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
    /// A process that commits has one: a write's sink commits where it runs.
    catalog: Option<Catalog>,
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
        Self {
            storage,
            catalog: None,
            inner,
        }
    }

    /// The codec of a process that also commits, with `catalog`: it decodes writes, whose sinks commit where they run.
    pub fn with_catalog(mut self, catalog: Catalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// The catalog a write's sink commits with, which a process that only reads lacks.
    fn writer(&self) -> Result<&Catalog> {
        self.catalog.as_ref().ok_or_else(|| {
            DataFusionError::Plan("a write commits where it runs, and this process has no catalog".into())
        })
    }

    fn encode_rows(rows: &Selection) -> Result<RowsBytes> {
        Ok(RowsBytes {
            on: rows.table.on.value(),
            table: rows.table.table.clone(),
            filters: rows
                .filters
                .iter()
                .map(|f| Ok(f.to_bytes()?.to_vec()))
                .collect::<Result<_>>()?,
        })
    }

    /// The rows again, made as the process that planned them made them, in a session of `ctx`'s configuration.
    fn decode_rows(&self, bytes: RowsBytes, state: &SessionState, ctx: &TaskContext) -> Result<Selection> {
        let table = rebuilt(self.writer()?, &self.storage, bytes.on, bytes.table, None);
        let filters = bytes
            .filters
            .iter()
            .map(|f| Expr::from_bytes_with_ctx(f, ctx))
            .collect::<Result<_>>()?;
        Selection::new(table, filters, state)
    }

    fn encode_insert(&self, insert: &Insert, buf: &mut Vec<u8>) -> Result<()> {
        let bytes = InsertBytes {
            on: insert.table.on.value(),
            table: insert.table.table.clone(),
            overwrite: insert.overwrite,
        };
        encode(Kind::Insert, &bytes, buf)
    }

    fn decode_insert(&self, bytes: InsertBytes, inputs: &[Arc<dyn ExecutionPlan>]) -> Result<Arc<dyn ExecutionPlan>> {
        let catalog = self.writer()?;
        let [input] = inputs else {
            return Err(DataFusionError::Internal(format!(
                "an insert with {} inputs",
                inputs.len()
            )));
        };
        let sink = Insert {
            table: rebuilt(catalog, &self.storage, bytes.on, bytes.table, None),
            overwrite: bytes.overwrite,
        };
        Ok(Arc::new(DataSinkExec::new(Arc::clone(input), Arc::new(sink), None)))
    }

    fn encode_delete(&self, delete: &Delete, buf: &mut Vec<u8>) -> Result<()> {
        encode(Kind::Delete, &Self::encode_rows(&delete.rows)?, buf)
    }

    fn encode_update(&self, update: &Update, buf: &mut Vec<u8>) -> Result<()> {
        let bytes = UpdateBytes {
            rows: Self::encode_rows(&update.rows)?,
            assignments: update
                .statement
                .iter()
                .map(|(column, value)| Ok((column.clone(), value.to_bytes()?.to_vec())))
                .collect::<Result<_>>()?,
        };
        encode(Kind::Update, &bytes, buf)
    }

    /// A delete's or an update's sink over `inputs`, which DataFusion gives it none of: the rows it finds itself.
    fn sink_over(inputs: &[Arc<dyn ExecutionPlan>], sink: Arc<dyn DataSink>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = inputs else {
            return Err(DataFusionError::Internal(format!(
                "a write with {} inputs",
                inputs.len()
            )));
        };
        Ok(Arc::new(DataSinkExec::new(Arc::clone(input), sink, None)))
    }

    fn decode_delete(
        &self,
        bytes: RowsBytes,
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rows = self.decode_rows(bytes, &session(ctx), ctx)?;
        Self::sink_over(inputs, Arc::new(Delete { rows }))
    }

    fn decode_update(
        &self,
        bytes: UpdateBytes,
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = session(ctx);
        let rows = self.decode_rows(bytes.rows, &state, ctx)?;
        let assignments = bytes
            .assignments
            .iter()
            .map(|(column, value)| Ok((column.clone(), Expr::from_bytes_with_ctx(value, ctx)?)))
            .collect::<Result<_>>()?;
        Self::sink_over(inputs, Arc::new(Update::new(rows, assignments, &state)?))
    }

    fn encode_merge(&self, merge: &Merge, buf: &mut Vec<u8>) -> Result<()> {
        let combined = DfSchema::try_from(merge.combined.as_ref())
            .map_err(|e| DataFusionError::Internal(format!("a merge's schema: {e}")))?;
        let bytes = MergeBytes {
            on: merge.table.on.value(),
            table: merge.table.table.clone(),
            source: physical_plan_to_bytes_with_extension_codec(Arc::clone(&merge.source), self)?.to_vec(),
            combined: combined.encode_to_vec(),
            join_on: expr_bytes(&merge.on)?,
            clauses: merge.statement.iter().map(ClauseBytes::of).collect::<Result<_>>()?,
        };
        encode(Kind::Merge, &bytes, buf)
    }

    fn decode_merge(
        &self,
        bytes: MergeBytes,
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = rebuilt(self.writer()?, &self.storage, bytes.on, bytes.table, None);
        let source = physical_plan_from_bytes_with_extension_codec(&bytes.source, ctx, self)?;
        let combined = DfSchema::decode(bytes.combined.as_slice())
            .map_err(|e| DataFusionError::Internal(format!("a merge's schema: {e}")))?;
        let combined =
            DFSchema::try_from(&combined).map_err(|e| DataFusionError::Internal(format!("a merge's schema: {e}")))?;
        let on = Expr::from_bytes_with_ctx(&bytes.join_on, ctx)?;
        let clauses = bytes.clauses.iter().map(|c| c.clause(ctx)).collect::<Result<_>>()?;
        let merge = Merge::new(table, source, Arc::new(combined), on, clauses, &session(ctx))?;
        Self::sink_over(inputs, Arc::new(merge))
    }

    fn encode_scan(&self, config: &FileScanConfig, scan: &MetaLakeSource, buf: &mut Vec<u8>) -> Result<()> {
        if scan.positions() {
            return Err(DataFusionError::NotImplemented(
                "a scan whose rows say their file and position does not cross processes yet".into(),
            ));
        }
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
        // DataFusion's own encoding of the scan, over the Parquet source inside: groups and ranges, predicate,
        // projection, limit, statistics.
        let parquet = FileScanConfigBuilder::from(config.clone()).with_source(Arc::clone(scan.parquet())).build();
        let parquet: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(parquet);
        let standard = physical_plan_to_bytes_with_extension_codec(parquet, self.inner.as_ref())?;
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
        let source = MetaLakeSource::new(Arc::new(parquet), Arc::new(table), false);
        let config = FileScanConfigBuilder::from(config)
            .with_source(Arc::new(source))
            .with_file_groups(groups)
            .with_expr_adapter(Some(Arc::new(ByFieldId { evolve })))
            .build();
        Ok(DataSourceExec::from_data_source(config))
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
        match kind_of(buf) {
            Some(k) if k == Kind::Insert as u8 => {
                let bytes = decode::<InsertBytes>(Kind::Insert, buf).expect("MetaLake's bytes")?;
                self.decode_insert(bytes, inputs)
            }
            Some(k) if k == Kind::Delete as u8 => {
                let bytes = decode::<RowsBytes>(Kind::Delete, buf).expect("MetaLake's bytes")?;
                self.decode_delete(bytes, inputs, ctx)
            }
            Some(k) if k == Kind::ChangesScan as u8 => {
                let bytes = decode::<ChangesScanBytes>(Kind::ChangesScan, buf).expect("MetaLake's bytes")?;
                let exec = ChangesExec::new(bytes.changes.changes()?, self.storage.clone(), bytes.projection)?;
                Ok(Arc::new(exec))
            }
            Some(k) if k == Kind::Merge as u8 => {
                let bytes = decode::<MergeBytes>(Kind::Merge, buf).expect("MetaLake's bytes")?;
                self.decode_merge(bytes, inputs, ctx)
            }
            Some(k) if k == Kind::Written as u8 => {
                decode::<()>(Kind::Written, buf).expect("MetaLake's bytes")?;
                let [sink] = inputs else {
                    return internal_err!("a write's row wraps one sink, not {}", inputs.len());
                };
                Ok(Arc::new(MetaWriteExec::new(Arc::clone(sink))))
            }
            Some(k) if k == Kind::Update as u8 => {
                let bytes = decode::<UpdateBytes>(Kind::Update, buf).expect("MetaLake's bytes")?;
                self.decode_update(bytes, inputs, ctx)
            }
            Some(_) => {
                let bytes = decode::<ScanBytes>(Kind::Scan, buf).expect("MetaLake's bytes")?;
                self.decode_scan(bytes, ctx)
            }
            None => self.inner.try_decode(buf, inputs, ctx, proto_converter),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Ok(config) = file_scan(exec)
            && let Some(scan) = source::of(config)
        {
            return self.encode_scan(config, scan, buf);
        }
        if node.downcast_ref::<MetaWriteExec>().is_some() {
            return encode(Kind::Written, &(), buf);
        }
        if let Some(changes) = node.downcast_ref::<ChangesExec>() {
            let bytes = ChangesScanBytes {
                changes: ChangesBytes::of(&changes.changes),
                projection: changes.projection.clone(),
            };
            return encode(Kind::ChangesScan, &bytes, buf);
        }
        if let Some(sink) = node.downcast_ref::<DataSinkExec>().map(DataSinkExec::sink) {
            if let Some(insert) = sink.downcast_ref::<Insert>() {
                return self.encode_insert(insert, buf);
            }
            if let Some(delete) = sink.downcast_ref::<Delete>() {
                return self.encode_delete(delete, buf);
            }
            if let Some(update) = sink.downcast_ref::<Update>() {
                return self.encode_update(update, buf);
            }
            if let Some(merge) = sink.downcast_ref::<Merge>() {
                return self.encode_merge(merge, buf);
            }
        }
        self.inner.try_encode(node, buf, proto_converter)
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
    fn positioned_scan() -> FileScanConfig {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let definition = serde_json::json!({
            "table_id": "6f1c2a3e-1b2c-4d5e-8f90-a1b2c3d4e5f6", "namespace": ["db"], "name": "t",
            "location": "memory:///wh/db/t",
            "fields": [{"field_id": 1, "position": 0, "name": "k", "type": "long", "required": true}],
        });
        let described = serde_json::json!({"ref_id": 1, "seq": 1, "definition": definition.to_string()});
        let table: Table = serde_json::from_value(described).unwrap();
        let parquet = ParquetSource::new(TableSchema::builder(table.schema().clone()).build());
        let source = MetaLakeSource::new(Arc::new(parquet), Arc::new(table), true);
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source)).build()
    }

    #[test]
    fn encode_scan_with_positions_refused() {
        let codec = MetaLakePhysicalCodec::new(Storage::in_memory(), Arc::new(DefaultPhysicalExtensionCodec {}));
        let config = positioned_scan();
        let scan = source::of(&config).expect("a MetaLake source");
        let refused = codec.encode_scan(&config, scan, &mut Vec::new()).unwrap_err();
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
