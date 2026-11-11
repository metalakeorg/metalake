//! MetaLake's codecs: a plan that crosses processes (Ballista's client to its scheduler, the scheduler to its executors)
//! keeps MetaLake's tables as values, rebuilt on the other side with that process's catalog and storage. Nothing is
//! asked of Postgres while a plan is decoded: DataFusion decodes synchronously.
//!
//! Each codec wraps an inner one, which encodes whatever is not MetaLake's. MetaLake's bytes start with `MLK` and a
//! version, so that bytes of another kind go to the inner codec and bytes of an unknown version are refused, not misread.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use metalake::{Catalog, Ref, RefValue, Storage, Table};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::point::StatementPoint;
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

#[cfg(test)]
mod tests {
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

    #[test]
    fn statement_round_trip() {
        let mut buf = Vec::new();
        let id = Uuid::new_v4();
        encode(Kind::Table, &StatementBytes { id, refs: vec![1, 2] }, &mut buf).unwrap();
        let back = decode::<StatementBytes>(Kind::Table, &buf).unwrap().unwrap();
        assert_eq!((back.id, back.refs), (id, vec![1, 2]));
    }
}
