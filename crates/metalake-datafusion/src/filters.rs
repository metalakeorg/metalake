//! What of a query's filters Postgres can use: a range on the table's first cluster key, the one its planning indexes
//! (STA-3). Every filter stays with DataFusion, which applies it again; this only leaves files out of the plan.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use metalake::partition::Scalar;
use metalake::{Result, Scan, Table};

/// `scan`, with the range `filters` set on `table`'s first cluster key, if they set one.
pub(crate) fn pruned(scan: Scan, table: &Table, filters: &[Expr]) -> Result<Scan> {
    let key = table.cluster_key().first().and_then(|id| {
        let field = table.fields().iter().find(|f| f.field_id == *id)?;
        field.parent_field_id.is_none().then_some(&field.name)
    });
    let Some(key) = key else {
        return Ok(scan);
    };
    let (mut lo, mut hi) = (None, None);
    for filter in filters {
        let Some((operator, value)) = bound(filter, key) else {
            continue;
        };
        match operator {
            Operator::Eq => (lo, hi) = (Some(value), Some(value)),
            Operator::Gt | Operator::GtEq => lo = lo.or(Some(value)),
            _ => hi = hi.or(Some(value)),
        }
    }
    if lo.is_none() && hi.is_none() {
        return Ok(scan);
    }
    scan.filter(key, lo, hi)
}

/// The comparison `filter` makes of column `key` with a literal, as `key <operator> value`.
fn bound<'a>(filter: &'a Expr, key: &str) -> Option<(Operator, Scalar<'a>)> {
    // DataFusion's simplifier has put the column on the left; a filter it did not is left out, which costs only files.
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
        return None;
    };
    let (Expr::Column(column), Expr::Literal(value, _)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    let in_range = matches!(
        op,
        Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    );
    (in_range && column.name == key).then_some(())?;
    Some((*op, scalar(value)?))
}

fn scalar(value: &ScalarValue) -> Option<Scalar<'_>> {
    Some(match value {
        ScalarValue::Int8(Some(v)) => Scalar::Long(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Scalar::Long(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Scalar::Long(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Scalar::Long(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::Utf8View(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            Scalar::String(v)
        }
        _ => return None,
    })
}
