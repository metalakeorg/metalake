//! A file's columns under the table: by field id, not by name, since a file keeps the names it was written with. A
//! column as the table has it stays a plain column, so DataFusion prunes on it. One that evolved goes through the
//! core's `Evolve`, the one definition of what a read sees, whatever its nesting; a field the file lacks is a constant.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use metalake::Evolve;
use metalake::schema::FIELD_ID;

use crate::catalog::external;

/// Adapts each file of a table to it.
#[derive(Debug)]
pub(crate) struct ByFieldId {
    pub evolve: Arc<Evolve>,
}

impl PhysicalExprAdapterFactory for ByFieldId {
    fn create(&self, logical: SchemaRef, physical: SchemaRef) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(FileAdapter {
            logical,
            physical,
            evolve: self.evolve.clone(),
        }))
    }
}

#[derive(Debug)]
struct FileAdapter {
    logical: SchemaRef,
    physical: SchemaRef,
    evolve: Arc<Evolve>,
}

impl PhysicalExprAdapter for FileAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            let Some(column) = e.downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };
            let Ok(field) = self.logical.field_with_name(column.name()) else {
                return Ok(Transformed::no(e));
            };
            // A column with no field id is none of the table's: a position, which the reader gives, at its place there.
            if !field.metadata().contains_key(FIELD_ID) {
                let at = self.physical.index_of(column.name())?;
                return Ok(Transformed::yes(Arc::new(Column::new(column.name(), at))));
            }
            self.adapted(field).map(Transformed::yes)
        })
        .data()
    }
}

impl FileAdapter {
    fn adapted(&self, field: &Field) -> Result<Arc<dyn PhysicalExpr>> {
        let found = field.metadata().get(FIELD_ID).and_then(|id| {
            let in_file = |f: &FieldRef| f.metadata().get(FIELD_ID) == Some(id);
            self.physical.fields().iter().position(in_file)
        });
        let Some(at) = found else {
            let constant: ArrayRef = self.evolve.column(field, None, 1).map_err(external)?;
            return Ok(Arc::new(Literal::new(ScalarValue::try_from_array(&constant, 0)?)));
        };
        let in_file = self.physical.field(at);
        let column = Arc::new(Column::new(in_file.name(), at));
        if in_file.data_type() == field.data_type() {
            return Ok(column);
        }
        Ok(Arc::new(Evolved {
            column,
            field: Arc::new(field.clone()),
            evolve: self.evolve.clone(),
        }))
    }
}

/// A file's column under the table's field, by the core's rules.
#[derive(Debug)]
struct Evolved {
    column: Arc<dyn PhysicalExpr>,
    field: FieldRef,
    evolve: Arc<Evolve>,
}

impl PartialEq for Evolved {
    fn eq(&self, other: &Self) -> bool {
        self.column.as_ref() == other.column.as_ref() && self.field == other.field
    }
}

impl Eq for Evolved {}

impl Hash for Evolved {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        self.field.hash(state);
    }
}

impl fmt::Display for Evolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "evolved({} as {})", self.column, self.field.name())
    }
}

impl PhysicalExpr for Evolved {
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(self.field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        let file = self.column.evaluate(batch)?.into_array(rows)?;
        let evolved = self.evolve.column(&self.field, Some(&file), rows).map_err(external)?;
        Ok(ColumnarValue::Array(evolved))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.column]
    }

    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            column: children.remove(0),
            field: self.field.clone(),
            evolve: self.evolve.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
