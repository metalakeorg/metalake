//! Changes of a table's definition, as SQL's `alter table` asks them, put in the protocol's shape against the table as a
//! read saw it (§6, §7): a column added under an id drawn from the table's counter (ID-5), renamed or retyped under its
//! own (TYP-3), made optional, or dropped; and the table renamed (TBL-1). What the format does not allow is refused here,
//! with its rule, before the protocol would; the protocol checks it again at commit (CMT-7, CMT-9).

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::DataType;
use serde_json::{Map, Value, json};

use crate::error::{Error, Result};
use crate::schema::spec_type;
use crate::table::{Field, Table};

/// One change of a table's definition. Columns are the table's top-level fields, by name.
#[derive(Debug, Clone, PartialEq)]
pub enum TableChange {
    /// A new column, last. A required one needs a default, which rows written before it read (TYP-5).
    AddColumn {
        name: String,
        ty: DataType,
        required: bool,
        /// Its value in Iceberg's single-value JSON: the rows written before have it, and so do writes that omit it.
        default: Option<Value>,
        doc: Option<String>,
    },
    DropColumn(String),
    RenameColumn {
        from: String,
        to: String,
    },
    /// A wider type, as Iceberg allows one (TYP-3): `int` to `long`, `float` to `double`, `date` to a timestamp, a
    /// decimal to a greater precision of the same scale.
    Widen {
        column: String,
        ty: DataType,
    },
    /// The column made optional: a required one may hold nulls from then on.
    DropNotNull(String),
    /// The table's new namespace and name.
    Rename {
        namespace: Vec<String>,
        name: String,
    },
}

/// The protocol's definition change of `changes` to `table`, drawing a new column's id from `next_id`.
pub(crate) async fn definition<F, Fut>(table: &Table, changes: &[TableChange], mut next_id: F) -> Result<Value>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<i32>>,
{
    let mut fields: BTreeMap<i32, Field> = table.fields().iter().map(|f| (f.field_id, f.clone())).collect();
    let (mut added, mut changed, mut dropped) = (BTreeSet::new(), BTreeSet::new(), Vec::new());
    let mut definition = Map::new();
    for change in changes {
        match change {
            TableChange::AddColumn {
                name,
                ty,
                required,
                default,
                doc,
            } => {
                if column(&fields, name).is_ok() {
                    return Err(Error::Invalid(format!("a column named {name} exists")));
                }
                if *required && default.as_ref().is_none_or(Value::is_null) {
                    return Err(Error::Invalid(format!(
                        "{name}: a required column added to a table needs a default, which the rows already written \
                         read (TYP-5)"
                    )));
                }
                let ty = spec_type(ty).map_err(|e| Error::Invalid(format!("{name}: {e}")))?;
                let position = fields
                    .values()
                    .filter(|f| f.parent_field_id.is_none())
                    .map(|f| f.position)
                    .max()
                    .map_or(0, |p| p + 1);
                let id = next_id().await?;
                fields.insert(
                    id,
                    Field {
                        field_id: id,
                        parent_field_id: None,
                        position,
                        name: name.clone(),
                        ty,
                        required: *required,
                        doc: doc.clone(),
                        initial_default: default.clone(),
                        write_default: default.clone(),
                    },
                );
                added.insert(id);
            }
            TableChange::DropColumn(name) => {
                let id = column(&fields, name)?;
                fields.remove(&id);
                // A column added by these same changes is never written.
                if !added.remove(&id) {
                    changed.remove(&id);
                    dropped.push(id);
                }
            }
            TableChange::RenameColumn { from, to } => {
                let id = column(&fields, from)?;
                if column(&fields, to).is_ok() {
                    return Err(Error::Invalid(format!("a column named {to} exists")));
                }
                fields.get_mut(&id).expect("found").name = to.clone();
                changed.insert(id);
            }
            TableChange::Widen { column: name, ty } => {
                let id = column(&fields, name)?;
                let to = spec_type(ty).map_err(|e| Error::Invalid(format!("{name}: {e}")))?;
                let field = fields.get_mut(&id).expect("found");
                if !widens(&field.ty, &to) {
                    return Err(Error::Invalid(format!(
                        "{name} cannot change from {} to {to}: a type only widens as Iceberg allows (TYP-3)",
                        field.ty
                    )));
                }
                field.ty = to;
                changed.insert(id);
            }
            TableChange::DropNotNull(name) => {
                let id = column(&fields, name)?;
                fields.get_mut(&id).expect("found").required = false;
                changed.insert(id);
            }
            TableChange::Rename { namespace, name } => {
                definition.insert("rename".into(), json!({"namespace": namespace, "name": name}));
            }
        }
    }
    let rows: Vec<Value> = added
        .iter()
        .chain(changed.iter().filter(|id| !added.contains(id)))
        .map(|id| row(&fields[id], added.contains(id)))
        .collect();
    if !rows.is_empty() {
        definition.insert("fields".into(), Value::Array(rows));
    }
    if !dropped.is_empty() {
        definition.insert("drop_fields".into(), json!(dropped));
    }
    Ok(Value::Object(definition))
}

/// The id of the column `name`.
fn column(fields: &BTreeMap<i32, Field>, name: &str) -> Result<i32> {
    fields
        .values()
        .find(|f| f.parent_field_id.is_none() && f.name == name)
        .map(|f| f.field_id)
        .ok_or_else(|| Error::NotFound(format!("column {name}")))
}

/// A field as the protocol takes it: whole, a new one marked.
fn row(f: &Field, new: bool) -> Value {
    let mut row = json!({
        "field_id": f.field_id, "parent_field_id": f.parent_field_id, "position": f.position, "name": f.name,
        "type": f.ty, "required": f.required, "doc": f.doc,
        "initial_default": f.initial_default, "write_default": f.write_default,
    });
    if new {
        row["new"] = json!(true);
    }
    row
}

/// Whether a field of type `from` may take type `to` (TYP-3).
pub(crate) fn widens(from: &str, to: &str) -> bool {
    let decimal = |ty: &str| -> Option<(u32, u32)> {
        let (p, s) = ty.strip_prefix("decimal(")?.strip_suffix(')')?.split_once(',')?;
        Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
    };
    match (from, to) {
        _ if from == to => true,
        ("unknown", _) | ("int", "long") | ("float", "double") => true,
        ("date", "timestamp" | "timestamp_ns") => true,
        _ => matches!((decimal(from), decimal(to)), (Some((p, s)), Some((q, t))) if s == t && q > p),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typ3_widenings() {
        for (from, to) in [
            ("int", "long"),
            ("float", "double"),
            ("date", "timestamp"),
            ("date", "timestamp_ns"),
            ("decimal(10,2)", "decimal(12,2)"),
            ("unknown", "string"),
            ("long", "long"),
        ] {
            assert!(widens(from, to), "{from} to {to}");
        }
        for (from, to) in [
            ("long", "int"),
            ("int", "double"),
            ("string", "long"),
            ("decimal(10,2)", "decimal(10,3)"),
            ("decimal(12,2)", "decimal(10,2)"),
            ("timestamp", "date"),
        ] {
            assert!(!widens(from, to), "{from} to {to}");
        }
    }
}
