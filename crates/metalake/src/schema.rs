//! The types of the specification (TYP-1, the types of Iceberg) as Arrow. The format stores what a column is; Arrow is
//! how an engine holds it. This is the one place that translates, and every field carries its id in the metadata key
//! Parquet and DataFusion read, `PARQUET:field_id`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, make_array};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Fields, Schema, TimeUnit};

use crate::error::{Error, Result};
use crate::table::Field;

/// The metadata key of a field's id, as Parquet's Arrow reader and writer use it.
pub const FIELD_ID: &str = "PARQUET:field_id";

/// The zone of a `timestamptz`: instants, stored in UTC.
const UTC: &str = "+00:00";

/// The Arrow schema of a table's fields, which `describe` gives parents first and in position order.
pub fn arrow_schema(fields: &[Field]) -> Result<Schema> {
    Ok(Schema::new(children(fields, None)?))
}

fn children(fields: &[Field], parent: Option<i32>) -> Result<Vec<ArrowField>> {
    let mut found: Vec<&Field> = fields.iter().filter(|f| f.parent_field_id == parent).collect();
    found.sort_by_key(|f| f.position);
    found.into_iter().map(|f| arrow_field(fields, f, &f.name)).collect()
}

fn child<'a>(fields: &'a [Field], parent: &Field, name: &str) -> Result<&'a Field> {
    fields
        .iter()
        .find(|f| f.parent_field_id == Some(parent.field_id) && f.name == name)
        .ok_or_else(|| Error::Schema(format!("{} {} has no {name} field", parent.ty, parent.name)))
}

fn arrow_field(fields: &[Field], f: &Field, name: &str) -> Result<ArrowField> {
    let ty = match f.ty.as_str() {
        "struct" => DataType::Struct(Fields::from(children(fields, Some(f.field_id))?)),
        "list" => DataType::List(Arc::new(arrow_field(fields, child(fields, f, "element")?, "element")?)),
        "map" => {
            let key = arrow_field(fields, child(fields, f, "key")?, "key")?;
            let value = arrow_field(fields, child(fields, f, "value")?, "value")?;
            let entries = DataType::Struct(Fields::from(vec![key.with_nullable(false), value]));
            DataType::Map(Arc::new(ArrowField::new("key_value", entries, false)), false)
        }
        primitive => arrow_type(primitive)?,
    };
    let metadata = HashMap::from([(FIELD_ID.to_string(), f.field_id.to_string())]);
    Ok(ArrowField::new(name, ty, !f.required).with_metadata(metadata))
}

/// The Arrow type of a primitive type of the specification, in its canonical form.
pub fn arrow_type(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "unknown" => DataType::Null,
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into())),
        "string" => DataType::Utf8,
        "uuid" => DataType::FixedSizeBinary(16),
        "binary" => DataType::Binary,
        other => {
            if let Some((precision, scale)) = parameters(other, "decimal(", ")") {
                let (p, s) = (precision.parse::<u8>(), scale.parse::<i8>());
                match (p, s) {
                    (Ok(p), Ok(s)) if (1..=38).contains(&p) => DataType::Decimal128(p, s),
                    _ => return Err(Error::Schema(format!("{other} is not a decimal of the specification"))),
                }
            } else if let Some(length) = other.strip_prefix("fixed[").and_then(|r| r.strip_suffix(']')) {
                match length.parse::<i32>() {
                    Ok(l) if l >= 1 => DataType::FixedSizeBinary(l),
                    _ => {
                        return Err(Error::Schema(format!(
                            "{other} is not a fixed type of the specification"
                        )));
                    }
                }
            } else {
                return Err(Error::Schema(format!("the core does not read or write {other} yet")));
            }
        }
    })
}

/// The fields of a new table from an Arrow schema, the reverse of [`arrow_schema`]: ids drawn from 1 in the order
/// the fields appear, depth first (a struct's children, a list's element, a map's key and value after their
/// parent). Arrow types the specification has no type for are refused by name; cast them first.
pub fn spec_fields(schema: &Schema) -> Result<Vec<Field>> {
    let mut fields = Vec::new();
    let mut next = 1;
    for (i, f) in schema.fields().iter().enumerate() {
        add_field(
            &mut fields,
            &mut next,
            None,
            i as i32 + 1,
            f.name(),
            f.data_type(),
            f.is_nullable(),
        )?;
    }
    Ok(fields)
}

fn add_field(
    out: &mut Vec<Field>,
    next: &mut i32,
    parent: Option<i32>,
    position: i32,
    name: &str,
    ty: &DataType,
    nullable: bool,
) -> Result<()> {
    let id = *next;
    *next += 1;
    let (spec, children): (String, Vec<(String, DataType, bool)>) = match ty {
        DataType::Struct(fs) => (
            "struct".into(),
            fs.iter()
                .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
                .collect(),
        ),
        DataType::List(e) | DataType::LargeList(e) => (
            "list".into(),
            vec![("element".into(), e.data_type().clone(), e.is_nullable())],
        ),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => (
                "map".into(),
                vec![
                    ("key".into(), kv[0].data_type().clone(), false),
                    ("value".into(), kv[1].data_type().clone(), kv[1].is_nullable()),
                ],
            ),
            _ => {
                return Err(Error::Schema(format!(
                    "{name}: a map's entries are a struct of a key and a value"
                )));
            }
        },
        primitive => (
            spec_type(primitive).map_err(|e| Error::Schema(format!("{name}: {e}")))?,
            Vec::new(),
        ),
    };
    out.push(Field {
        field_id: id,
        parent_field_id: parent,
        position,
        name: name.to_string(),
        ty: spec,
        required: !nullable,
        doc: None,
        initial_default: None,
        write_default: None,
    });
    for (i, (child, ty, nullable)) in children.iter().enumerate() {
        add_field(out, next, Some(id), i as i32 + 1, child, ty, *nullable)?;
    }
    Ok(())
}

/// The type of the specification of a primitive Arrow type.
pub fn spec_type(ty: &DataType) -> Result<String> {
    Ok(match ty {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Date32 => "date".into(),
        DataType::Time64(TimeUnit::Microsecond) => "time".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamptz".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp_ns".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "timestamptz_ns".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Binary | DataType::LargeBinary => "binary".into(),
        DataType::FixedSizeBinary(n) => format!("fixed[{n}]"),
        DataType::Decimal128(p, s) if *s >= 0 => format!("decimal({p},{s})"),
        other => {
            return Err(Error::Schema(format!(
                "{other} has no type in the specification: cast it to one that has"
            )));
        }
    })
}

/// The two parameters of `prefix<a>,<b>suffix`, trimmed.
fn parameters<'a>(ty: &'a str, prefix: &str, suffix: &str) -> Option<(&'a str, &'a str)> {
    let (a, b) = ty.strip_prefix(prefix)?.strip_suffix(suffix)?.split_once(',')?;
    Some((a.trim(), b.trim()))
}

/// The same array under `ty`, which must be the same physical type: only names, ids, nullability and a timestamp's
/// zone may differ, and nested fields are retyped in turn.
pub(crate) fn retype(array: &ArrayRef, ty: &DataType) -> std::result::Result<ArrayRef, ArrowError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn field(id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, required: bool) -> Field {
        Field {
            field_id: id,
            parent_field_id: parent,
            position,
            name: name.into(),
            ty: ty.into(),
            required,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    fn id(f: &ArrowField) -> &str {
        f.metadata()[FIELD_ID].as_str()
    }

    #[test]
    fn every_primitive_type_has_its_arrow_type() {
        let cases = [
            ("unknown", DataType::Null),
            ("boolean", DataType::Boolean),
            ("int", DataType::Int32),
            ("long", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("date", DataType::Date32),
            ("time", DataType::Time64(TimeUnit::Microsecond)),
            ("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None)),
            (
                "timestamptz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            ),
            ("timestamp_ns", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            (
                "timestamptz_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            ),
            ("string", DataType::Utf8),
            ("uuid", DataType::FixedSizeBinary(16)),
            ("binary", DataType::Binary),
            ("decimal(38,10)", DataType::Decimal128(38, 10)),
            ("decimal(1,0)", DataType::Decimal128(1, 0)),
            ("fixed[3]", DataType::FixedSizeBinary(3)),
        ];
        for (ty, want) in cases {
            assert_eq!(arrow_type(ty).unwrap(), want, "{ty}");
        }
    }

    #[test]
    fn types_the_core_does_not_read_yet_are_refused_by_name() {
        for ty in [
            "variant",
            "geometry",
            "geography",
            "decimal(39,0)",
            "decimal(0,0)",
            "fixed[0]",
            "nonsense",
        ] {
            let refused = arrow_type(ty).unwrap_err();
            assert!(
                matches!(&refused, Error::Schema(m) if m.contains(ty)),
                "{ty}: {refused}"
            );
        }
    }

    #[test]
    fn nested_fields_keep_their_ids_names_and_nullability() {
        let fields = [
            field(1, None, 1, "k", "long", true),
            field(2, None, 2, "s", "struct", false),
            field(3, Some(2), 2, "y", "string", true),
            field(9, Some(2), 1, "x", "int", false),
            field(4, None, 3, "tags", "list", false),
            field(5, Some(4), 1, "element", "string", true),
            field(6, None, 4, "m", "map", false),
            field(7, Some(6), 1, "key", "string", true),
            field(8, Some(6), 2, "value", "double", false),
        ];
        let schema = arrow_schema(&fields).unwrap();
        let top: Vec<(&str, &str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), id(f), f.is_nullable()))
            .collect();
        assert_eq!(
            top,
            [
                ("k", "1", false),
                ("s", "2", true),
                ("tags", "4", true),
                ("m", "6", true)
            ]
        );

        let DataType::Struct(inner) = schema.field(1).data_type() else {
            panic!("a struct")
        };
        let inner: Vec<(&str, &str, bool)> = inner
            .iter()
            .map(|f| (f.name().as_str(), id(f), f.is_nullable()))
            .collect();
        assert_eq!(inner, [("x", "9", true), ("y", "3", false)]);

        let DataType::List(element) = schema.field(2).data_type() else {
            panic!("a list")
        };
        assert_eq!(
            (element.name().as_str(), id(element), element.is_nullable()),
            ("element", "5", false)
        );

        let DataType::Map(entries, sorted) = schema.field(3).data_type() else {
            panic!("a map")
        };
        assert!(!sorted && !entries.is_nullable());
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("map entries")
        };
        let kv: Vec<(&str, &str, bool, &DataType)> = kv
            .iter()
            .map(|f| (f.name().as_str(), id(f), f.is_nullable(), f.data_type()))
            .collect();
        assert_eq!(
            kv,
            [
                ("key", "7", false, &DataType::Utf8),
                ("value", "8", true, &DataType::Float64)
            ]
        );
    }

    #[test]
    fn an_arrow_schema_becomes_fields_and_comes_back_the_same() {
        let fields = [
            field(1, None, 1, "k", "long", true),
            field(2, None, 2, "s", "struct", false),
            field(3, Some(2), 1, "x", "int", false),
            field(4, Some(2), 2, "y", "string", true),
            field(5, None, 3, "tags", "list", false),
            field(6, Some(5), 1, "element", "string", true),
            field(7, None, 4, "m", "map", false),
            field(8, Some(7), 1, "key", "string", true),
            field(9, Some(7), 2, "value", "decimal(10,2)", false),
            field(10, None, 5, "at", "timestamptz", false),
        ];
        let schema = arrow_schema(&fields).unwrap();
        assert_eq!(spec_fields(&schema).unwrap(), fields);
    }

    #[test]
    fn arrow_types_without_a_spec_type_are_refused() {
        for ty in [
            DataType::UInt64,
            DataType::Int16,
            DataType::Utf8View,
            DataType::Time32(TimeUnit::Millisecond),
        ] {
            let schema = Schema::new(vec![ArrowField::new("c", ty.clone(), true)]);
            assert!(
                matches!(spec_fields(&schema), Err(Error::Schema(m)) if m.starts_with("c: ")),
                "{ty}"
            );
        }
    }

    #[test]
    fn a_list_without_its_element_is_refused() {
        let fields = [field(4, None, 1, "tags", "list", false)];
        assert!(matches!(arrow_schema(&fields), Err(Error::Schema(m)) if m.contains("element")));
    }
}
