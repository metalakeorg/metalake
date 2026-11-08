//! Schemas: the JSON schema of a config for the API's forms, and the columns a table is declared with against the
//! table's own.

use arrow_schema::{DataType, Schema};
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, SchemaGenerator};
use serde_json::Value;

/// The JSON schema of `T` as Arroyo's API renders a connector's config. The same as `arroyo_connectors::render_schema`,
/// which this crate can't use, since `arroyo-connectors` depends on it.
pub fn render_schema<T: ?Sized + JsonSchema>() -> String {
    let mut settings = SchemaSettings::draft2019_09();
    settings.inline_subschemas = true;
    let schema = SchemaGenerator::new(settings).into_root_schema_for::<T>();
    serde_json::to_string(&sanitize(schema.to_value())).unwrap()
}

/// What Arroyo's console reads: no defaults, a nullable type as the type itself, and `oneOf` for `anyOf`.
fn sanitize(mut schema: Value) -> Value {
    fn walk(v: &mut Value) {
        match v {
            Value::Object(obj) => {
                obj.remove("default");
                if let Some(Value::Array(types)) = obj.get_mut("type")
                    && types.len() == 2
                    && types.iter().any(|t| t == "null")
                    && types.iter().all(|t| t.is_string())
                {
                    let non_null = types
                        .iter()
                        .find_map(|t| t.as_str().filter(|s| *s != "null"))
                        .unwrap()
                        .to_owned();
                    obj.insert("type".to_string(), Value::String(non_null));
                }
                if let Some(Value::Array(values)) = obj.get_mut("enum")
                    && let Some(i) = values.iter().position(|v| v.is_null())
                {
                    values.remove(i);
                }
                if let Some(any_of) = obj.remove("anyOf") {
                    obj.insert("oneOf".to_string(), any_of);
                }
                obj.values_mut().for_each(walk);
            }
            Value::Array(values) => values.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut schema);
    schema
}

/// Checks the columns a table is declared with against the table's: each must be one of its columns, by name, of a type
/// the source can give it. A column of the table that isn't declared is fine: it isn't read.
pub fn check_columns(declared: &Schema, table: &Schema) -> Result<(), String> {
    for field in declared.fields() {
        let Ok(column) = table.field_with_name(field.name()) else {
            return Err(format!("the table has no column '{}'", field.name()));
        };
        if !compatible(column.data_type(), field.data_type()) {
            return Err(format!(
                "column '{}' is {} in the table, and declared {}",
                field.name(),
                column.data_type(),
                field.data_type()
            ));
        }
    }
    Ok(())
}

/// Checks the columns a sink is declared with against its table's: each is one of the table's, of a type it can be
/// written as, and every column the table requires is declared, since nothing else could fill it.
pub fn check_sink_columns(declared: &Schema, table: &Schema) -> Result<(), String> {
    check_columns(declared, table)?;
    match table
        .fields()
        .iter()
        .find(|c| !c.is_nullable() && declared.field_with_name(c.name()).is_err())
    {
        Some(column) => Err(format!(
            "the table requires column '{}', and it isn't declared",
            column.name()
        )),
        None => Ok(()),
    }
}

/// Whether a column of type `table` can be read as `declared`. A timestamp is an instant in any unit, and Arroyo's SQL
/// declares no time zone, so the source casts timestamps; strings are strings in any of Arrow's layouts.
pub(crate) fn compatible(table: &DataType, declared: &DataType) -> bool {
    use DataType::*;
    let text = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    match (table, declared) {
        (Timestamp(..), Timestamp(..)) => true,
        (t, d) if text(t) && text(d) => true,
        (t, d) => t == d,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, TimeUnit};

    fn schema(fields: Vec<(&str, DataType)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn check_columns_subset_and_mismatch() {
        let table = schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            ("unread", DataType::Float64),
        ]);
        let declared = schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::LargeUtf8),
            ("at", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ]);
        assert_eq!(
            check_columns(&declared, &table),
            Ok(()),
            "a subset, with compatible types"
        );

        let missing = schema(vec![("other", DataType::Int64)]);
        assert_eq!(
            check_columns(&missing, &table),
            Err("the table has no column 'other'".into())
        );

        let other_type = schema(vec![("id", DataType::Int32)]);
        assert!(
            check_columns(&other_type, &table)
                .unwrap_err()
                .contains("is Int64 in the table")
        );
    }

    #[test]
    fn check_sink_columns_required_and_unknown() {
        let table = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]);
        let id_only = schema(vec![("id", DataType::Int64)]);
        assert_eq!(
            check_sink_columns(&id_only, &table),
            Ok(()),
            "an optional column may be left out"
        );
        let note_only = schema(vec![("note", DataType::Utf8)]);
        assert_eq!(
            check_sink_columns(&note_only, &table),
            Err("the table requires column 'id', and it isn't declared".into())
        );
        let unknown = schema(vec![("id", DataType::Int64), ("other", DataType::Int64)]);
        assert!(check_sink_columns(&unknown, &table).is_err());
    }

    #[test]
    fn render_schema_console_format() {
        let rendered: Value = serde_json::from_str(&render_schema::<crate::config::MetaLakeTable>()).unwrap();
        let text = rendered.to_string();
        assert!(!text.contains("\"default\""));
        assert!(!text.contains("anyOf"));
        assert_eq!(rendered["title"], "MetaLake table");
    }
}
