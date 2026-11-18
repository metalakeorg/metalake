//! Iceberg's name mapping (ENG-4): which field id each column of a file with no field ids is, found by its name. It is
//! the table property `schema.name-mapping.default`, in Iceberg's JSON, so that an Iceberg client reads the same files
//! the same way. A field may answer to several names, which keeps a rename readable in files written before it.
//!
//! The one use of a mapping is [`NameMapping::apply`]: it gives a file's Arrow schema the ids it lacks. Everything that
//! finds a file's columns by id (`scan`, `stats`, an engine's adapter) then reads such a file as it reads any other, so
//! there is one resolution and not two (ENG-3, step 3).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, FieldRef, Fields, Schema};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::FIELD_ID;
use crate::table::Field;

/// The table property that holds a table's name mapping (ENG-4).
pub const PROPERTY: &str = "schema.name-mapping.default";

/// A table's name mapping: one entry per top-level field, as Iceberg writes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NameMapping(Vec<MappedField>);

/// A field of a mapping: its id, the names a file may give it, and its children's mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MappedField {
    #[serde(rename = "field-id", default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    pub names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<MappedField>,
}

impl NameMapping {
    /// A mapping in Iceberg's JSON.
    pub fn parse(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map(Self)
            .map_err(|e| Error::Schema(format!("{PROPERTY}: {e}")))
    }

    /// Iceberg's JSON of the mapping.
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.0).expect("a mapping serializes")
    }

    /// The mapping of a table's fields, each by its own name, nested fields under their parent.
    pub fn of_fields(fields: &[Field]) -> Self {
        let mut children: HashMap<Option<i32>, Vec<&Field>> = HashMap::new();
        for f in fields {
            children.entry(f.parent_field_id).or_default().push(f);
        }
        fn under(parent: Option<i32>, children: &HashMap<Option<i32>, Vec<&Field>>) -> Vec<MappedField> {
            let mut found = children.get(&parent).cloned().unwrap_or_default();
            found.sort_by_key(|f| f.position);
            found
                .into_iter()
                .map(|f| MappedField {
                    field_id: Some(f.field_id),
                    names: vec![f.name.clone()],
                    fields: under(Some(f.field_id), children),
                })
                .collect()
        }
        Self(under(None, &children))
    }

    /// The top-level entries.
    pub fn fields(&self) -> &[MappedField] {
        &self.0
    }

    /// `schema` with the field ids it lacks, from the mapping, by name: top-level columns, a struct's children, a
    /// list's element and a map's key and value, whatever names the file gives those three, as Iceberg reads them.
    /// A column that has an id keeps it (ENG-3, step 1); one the mapping does not name stays without, and so reads as
    /// a field the file lacks.
    pub fn apply(&self, schema: &Schema) -> Schema {
        let fields: Vec<FieldRef> = schema
            .fields()
            .iter()
            .map(|f| Arc::new(mapped(f, find(&self.0, f.name()))))
            .collect();
        Schema::new_with_metadata(fields, schema.metadata().clone())
    }
}

/// The entry of `entries` that answers to `name`.
fn find<'a>(entries: &'a [MappedField], name: &str) -> Option<&'a MappedField> {
    entries.iter().find(|e| e.names.iter().any(|n| n == name))
}

/// `f`, given the id of `entry` if it has none, and its children theirs.
fn mapped(f: &ArrowField, entry: Option<&MappedField>) -> ArrowField {
    let children = entry.map_or(&[][..], |e| e.fields.as_slice());
    let by_position = |i: usize| children.get(i);
    let data_type = match f.data_type() {
        DataType::Struct(fs) => DataType::Struct(Fields::from(
            fs.iter()
                .map(|c| Arc::new(mapped(c, find(children, c.name()))))
                .collect::<Vec<_>>(),
        )),
        DataType::List(e) => DataType::List(Arc::new(mapped(e, by_position(0)))),
        DataType::LargeList(e) => DataType::LargeList(Arc::new(mapped(e, by_position(0)))),
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => {
                let kv: Fields = kv
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Arc::new(mapped(c, by_position(i))))
                    .collect::<Vec<_>>()
                    .into();
                DataType::Map(
                    Arc::new(entries.as_ref().clone().with_data_type(DataType::Struct(kv))),
                    *sorted,
                )
            }
            other => DataType::Map(
                Arc::new(entries.as_ref().clone().with_data_type(other.clone())),
                *sorted,
            ),
        },
        other => other.clone(),
    };
    let mut field = f.clone().with_data_type(data_type);
    if !f.metadata().contains_key(FIELD_ID)
        && let Some(id) = entry.and_then(|e| e.field_id)
    {
        let mut metadata = f.metadata().clone();
        metadata.insert(FIELD_ID.to_string(), id.to_string());
        field = field.with_metadata(metadata);
    }
    field
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str) -> Field {
        Field {
            field_id: id,
            parent_field_id: parent,
            position,
            name: name.into(),
            ty: ty.into(),
            required: false,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    fn id(f: &ArrowField) -> Option<&str> {
        f.metadata().get(FIELD_ID).map(String::as_str)
    }

    /// Iceberg's own example of a mapping, from its table specification.
    const ICEBERG: &str = r#"[
        {"field-id": 1, "names": ["id", "record_id"]},
        {"field-id": 2, "names": ["data"]},
        {"field-id": 3, "names": ["location"], "fields": [
            {"field-id": 4, "names": ["latitude", "lat"]},
            {"field-id": 5, "names": ["longitude", "long"]}]}]"#;

    #[test]
    fn eng4_iceberg_json_round_trip() {
        let mapping = NameMapping::parse(ICEBERG).unwrap();
        assert_eq!(mapping.fields().len(), 3);
        assert_eq!(mapping.fields()[2].fields[1].names, ["longitude", "long"]);
        assert_eq!(NameMapping::parse(&mapping.to_json()).unwrap(), mapping);
        let unnamed = NameMapping::parse(r#"[{"names": ["gone"]}]"#).unwrap();
        assert_eq!(unnamed.fields()[0].field_id, None);
        assert_eq!(unnamed.to_json(), r#"[{"names":["gone"]}]"#);
        assert!(NameMapping::parse(r#"{"field-id": 1}"#).is_err());
    }

    #[test]
    fn eng4_of_fields_nests_in_position_order() {
        let fields = [
            field(2, None, 2, "loc", "struct"),
            field(1, None, 1, "k", "long"),
            field(4, Some(2), 2, "lon", "double"),
            field(3, Some(2), 1, "lat", "double"),
        ];
        let json = NameMapping::of_fields(&fields).to_json();
        assert_eq!(
            json,
            r#"[{"field-id":1,"names":["k"]},{"field-id":2,"names":["loc"],"fields":[{"field-id":3,"names":["lat"]},{"field-id":4,"names":["lon"]}]}]"#
        );
    }

    #[test]
    fn eng3_apply_gives_ids_by_name_and_keeps_the_file_s() {
        let mapping = NameMapping::parse(ICEBERG).unwrap();
        let point = Fields::from(vec![
            ArrowField::new("lat", DataType::Float64, true),
            ArrowField::new("long", DataType::Float64, true),
        ]);
        let file = Schema::new(vec![
            ArrowField::new("record_id", DataType::Int64, false),
            ArrowField::new("data", DataType::Utf8, true)
                .with_metadata([(FIELD_ID.to_string(), "9".to_string())].into()),
            ArrowField::new("location", DataType::Struct(point), true),
            ArrowField::new("extra", DataType::Utf8, true),
        ]);
        let mapped = mapping.apply(&file);
        let ids: Vec<Option<&str>> = mapped.fields().iter().map(|f| id(f)).collect();
        assert_eq!(
            ids,
            [Some("1"), Some("9"), Some("3"), None],
            "an alias, the file's own id, a struct, no name"
        );
        let DataType::Struct(children) = mapped.field(2).data_type() else {
            panic!()
        };
        assert_eq!((id(&children[0]), id(&children[1])), (Some("4"), Some("5")));
    }

    #[test]
    fn eng3_apply_lists_and_maps_by_position() {
        let fields = [
            field(1, None, 1, "tags", "list"),
            field(2, Some(1), 1, "element", "string"),
            field(3, None, 2, "attrs", "map"),
            field(4, Some(3), 1, "key", "string"),
            field(5, Some(3), 2, "value", "long"),
        ];
        let mapping = NameMapping::of_fields(&fields);
        // Hive's writers name a list's element `array` or `bag`, and a map's entries as they please.
        let entries = ArrowField::new(
            "key_value",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("k", DataType::Utf8, false),
                ArrowField::new("v", DataType::Int64, true),
            ])),
            false,
        );
        let file = Schema::new(vec![
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("array", DataType::Utf8, true))),
                true,
            ),
            ArrowField::new("attrs", DataType::Map(Arc::new(entries), false), true),
        ]);
        let mapped = mapping.apply(&file);
        let DataType::List(element) = mapped.field(0).data_type() else {
            panic!()
        };
        assert_eq!((id(mapped.field(0)), id(element)), (Some("1"), Some("2")));
        let DataType::Map(entries, _) = mapped.field(1).data_type() else {
            panic!()
        };
        let DataType::Struct(kv) = entries.data_type() else {
            panic!()
        };
        assert_eq!(
            (id(mapped.field(1)), id(&kv[0]), id(&kv[1])),
            (Some("3"), Some("4"), Some("5"))
        );
    }
}
