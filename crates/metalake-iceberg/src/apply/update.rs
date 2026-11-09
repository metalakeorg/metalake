//! An `updateTable` as one commit (ICE-6): what it asks, checked before anything is read. A commit may carry several
//! snapshots, as a client's transaction does when it deletes and then appends, and they are still one commit.

use std::collections::BTreeMap;

use serde_json::Value;

use super::Refused;
use crate::project::metadata::{COPY_ON_WRITE, ICEBERG_METADATA, Materialize};
use crate::wire::commit::{CommitTableRequest, Entry, ProposedSnapshot, Requirement, Update};

/// The format a table is served in (ICE-1).
const FORMAT_VERSION: i32 = 2;

/// What Iceberg computes for a snapshot's summary over the client's base and its manifests: the catalog states these
/// itself or has no use for them (CMT-1), and any other key stays the client's (CMT-12).
const COMPUTED_KEYS: [&str; 4] = [
    "operation",
    "changed-partition-count",
    "partition-summaries-included",
    "replace-partitions",
];
const COMPUTED_PREFIXES: [&str; 7] = [
    "added-",
    "deleted-",
    "removed-",
    "total-",
    "manifests-",
    "entries-",
    "partitions.",
];

/// The definition of a table that a stage-create left with its client, sent back whole (REST-7).
#[derive(Debug, Clone, PartialEq)]
pub struct Creation {
    pub table_id: String,
    pub schema: Value,
    pub spec: Value,
    pub sort_order: Option<Value>,
    pub location: String,
}

/// What one table's entry of a commit asks.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub ref_name: String,
    /// What `assert-ref-snapshot-id` expects of the ref, which may be that it has none.
    pub expected: Option<Option<i64>>,
    pub table_uuid: Option<String>,
    pub creation: Option<Creation>,
    /// The snapshots proposed, in order: each the parent of the next.
    pub snapshots: Vec<ProposedSnapshot>,
    /// By key: its new value, or `None` when it goes (REST-9).
    pub properties: BTreeMap<String, Option<String>>,
}

impl Change {
    /// Whether the commit sets or removes when the table is materialized (REST-9), which the worker that materializes
    /// on commit is told of at once.
    pub fn changes_materialize(&self) -> bool {
        self.properties.contains_key(ICEBERG_METADATA)
    }

    /// The key that makes a repeat of this commit apply nothing (REST-3). A change of properties has none, as repeating
    /// it changes nothing.
    pub fn app_id(&self, table_id: &str) -> Option<String> {
        match (self.snapshots.last(), &self.creation) {
            (Some(last), _) => Some(format!("iceberg:{table_id}:{}", last.snapshot_id)),
            (None, Some(_)) => Some(format!("iceberg:{table_id}:create")),
            (None, None) => None,
        }
    }

    /// The operation the last snapshot's summary states.
    pub fn operation(&self) -> Option<&str> {
        self.snapshots.last()?.summary.get("operation").map(String::as_str)
    }

    /// What the last snapshot's summary keeps of the client's: how an engine finds what it committed (CMT-12).
    pub fn client_summary(&self) -> BTreeMap<String, String> {
        let computed =
            |key: &str| COMPUTED_KEYS.contains(&key) || COMPUTED_PREFIXES.iter().any(|prefix| key.starts_with(prefix));
        let last = self.snapshots.last().map(|s| &s.summary);
        let kept = last.into_iter().flatten().filter(|(key, _)| !computed(key));
        kept.map(|(key, value)| (key.clone(), value.clone())).collect()
    }

    /// Holds `assert-table-uuid` to the table the name resolved to. A table's id never changes, so this needs no lock.
    pub fn check_table(&self, table_id: &str) -> Result<(), Refused> {
        check_uuid(self.table_uuid.as_deref(), table_id)
    }
}

/// Holds an asserted uuid to the table's id: another table under the name is a conflict, as the client's is gone.
pub fn check_uuid(asserted: Option<&str>, table_id: &str) -> Result<(), Refused> {
    match asserted {
        Some(uuid) if !uuid.eq_ignore_ascii_case(table_id) => {
            Err(Refused::Conflict(format!("the table is {table_id}, not {uuid}")))
        }
        _ => Ok(()),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T, Refused> {
    Err(Refused::Invalid(message.into()))
}

/// The requirements of a commit: whether it creates, its uuid, and the ref it expects, with what it expects of it.
type Required = (bool, Option<String>, Option<(String, Option<i64>)>);

fn requirements(entries: Vec<Entry<Requirement>>) -> Result<Required, Refused> {
    let (mut creates, mut uuid, mut expected) = (false, None, None);
    let alone = entries.len() == 1;
    for entry in entries {
        match entry.known {
            Requirement::AssertCreate if alone => creates = true,
            Requirement::AssertCreate => return invalid("assert-create goes alone"),
            Requirement::AssertTableUuid { uuid: wanted } => uuid = Some(wanted),
            Requirement::AssertRefSnapshotId { reference, snapshot_id } => {
                let wanted = (reference, snapshot_id);
                if expected.as_ref().is_some_and(|seen| seen != &wanted) {
                    return invalid("assert-ref-snapshot-id may be asked of one ref, once");
                }
                expected = Some(wanted);
            }
            // They guard changes of a definition, which are refused: holding them to nothing would hide that.
            Requirement::Other => return invalid(format!("{} is not served", entry.name)),
        }
    }
    Ok((creates, uuid, expected))
}

/// What the updates of a stage-create carry, as they arrive.
#[derive(Debug, Default)]
struct Definition {
    uuid: Option<String>,
    schema: Option<Value>,
    spec: Option<Value>,
    sort_order: Option<Value>,
    location: Option<String>,
}

impl Definition {
    fn creation(self) -> Result<Creation, Refused> {
        let needs = |what: &str| format!("a commit that creates a table needs {what}");
        let (Some(table_id), Some(schema), Some(location)) = (self.uuid, self.schema, self.location) else {
            return invalid(needs("assign-uuid, add-schema and set-location"));
        };
        Ok(Creation {
            table_id,
            schema,
            spec: self.spec.ok_or_else(|| Refused::Invalid(needs("add-spec")))?,
            sort_order: self.sort_order,
            location,
        })
    }
}

/// One value of a definition, which a table has one of when it is created.
fn set_once<T>(slot: &mut Option<T>, value: T, name: &str, creates: bool) -> Result<(), Refused> {
    if !creates {
        return invalid(format!(
            "{name} is not served: a definition changes only when a table is created"
        ));
    }
    if slot.replace(value).is_some() {
        return invalid(format!("{name} is sent once"));
    }
    Ok(())
}

/// REST-4 and REST-9: a table's write modes are the server's.
fn is_reserved_property(key: &str) -> bool {
    COPY_ON_WRITE.contains(&key)
}

/// REST-4 and REST-9: a property may be anything but another write mode, or a time to materialize that is none.
pub(super) fn check_property(key: &str, value: &str) -> Result<(), Refused> {
    if is_reserved_property(key) && value != "copy-on-write" {
        return invalid(format!("{key} is copy-on-write: Iceberg clients write no deletes here"));
    }
    if key == ICEBERG_METADATA && Materialize::named(value).is_none() {
        return invalid(format!("{key} is on-load or on-commit, not {value}"));
    }
    Ok(())
}

/// The ref a commit moves. Its snapshots must end at what the ref is set to, each the parent of the next: anything else
/// is a stage, a rollback or a fast-forward, which ICE-6 does not make a commit of files.
fn target_ref(snapshots: &[ProposedSnapshot], moves: &[(String, i64)]) -> Result<Option<String>, Refused> {
    let (Some((name, to)), Some(last)) = (moves.last(), snapshots.last()) else {
        return match (moves.is_empty(), snapshots.is_empty()) {
            (true, true) => Ok(None),
            (true, false) => invalid("a snapshot that no ref is set to is not served"),
            (false, _) => invalid("set-snapshot-ref is served only to a snapshot the commit adds"),
        };
    };
    if moves.iter().any(|(other, _)| other != name) {
        return invalid("a commit moves one ref");
    }
    if *to != last.snapshot_id {
        return invalid("set-snapshot-ref is served only to the last snapshot the commit adds");
    }
    let chained = snapshots
        .windows(2)
        .all(|pair| pair[1].parent_snapshot_id == Some(pair[0].snapshot_id));
    if !chained {
        return invalid("the snapshots of a commit are each the parent of the next");
    }
    Ok(Some(name.clone()))
}

/// What a commit asks, or why it is refused: no read of the table can change the answer.
/// The uuid a commit asserts of its table, read apart so that it is held before anything the commit asks is refused: a
/// client told its table is another rebuilds on it, where one told only what it may not ask fails (REST-3).
pub fn asserted_uuid(request: &CommitTableRequest) -> Option<String> {
    request.requirements.iter().find_map(|entry| match &entry.known {
        Requirement::AssertTableUuid { uuid } => Some(uuid.clone()),
        _ => None,
    })
}

pub fn change(request: CommitTableRequest) -> Result<Change, Refused> {
    let (creates, table_uuid, expected) = requirements(request.requirements)?;
    let mut definition = Definition::default();
    let (mut snapshots, mut moves, mut properties) = (Vec::new(), Vec::new(), BTreeMap::new());
    for entry in request.updates {
        let name = entry.name.as_str();
        match entry.known {
            Update::AssignUuid { uuid } => set_once(&mut definition.uuid, uuid, name, creates)?,
            Update::UpgradeFormatVersion { format_version } if !creates || format_version != FORMAT_VERSION => {
                return invalid(format!("tables are served in format {FORMAT_VERSION}"));
            }
            Update::UpgradeFormatVersion { .. } => {}
            Update::AddSchema { schema } => set_once(&mut definition.schema, schema, name, creates)?,
            Update::AddSpec { spec } => set_once(&mut definition.spec, spec, name, creates)?,
            Update::AddSortOrder { sort_order } => set_once(&mut definition.sort_order, sort_order, name, creates)?,
            Update::SetLocation { location } => set_once(&mut definition.location, location, name, creates)?,
            // With one of each, there is only the one to make current.
            Update::SetCurrentSchema {} | Update::SetDefaultSpec {} | Update::SetDefaultSortOrder {} if creates => {}
            Update::SetCurrentSchema {} | Update::SetDefaultSpec {} | Update::SetDefaultSortOrder {} => {
                return invalid(format!(
                    "{name} is not served: a definition changes only when a table is created"
                ));
            }
            Update::SetProperties { updates } => {
                for (key, value) in updates {
                    check_property(&key, &value)?;
                    properties.insert(key, Some(value));
                }
            }
            Update::RemoveProperties { removals } => {
                for key in removals {
                    if is_reserved_property(&key) {
                        return invalid(format!("{key} is the server's, and stays"));
                    }
                    properties.insert(key, None);
                }
            }
            Update::AddSnapshot { snapshot } => snapshots.push(snapshot),
            // Hints a client may recompute, which Trino sends with every write. The catalog keeps its own statistics.
            Update::SetStatistics {}
            | Update::RemoveStatistics {}
            | Update::SetPartitionStatistics {}
            | Update::RemovePartitionStatistics {} => {}
            Update::SetSnapshotRef { kind, .. } if kind != "branch" => {
                return invalid("a tag is not moved through Iceberg");
            }
            Update::SetSnapshotRef {
                ref_name, snapshot_id, ..
            } => moves.push((ref_name, snapshot_id)),
            Update::Other => return invalid(format!("{name} is not served")),
        }
    }
    let target = target_ref(&snapshots, &moves)?;
    let (expected_ref, expected) = expected.unzip();
    if target.is_some() && expected_ref.is_some() && target != expected_ref {
        return invalid("a commit asserts the ref it moves");
    }
    let ref_name = target.or(expected_ref).unwrap_or_else(|| "main".to_string());
    let creation = if creates { Some(definition.creation()?) } else { None };
    Ok(Change {
        ref_name,
        expected,
        table_uuid,
        creation,
        snapshots,
        properties,
    })
}

/// REST-9: a commit of several tables is on one ref.
pub fn one_ref<'a>(mut changes: impl Iterator<Item = &'a Change>) -> Result<&'a str, Refused> {
    let Some(first) = changes.next() else {
        return invalid("a transaction changes a table");
    };
    if changes.any(|c| c.ref_name != first.ref_name) {
        return invalid("a transaction is on one ref");
    }
    Ok(&first.ref_name)
}

#[cfg(test)]
mod tests {
    use serde_json::{from_value, json};

    use super::*;

    /// As PyIceberg 0.12 serializes an append.
    fn append() -> Value {
        json!({"identifier": {"namespace": ["db"], "name": "t"}, "requirements": [
                {"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000001"},
                {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 5}],
               "updates": [
                {"action": "add-snapshot", "snapshot": {"snapshot-id": 77, "parent-snapshot-id": 5, "sequence-number": 3,
                 "timestamp-ms": 1793989113887_i64, "manifest-list": "file:///t/metadata/snap-77.avro",
                 "summary": {"operation": "append", "added-data-files": "1"}, "schema-id": 5}},
                {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 77}]})
    }

    /// As PyIceberg 0.12 serializes the commit of a stage-create.
    fn create() -> Value {
        json!({"identifier": {"namespace": ["db"], "name": "t"}, "requirements": [{"type": "assert-create"}], "updates": [
                {"action": "assign-uuid", "uuid": "00000000-0000-0000-0000-000000000002"},
                {"action": "upgrade-format-version", "format-version": 2},
                {"action": "add-schema", "schema": {"type": "struct", "fields": [
                    {"id": 1, "name": "k", "type": "long", "required": true}],
                    "schema-id": 0, "identifier-field-ids": []}},
                {"action": "set-current-schema", "schema-id": -1},
                {"action": "add-spec", "spec": {"spec-id": 0, "fields": []}},
                {"action": "set-default-spec", "spec-id": -1},
                {"action": "add-sort-order", "sort-order": {"order-id": 0, "fields": []}},
                {"action": "set-default-sort-order", "sort-order-id": -1},
                {"action": "set-location", "location": "file:///t"},
                {"action": "set-properties", "updates": {"a": "1"}}]})
    }

    fn asked(body: Value) -> Result<Change, Refused> {
        change(from_value(body).expect("a request"))
    }

    fn refused(body: Value, says: &str) {
        match asked(body) {
            Err(Refused::Invalid(message)) => assert!(message.contains(says), "{message}"),
            other => panic!("{other:?}"),
        }
    }

    fn with_updates(mut body: Value, updates: Value) -> Value {
        body["updates"] = updates;
        body
    }

    fn snapshot(id: i64, parent: i64) -> Value {
        json!({"action": "add-snapshot", "snapshot": {"snapshot-id": id, "parent-snapshot-id": parent,
               "manifest-list": format!("file:///{id}")}})
    }

    fn set_ref(name: &str, id: i64) -> Value {
        json!({"action": "set-snapshot-ref", "ref-name": name, "type": "branch", "snapshot-id": id})
    }

    #[test]
    fn ice6_append_request() {
        let change = asked(append()).unwrap();
        assert_eq!(change.ref_name, "main");
        assert_eq!(change.expected, Some(Some(5)));
        assert_eq!(change.snapshots[0].manifest_list, "file:///t/metadata/snap-77.avro");
        assert_eq!(change.snapshots[0].summary["operation"], "append");
        assert_eq!(change.app_id("u").as_deref(), Some("iceberg:u:77"));
        assert_eq!(change.check_table("00000000-0000-0000-0000-000000000001"), Ok(()));
        assert_eq!(
            change.check_table("00000000-0000-0000-0000-00000000000A"),
            Err(Refused::Conflict(
                "the table is 00000000-0000-0000-0000-00000000000A, not 00000000-0000-0000-0000-000000000001".into()
            ))
        );
    }

    #[test]
    fn rest7_stage_create_request() {
        let change = asked(create()).unwrap();
        let creation = change.creation.as_ref().unwrap();
        assert_eq!(creation.table_id, "00000000-0000-0000-0000-000000000002");
        assert_eq!(creation.location, "file:///t");
        assert_eq!(creation.schema["fields"][0]["name"], "k");
        assert_eq!(change.properties["a"], Some("1".to_string()));
        assert_eq!(change.app_id("u").as_deref(), Some("iceberg:u:create"));

        let without = |action: &str| {
            let mut body = create();
            body["updates"]
                .as_array_mut()
                .unwrap()
                .retain(|u| u["action"] != action);
            body
        };
        refused(without("assign-uuid"), "assign-uuid");
        refused(without("add-spec"), "add-spec");
    }

    #[test]
    fn cmt12_client_summary_keeps_engine_keys() {
        let mut body = append();
        body["updates"][0]["snapshot"]["summary"] = json!({
            "operation": "append", "added-data-files": "1", "deleted-records": "2", "removed-files-size": "3",
            "total-records": "4", "manifests-created": "5", "entries-processed": "6", "partitions.k=1": "7",
            "changed-partition-count": "8", "partition-summaries-included": "true", "replace-partitions": "true",
            "spark.sql.streaming.queryId": "q", "spark.sql.streaming.epochId": "7",
            "flink.max-committed-checkpoint-id": "9", "engine-name": "spark", "job": "a",
        });
        let kept = asked(body).expect("an append").client_summary();
        let keys: Vec<&str> = kept.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "engine-name",
                "flink.max-committed-checkpoint-id",
                "job",
                "spark.sql.streaming.epochId",
                "spark.sql.streaming.queryId",
            ]
        );
    }

    #[test]
    fn cmt12_last_snapshot_summary_kept() {
        let mut change = asked(append()).expect("an append");
        let mut first = change.snapshots[0].clone();
        first.summary = BTreeMap::from([("job".to_string(), "first".to_string())]);
        let mut last = first.clone();
        last.summary.insert("job".to_string(), "last".to_string());
        change.snapshots = vec![first, last];
        assert_eq!(change.client_summary()["job"], "last");
    }

    #[test]
    fn ice6_snapshot_chain() {
        let body = with_updates(
            append(),
            json!([snapshot(7, 5), set_ref("main", 7), snapshot(8, 7), set_ref("main", 8)]),
        );
        let change = asked(body).unwrap();
        assert_eq!(change.snapshots.len(), 2);
        assert_eq!(change.app_id("u").as_deref(), Some("iceberg:u:8"));

        let unchained = json!([snapshot(7, 5), snapshot(8, 6), set_ref("main", 8)]);
        refused(with_updates(append(), unchained), "parent");
        refused(
            with_updates(append(), json!([snapshot(7, 5), snapshot(8, 7), set_ref("main", 7)])),
            "last",
        );
    }

    #[test]
    fn ice6_invalid_ref_updates_refused() {
        refused(with_updates(append(), json!([snapshot(7, 5)])), "no ref");
        refused(with_updates(append(), json!([set_ref("main", 7)])), "adds");
        refused(
            with_updates(append(), json!([snapshot(7, 5), set_ref("main", 7), set_ref("b", 7)])),
            "one ref",
        );
        refused(
            with_updates(append(), json!([snapshot(7, 5), set_ref("b", 7)])),
            "asserts",
        );
        let tag = json!({"action": "set-snapshot-ref", "ref-name": "main", "type": "tag", "snapshot-id": 7});
        refused(with_updates(append(), json!([snapshot(7, 5), tag])), "tag");
    }

    #[test]
    fn ice6_unsupported_updates_refused() {
        refused(
            with_updates(append(), json!([{"action": "remove-snapshots", "snapshot-ids": [1]}])),
            "remove-snapshots",
        );
        refused(
            with_updates(append(), json!([{"action": "add-schema", "schema": {}}])),
            "add-schema",
        );
        refused(
            with_updates(append(), json!([{"action": "set-current-schema", "schema-id": 1}])),
            "set-current-schema",
        );
        refused(
            with_updates(append(), json!([{"action": "set-location", "location": "x"}])),
            "set-location",
        );
        let mut body = append();
        body["requirements"] = json!([{"type": "assert-current-schema-id", "current-schema-id": 1}]);
        refused(body, "assert-current-schema-id");
        let mut body = create();
        body["updates"]
            .as_array_mut()
            .unwrap()
            .push(json!({"action": "add-spec", "spec": {}}));
        refused(body, "add-spec is sent once");
        let mut body = create();
        body["updates"][1] = json!({"action": "upgrade-format-version", "format-version": 3});
        refused(body, "format 2");
        let mut body = create();
        body["requirements"] = json!([{"type": "assert-create"}, {"type": "assert-table-uuid", "uuid": "u"}]);
        refused(body, "alone");
    }

    #[test]
    fn ice6_statistics_updates_ignored() {
        let statistics = json!({"action": "set-statistics", "snapshot-id": 7, "statistics": {"statistics-path": "x"}});
        let change = asked(with_updates(append(), json!([statistics]))).unwrap();
        assert!(change.snapshots.is_empty() && change.properties.is_empty());
    }

    #[test]
    fn rest9_property_updates() {
        let body = with_updates(
            append(),
            json!([{"action": "set-properties", "updates": {"a": "1", "b": "2"}},
                                                 {"action": "remove-properties", "removals": ["b", "c"]}]),
        );
        let change = asked(body).unwrap();
        let expected = [("a", Some("1")), ("b", None), ("c", None)];
        let expected = expected.map(|(k, v)| (k.to_string(), v.map(str::to_string)));
        assert_eq!(change.properties, BTreeMap::from(expected));
        assert_eq!(change.app_id("u"), None, "a repeat changes nothing");

        let set = |value: &str| json!([{"action": "set-properties", "updates": {"write.merge.mode": value}}]);
        assert!(asked(with_updates(append(), set("copy-on-write"))).is_ok());
        refused(with_updates(append(), set("merge-on-read")), "write.merge.mode");
        let remove = json!([{"action": "remove-properties", "removals": ["write.update.mode"]}]);
        refused(with_updates(append(), remove), "write.update.mode");

        // when to materialize is one of two, and removing it is on load again
        let set = |value: &str| json!([{"action": "set-properties", "updates": {ICEBERG_METADATA: value}}]);
        for named in ["on-load", "on-commit"] {
            let change = asked(with_updates(append(), set(named))).unwrap();
            assert!(change.changes_materialize(), "setting it, {named}, wakes the worker");
        }
        refused(with_updates(append(), set("always")), ICEBERG_METADATA);
        let remove = json!([{"action": "remove-properties", "removals": [ICEBERG_METADATA]}]);
        assert!(
            asked(with_updates(append(), remove)).unwrap().changes_materialize(),
            "and removing it"
        );
        let other = json!([{"action": "set-properties", "updates": {"a": "1"}}]);
        assert!(
            !asked(with_updates(append(), other)).unwrap().changes_materialize(),
            "another property does not"
        );
        assert!(!asked(append()).unwrap().changes_materialize(), "nor files");
    }

    #[test]
    fn rest9_multi_ref_transaction_refused() {
        let on = |name: &str| Change {
            ref_name: name.to_string(),
            expected: None,
            table_uuid: None,
            creation: None,
            snapshots: Vec::new(),
            properties: BTreeMap::new(),
        };
        assert_eq!(one_ref([on("b"), on("b")].iter()), Ok("b"));
        assert!(one_ref([on("main"), on("b")].iter()).is_err());
        assert!(one_ref([].iter()).is_err());
    }
}
