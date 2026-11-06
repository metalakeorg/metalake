//! R3: the protocol's commit of definitions, against the reference. Random sequences of definition changes on two
//! fresh catalogs with a branch: tables created (with partitioning, a sort order and files in the same commit),
//! renamed and dropped; fields added, promoted, changed against the rules and dropped; properties, partition specs,
//! sort orders and cluster keys. Faulty changes are refused by both, with the same SQLSTATE.

use std::collections::HashMap;
use std::sync::Mutex;

use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog, commit_reference};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

const LATEST: i64 = 9223372036854775806;
const MAIN: i32 = 1;
const BRANCH: i32 = 2;

#[derive(Debug, Clone)]
enum Def {
    Create {
        name: u8,
        files: bool,
        partitioned: bool,
        sorted: bool,
    },
    Rename {
        pick: u16,
        name: u8,
    },
    Drop {
        pick: u16,
    },
    AddField {
        pick: u16,
        name: u8,
        long: bool,
        required: bool,
        undrawn: bool,
    },
    Promote {
        pick: u16,
    },
    Demote {
        pick: u16,
    },
    DropField {
        pick: u16,
        missing: bool,
    },
    Property {
        pick: u16,
        key: u8,
        unset: bool,
    },
    Spec {
        pick: u16,
    },
    Sort {
        pick: u16,
    },
    ClusterKey {
        pick: u16,
        bad: bool,
    },
    Append {
        pick: u16,
    },
}

fn def() -> impl Strategy<Value = Def> {
    prop_oneof![
        4 => (0u8..4, any::<bool>(), any::<bool>(), any::<bool>())
            .prop_map(|(name, files, partitioned, sorted)| Def::Create { name, files, partitioned, sorted }),
        1 => (any::<u16>(), 0u8..5).prop_map(|(pick, name)| Def::Rename { pick, name }),
        1 => any::<u16>().prop_map(|pick| Def::Drop { pick }),
        3 => (any::<u16>(), 0u8..4, any::<bool>(), prop::bool::weighted(0.2), prop::bool::weighted(0.15))
            .prop_map(|(pick, name, long, required, undrawn)| Def::AddField { pick, name, long, required, undrawn }),
        1 => any::<u16>().prop_map(|pick| Def::Promote { pick }),
        1 => any::<u16>().prop_map(|pick| Def::Demote { pick }),
        1 => (any::<u16>(), prop::bool::weighted(0.4)).prop_map(|(pick, missing)| Def::DropField { pick, missing }),
        2 => (any::<u16>(), 0u8..3, any::<bool>()).prop_map(|(pick, key, unset)| Def::Property { pick, key, unset }),
        1 => any::<u16>().prop_map(|pick| Def::Spec { pick }),
        1 => any::<u16>().prop_map(|pick| Def::Sort { pick }),
        1 => (any::<u16>(), prop::bool::weighted(0.3)).prop_map(|(pick, bad)| Def::ClusterKey { pick, bad }),
        2 => any::<u16>().prop_map(|pick| Def::Append { pick }),
    ]
}

/// A visible field of a table, as the reference sees it.
struct Field {
    id: i32,
    name: String,
    ty: String,
    position: i32,
    required: bool,
}

struct Run {
    pair: Pair,
    created: u128,
    files: i64,
    names: HashMap<Uuid, u8>,
}

impl Run {
    async fn tables(&self, r: i32) -> Result<Vec<Uuid>, Error> {
        let rows = self
            .pair
            .on_reference
            .query(
                "select table_id from visible_table_names($1, $2) order by table_id",
                &[&r, &LATEST],
            )
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    async fn fields(&self, r: i32, t: Uuid) -> Result<Vec<Field>, Error> {
        let rows = self
            .pair
            .on_reference
            .query(
                "select field_id, name, type, position, required from visible_fields($1, $2, $3)
                  where parent_field_id is null order by field_id",
                &[&r, &t, &LATEST],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| Field {
                id: row.get(0),
                name: row.get(1),
                ty: row.get(2),
                position: row.get(3),
                required: row.get(4),
            })
            .collect())
    }

    /// Draws an id in both catalogs, as a writer does before its commit (ID-5); both must draw the same.
    async fn draw(&self, t: Uuid, kind: &str) -> Result<i32, Error> {
        let sql = "select next_id($1, $2)";
        let by_reference: i32 = self.pair.on_reference.query_one(sql, &[&t, &kind]).await?.get(0);
        self.pair
            .on_protocol
            .batch_execute(&format!("set search_path = {}", quote_ident(&self.pair.protocol)))
            .await?;
        let by_protocol: i32 = self.pair.on_protocol.query_one(sql, &[&t, &kind]).await?.get(0);
        self.pair.on_protocol.batch_execute("reset search_path").await?;
        if by_reference != by_protocol {
            return Err(format!("next_id drew {by_reference} and {by_protocol}").into());
        }
        Ok(by_reference)
    }

    fn file(&mut self, t: Uuid) -> metalake_protocol_tests::records::FileAdd {
        self.files += 1;
        stored_file(
            t,
            format!("gs://lake/d{}.parquet", self.files),
            10,
            1,
            self.files * 10,
            self.files * 10 + 9,
        )
    }

    /// The commit a definition change describes, resolved against the reference's state; None when there is no
    /// table to act on.
    async fn resolve(&mut self, r: i32, d: &Def) -> Result<Option<Commit>, Error> {
        let tables = self.tables(r).await?;
        let pick = |p: &u16| (!tables.is_empty()).then(|| tables[*p as usize % tables.len()]);
        let mut change = TableChange::default();
        let mut definition = serde_json::Map::new();
        match d {
            Def::Create {
                name,
                files,
                partitioned,
                sorted,
            } => {
                self.created += 1;
                change.table_id = Uuid::from_u128(0x5000 + self.created);
                self.names.insert(change.table_id, *name);
                definition.insert(
                    "create".into(),
                    json!({"namespace": ["db"], "name": format!("t{name}"), "location": "gs://lake"}),
                );
                definition.insert(
                    "fields".into(),
                    json!([{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                           {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "int", "required": false}]),
                );
                definition.insert("cluster_key".into(), json!([1]));
                if *partitioned {
                    definition.insert(
                        "partition_spec".into(),
                        json!([{"source_ids": [1], "field_id": 1000, "name": "k_bucket", "transform": "bucket[4]"}]),
                    );
                }
                if *sorted {
                    definition.insert(
                        "sort_order".into(),
                        json!([{"source_ids": [1], "transform": "identity", "direction": "asc", "null_order": "nulls-first"}]),
                    );
                }
                if *files && !*partitioned {
                    change.adds.push(self.file(change.table_id));
                }
            }
            Def::Rename { pick: p, name } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                definition.insert(
                    "rename".into(),
                    json!({"namespace": ["db"], "name": format!("t{name}")}),
                );
            }
            Def::Drop { pick: p } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                definition.insert("drop".into(), json!(true));
            }
            Def::AddField {
                pick: p,
                name,
                long,
                required,
                undrawn,
            } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                let fields = self.fields(r, t).await?;
                let id = if *undrawn { 900 } else { self.draw(t, "field").await? };
                let position = fields.iter().map(|f| f.position).max().unwrap_or(0) + 1;
                definition.insert(
                    "fields".into(),
                    json!([{"field_id": id, "new": true, "position": position, "name": format!("c{name}"),
                            "type": if *long { "long" } else { "string" }, "required": required}]),
                );
            }
            Def::Promote { pick: p } | Def::Demote { pick: p } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                let fields = self.fields(r, t).await?;
                let Some(f) = fields.iter().find(|f| matches!(f.ty.as_str(), "int" | "long")) else {
                    return Ok(None);
                };
                let ty = if matches!(d, Def::Promote { .. }) {
                    "long"
                } else {
                    "int"
                };
                definition.insert(
                    "fields".into(),
                    json!([{"field_id": f.id, "new": false, "position": f.position, "name": f.name, "type": ty,
                            "required": f.required}]),
                );
            }
            Def::DropField { pick: p, missing } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                let fields = self.fields(r, t).await?;
                let id = if *missing {
                    777
                } else {
                    fields.iter().map(|f| f.id).max().unwrap_or(777)
                };
                definition.insert("drop_fields".into(), json!([id]));
            }
            Def::Property { pick: p, key, unset } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                let value = if *unset {
                    Value::Null
                } else {
                    json!(format!("v{}", self.files))
                };
                definition.insert("properties".into(), json!({ format!("p{key}"): value }));
            }
            Def::Spec { pick: p } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                let id = self.draw(t, "partition_field").await?;
                definition.insert(
                    "partition_spec".into(),
                    json!([{"source_ids": [1], "field_id": id, "name": format!("k_trunc_{id}"), "transform": "truncate[10]"}]),
                );
            }
            Def::Sort { pick: p } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                definition.insert(
                    "sort_order".into(),
                    json!([{"source_ids": [1], "transform": "identity", "direction": "desc", "null_order": "nulls-last"}]),
                );
            }
            Def::ClusterKey { pick: p, bad } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                definition.insert("cluster_key".into(), if *bad { json!([1, 999]) } else { json!([1]) });
            }
            Def::Append { pick: p } => {
                let Some(t) = pick(p) else { return Ok(None) };
                change.table_id = t;
                change.adds.push(self.file(t));
            }
        }
        if !definition.is_empty() {
            change.definition = Some(Value::Object(definition));
        }
        Ok(Some(Commit {
            ref_id: r,
            operation: "update".into(),
            tables: vec![change],
            app: None,
            namespaces: None,
        }))
    }
}

async fn fresh() -> Result<Pair, Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let on_protocol = connect().await?;
    let on_reference: Client = connect().await?;
    install_protocol(&on_protocol, &protocol).await?;
    install_reference(&on_protocol, &reference).await?;
    on_reference
        .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
    // One table before the branch, so that every step on either ref has something to act on.
    let seed = TableChange {
        table_id: Uuid::from_u128(0x4000),
        definition: Some(json!({
            "create": {"namespace": ["db"], "name": "seed", "location": "gs://lake"},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                       {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "int", "required": false}],
            "cluster_key": [1],
        })),
        ..TableChange::default()
    };
    let created = commit_reference(
        &on_reference,
        &Commit {
            ref_id: MAIN,
            operation: "update".into(),
            tables: vec![seed],
            app: None,
            namespaces: None,
        },
    )
    .await?;
    if !matches!(created.outcome, Outcome::Committed(_)) {
        return Err(format!("the seed table was not created: {created:?}").into());
    }
    on_reference.execute("select create_ref('b', 1)", &[]).await?;
    clone_catalog(&on_protocol, &reference, &protocol).await?;
    let pair = Pair {
        protocol,
        reference,
        on_protocol,
        on_reference,
    };
    pair.same_rows().await?;
    Ok(pair)
}

#[test]
fn definition_commits_match_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let config = Config {
        cases: std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(48),
        ..Config::default()
    };
    let mut runner = TestRunner::new(config);
    // What each kind of change did, across all cases: a kind that never commits, or never is refused when it can
    // be, would make the comparison vacuous.
    let tally: Mutex<HashMap<(String, bool), u32>> = Mutex::new(HashMap::new());
    let steps = prop::collection::vec((prop_oneof![3 => Just(MAIN), 1 => Just(BRANCH)], def()), 1..14);
    runner
        .run(&steps, |steps| {
            runtime
                .block_on(async {
                    let mut run = Run {
                        pair: fresh().await?,
                        created: 0,
                        files: 0,
                        names: HashMap::new(),
                    };
                    for (r, d) in &steps {
                        if let Some(commit) = run.resolve(*r, d).await? {
                            let outcome = run.pair.commit(&commit).await?;
                            let kind = format!("{d:?}").split([' ', '{']).next().unwrap().to_string();
                            *tally
                                .lock()
                                .unwrap()
                                .entry((kind, matches!(outcome, Outcome::Committed(_))))
                                .or_default() += 1;
                        }
                    }
                    run.pair.drop().await
                })
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))
        })
        .unwrap();
    let tally = tally.into_inner().unwrap();
    let mut kinds: Vec<_> = tally.iter().collect();
    kinds.sort();
    eprintln!("{kinds:?}");
    // At the default number of cases only: a shorter run, as for mutants, need not reach every answer.
    if std::env::var("CASES").is_ok() {
        return;
    }
    for kind in [
        "Create",
        "Rename",
        "Drop",
        "AddField",
        "Promote",
        "Property",
        "Spec",
        "Sort",
        "ClusterKey",
        "Append",
    ] {
        assert!(
            tally.get(&(kind.to_string(), true)).is_some_and(|&n| n > 0),
            "no {kind} committed"
        );
    }
    for kind in ["AddField", "Demote", "DropField", "ClusterKey"] {
        assert!(
            tally.get(&(kind.to_string(), false)).is_some_and(|&n| n > 0),
            "no {kind} refused"
        );
    }
}
