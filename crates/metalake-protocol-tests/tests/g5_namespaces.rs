//! G5: the protocol's commit of namespaces (revision 0.1, NS-1 to NS-5), against the reference. Random sequences of
//! namespace changes (created with properties, properties set and removed, dropped, created again, parent and child
//! in one commit) and of tables created, renamed and dropped across namespaces that exist, were dropped or never
//! existed, on main and on a branch; after every commit both catalogs answer alike and hold the same rows.

use std::collections::HashMap;
use std::sync::Mutex;

use metalake_protocol_tests::differential::{clone_catalog, Commit, Outcome, Pair, TableChange};
use metalake_protocol_tests::{connect, fresh_schema, install_protocol, install_reference, quote_ident, Error};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{json, Value};
use uuid::Uuid;

const LATEST: i64 = 9223372036854775806;
const MAIN: i32 = 1;
const BRANCH: i32 = 2;

/// The namespaces steps draw from: `zz` is never created.
const SPACES: [&[&str]; 6] = [&["db"], &["a"], &["a", "b"], &["c"], &["c", "d"], &["zz"]];

#[derive(Debug, Clone)]
enum Step {
    CreateNs { ns: usize, props: bool },
    CreateWithParent { ns: usize },
    DropNs { ns: usize },
    SetProp { ns: usize, key: u8, unset: bool },
    CreateTable { ns: usize, name: u8, with_ns: bool },
    Rename { pick: u16, ns: usize, name: u8 },
    DropTable { pick: u16 },
    DropTableAndNs { pick: u16 },
}

fn step() -> impl Strategy<Value = Step> {
    let ns = 0..SPACES.len();
    prop_oneof![
        3 => (ns.clone(), any::<bool>()).prop_map(|(ns, props)| Step::CreateNs { ns, props }),
        1 => ns.clone().prop_map(|ns| Step::CreateWithParent { ns }),
        2 => ns.clone().prop_map(|ns| Step::DropNs { ns }),
        2 => (ns.clone(), 0u8..3, prop::bool::weighted(0.3)).prop_map(|(ns, key, unset)| Step::SetProp { ns, key, unset }),
        4 => (ns.clone(), 0u8..4, prop::bool::weighted(0.2))
            .prop_map(|(ns, name, with_ns)| Step::CreateTable { ns, name, with_ns }),
        2 => (any::<u16>(), ns.clone(), 0u8..4).prop_map(|(pick, ns, name)| Step::Rename { pick, ns, name }),
        1 => any::<u16>().prop_map(|pick| Step::DropTable { pick }),
        1 => any::<u16>().prop_map(|pick| Step::DropTableAndNs { pick }),
    ]
}

fn space(i: usize) -> Value {
    json!(SPACES[i])
}

struct Run {
    pair: Pair,
    created: u128,
    values: u32,
}

impl Run {
    async fn tables(&self, r: i32) -> Result<Vec<(Uuid, Vec<String>)>, Error> {
        let rows = self
            .pair
            .on_reference
            .query(
                "select table_id, namespace from visible_table_names($1, $2) order by table_id",
                &[&r, &LATEST],
            )
            .await?;
        Ok(rows.iter().map(|x| (x.get(0), x.get(1))).collect())
    }

    fn create(&mut self, ns: usize, name: u8) -> TableChange {
        self.created += 1;
        TableChange {
            table_id: Uuid::from_u128(0x6000 + self.created),
            definition: Some(json!({
                "create": {"namespace": space(ns), "name": format!("t{name}"), "location": "gs://lake"},
                "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
                "cluster_key": [1],
            })),
            ..TableChange::default()
        }
    }

    /// The commit a step describes on ref r, or None when there is nothing to act on.
    async fn resolve(&mut self, r: i32, s: &Step) -> Result<Option<Commit>, Error> {
        let tables = self.tables(r).await?;
        let pick = |p: &u16| (!tables.is_empty()).then(|| tables[*p as usize % tables.len()].clone());
        let (changes, spaces): (Vec<TableChange>, Value) = match s {
            Step::CreateNs { ns, props } => {
                self.values += 1;
                let p = if *props {
                    json!({"comment": format!("c{}", self.values)})
                } else {
                    json!({})
                };
                (
                    vec![],
                    json!([{"namespace": space(*ns), "create": true, "properties": p}]),
                )
            }
            Step::CreateWithParent { ns } => {
                let path = SPACES[*ns];
                let parent = &path[..path.len().saturating_sub(1).max(1)];
                (
                    vec![],
                    json!([{"namespace": parent, "create": true}, {"namespace": path, "create": true}]),
                )
            }
            Step::DropNs { ns } => (vec![], json!([{"namespace": space(*ns), "drop": true}])),
            Step::SetProp { ns, key, unset } => {
                self.values += 1;
                let v = if *unset {
                    Value::Null
                } else {
                    json!(format!("v{}", self.values))
                };
                (
                    vec![],
                    json!([{"namespace": space(*ns), "properties": {format!("p{key}"): v}}]),
                )
            }
            Step::CreateTable { ns, name, with_ns } => {
                let spaces = if *with_ns {
                    json!([{"namespace": space(*ns), "create": true}])
                } else {
                    json!([])
                };
                (vec![self.create(*ns, *name)], spaces)
            }
            Step::Rename { pick: p, ns, name } => {
                let Some((t, _)) = pick(p) else { return Ok(None) };
                let change = TableChange {
                    table_id: t,
                    definition: Some(json!({"rename": {"namespace": space(*ns), "name": format!("t{name}")}})),
                    ..TableChange::default()
                };
                (vec![change], json!([]))
            }
            Step::DropTable { pick: p } | Step::DropTableAndNs { pick: p } => {
                let Some((t, ns)) = pick(p) else { return Ok(None) };
                let change = TableChange {
                    table_id: t,
                    definition: Some(json!({"drop": true})),
                    ..TableChange::default()
                };
                let spaces = if matches!(s, Step::DropTableAndNs { .. }) {
                    json!([{"namespace": ns, "drop": true}])
                } else {
                    json!([])
                };
                (vec![change], spaces)
            }
        };
        Ok(Some(Commit {
            ref_id: r,
            operation: "update".into(),
            tables: changes,
            app: None,
            namespaces: Some(spaces),
        }))
    }
}

async fn fresh() -> Result<Pair, Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let on_protocol = connect().await?;
    let on_reference = connect().await?;
    install_protocol(&on_protocol, &protocol).await?;
    install_reference(&on_protocol, &reference).await?;
    on_reference
        .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
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
fn commits_of_namespaces_match_the_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cases = std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(48);
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    // What each kind of step did, printed: the refusals a run may not reach are deterministic tests below.
    let tally: Mutex<HashMap<(String, String), u32>> = Mutex::new(HashMap::new());
    let steps = prop::collection::vec((prop_oneof![3 => Just(MAIN), 1 => Just(BRANCH)], step()), 1..20);
    runner
        .run(&steps, |steps| {
            runtime
                .block_on(async {
                    let mut run = Run {
                        pair: fresh().await?,
                        created: 0,
                        values: 0,
                    };
                    for (r, s) in &steps {
                        if let Some(commit) = run.resolve(*r, s).await? {
                            let outcome = run.pair.commit(&commit).await?;
                            let kind = format!("{s:?}").split([' ', '{']).next().unwrap().to_string();
                            let answer = match outcome {
                                Outcome::Committed(_) => "ok".to_string(),
                                Outcome::Refused(state) => state,
                            };
                            *tally.lock().unwrap().entry((kind, answer)).or_default() += 1;
                        }
                    }
                    run.pair.drop().await
                })
                .map_err(|e: Error| TestCaseError::fail(format!("{e:?}")))
        })
        .unwrap();
    let tally = tally.into_inner().unwrap();
    let mut lines: Vec<_> = tally.iter().collect();
    lines.sort();
    eprintln!("{lines:?}");
}

/// LAY-5: both refuse to commit to a catalog whose writer_version they do not know, and to one of version 0.
#[tokio::test]
async fn a_writer_refuses_an_unknown_writer_version() -> Result<(), Error> {
    let pair = fresh().await?;
    let commit = Commit {
        ref_id: MAIN,
        operation: "update".into(),
        tables: vec![],
        app: None,
        namespaces: Some(json!([{"namespace": ["w"], "create": true}])),
    };
    for change in [
        "update meta set value = '3' where key = 'writer_version'",
        "delete from meta where key = 'writer_version'",
    ] {
        pair.both(change).await?;
        if pair.commit(&commit).await? != Outcome::Refused("0A000".into()) {
            return Err(format!("after `{change}` the commit was not refused with 0A000").into());
        }
    }
    pair.drop().await
}

/// The refusals a short random run may not reach: a path that is no namespace (NS-1), a dropped namespace named
/// before one that never existed (NS-2, in the order the commit names them), and a table created in a dropped
/// namespace or a namespace dropped with a table in it (NS-3, a conflict).
#[tokio::test]
async fn namespace_refusals_match_the_reference() -> Result<(), Error> {
    let mut run = Run {
        pair: fresh().await?,
        created: 0,
        values: 0,
    };
    let commit = |tables: Vec<TableChange>, spaces: Value| Commit {
        ref_id: MAIN,
        operation: "update".into(),
        tables,
        app: None,
        namespaces: Some(spaces),
    };
    let c = 3;
    let steps = [
        (commit(vec![], json!([{"namespace": [], "create": true}])), "22023"),
        (
            commit(vec![], json!([{"namespace": ["a", ""], "create": true}])),
            "22023",
        ),
        (commit(vec![], json!([{"namespace": ["c"], "create": true}])), "ok"),
        (commit(vec![], json!([{"namespace": ["c"], "drop": true}])), "ok"),
        (
            commit(
                vec![],
                json!([{"namespace": ["c"], "properties": {"p": "v"}},
                                  {"namespace": ["zz"], "properties": {"p": "v"}}]),
            ),
            "40001",
        ),
        (commit(vec![run.create(c, 0)], json!([])), "40001"),
        (commit(vec![run.create(0, 1)], json!([])), "ok"),
        (commit(vec![], json!([{"namespace": ["db"], "drop": true}])), "40001"),
    ];
    for (i, (commit, expected)) in steps.iter().enumerate() {
        let answer = match run.pair.commit(commit).await? {
            Outcome::Committed(_) => "ok".to_string(),
            Outcome::Refused(state) => state,
        };
        if answer != *expected {
            return Err(format!("step {i}: expected {expected}, answered {answer}").into());
        }
    }
    run.pair.drop().await
}
