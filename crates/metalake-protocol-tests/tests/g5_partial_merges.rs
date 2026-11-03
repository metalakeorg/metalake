//! G5: the protocol's partial merge (revision 0.1, MRG-6 to MRG-8), against the reference. Four tables, two in `db`
//! and two in `a`; random changes on main and on a branch (properties, files, vectors, fields, namespace properties,
//! the same change on both sides, commits that span two tables), then merges of a random selection of tables and
//! namespaces, split or not, with merged vectors supplied or not; after every call both catalogs answer alike and
//! hold the same rows. A second test checks that `merge_report` answers what the merge then does.

use std::collections::HashMap;
use std::sync::Mutex;

use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog};
use metalake_protocol_tests::records::{VectorSet, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

const LATEST: i64 = 9223372036854775806;
const OPEN: i64 = 9223372036854775807;
const MAIN: i32 = 1;
const TABLES: [u128; 4] = [0xb001, 0xb002, 0xb003, 0xb004];
const SPACES: [&str; 2] = ["db", "a"];

static TALLY: Mutex<Option<HashMap<String, u32>>> = Mutex::new(None);

fn count(what: String) {
    *TALLY
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(what)
        .or_default() += 1;
}

#[derive(Debug, Clone)]
enum Kind {
    Prop(u8),
    Append,
    Vector(bool),
    Field(u8),
}

#[derive(Debug, Clone)]
enum Step {
    Change {
        on_branch: bool,
        table: usize,
        kind: Kind,
    },
    Both {
        table: usize,
        kind: Kind,
    },
    Span {
        first: usize,
        second: usize,
    },
    NsProp {
        on_branch: bool,
        ns: usize,
        key: u8,
    },
    Merge {
        tables: u8,
        spaces: u8,
        split: bool,
        supply: bool,
    },
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        3 => (0u8..3).prop_map(Kind::Prop),
        2 => Just(Kind::Append),
        2 => any::<bool>().prop_map(Kind::Vector),
        1 => (0u8..3).prop_map(Kind::Field),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (prop::bool::weighted(0.7), 0usize..4, kind()).prop_map(|(on_branch, table, kind)| Step::Change { on_branch, table, kind }),
        2 => (0usize..4, kind()).prop_map(|(table, kind)| Step::Both { table, kind }),
        1 => (0usize..4, 0usize..4).prop_map(|(first, second)| Step::Span { first, second }),
        1 => (any::<bool>(), 0usize..2, 0u8..2).prop_map(|(on_branch, ns, key)| Step::NsProp { on_branch, ns, key }),
        3 => (0u8..16, 0u8..4, prop::bool::weighted(0.3), any::<bool>())
            .prop_map(|(tables, spaces, split, supply)| Step::Merge { tables, spaces, split, supply }),
    ]
}

type Called = Result<Option<i64>, String>;

fn called(result: Result<tokio_postgres::Row, tokio_postgres::Error>) -> Result<Called, Error> {
    match result {
        Ok(row) => Ok(Ok(row.get(0))),
        Err(e) => match e.as_db_error() {
            Some(db) => Ok(Err(format!(
                "{} {} ({})",
                db.code().code(),
                db.message(),
                db.where_().unwrap_or("")
            ))),
            None => Err(e.into()),
        },
    }
}

struct Run {
    pair: Pair,
    branch: i32,
    n: i64,
}

impl Run {
    async fn new() -> Result<Self, Error> {
        let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
        let on_protocol = connect().await?;
        let on_reference = connect().await?;
        install_protocol(&on_protocol, &protocol).await?;
        install_reference(&on_protocol, &reference).await?;
        on_reference
            .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
            .await?;
        clone_catalog(&on_protocol, &reference, &protocol).await?;
        let mut run = Run {
            pair: Pair {
                protocol,
                reference,
                on_protocol,
                on_reference,
            },
            branch: 0,
            n: 0,
        };
        let tables: Vec<TableChange> = TABLES
            .iter()
            .enumerate()
            .map(|(i, t)| TableChange {
                table_id: Uuid::from_u128(*t),
                definition: Some(json!({
                    "create": {"namespace": [SPACES[i / 2]], "name": format!("t{i}"), "location": "gs://lake"},
                    "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
                    "cluster_key": [1],
                })),
                adds: vec![stored_file(Uuid::from_u128(*t), format!("gs://lake/seed{i}.parquet"), 10, 1, 0, 9)],
                ..TableChange::default()
            })
            .collect();
        run.commit(MAIN, tables, Some(json!([{"namespace": ["a"], "create": true}])))
            .await?;
        let b: i32 = run
            .pair
            .on_reference
            .query_one("select create_ref('b', 1)", &[])
            .await?
            .get(0);
        let p = quote_ident(&run.pair.protocol);
        let mirrored: i32 = run
            .pair
            .on_protocol
            .query_one(&format!("select {p}.create_ref(name => 'b', parent => 1)"), &[])
            .await?
            .get(0);
        if b != mirrored {
            return Err(format!("the branch is {b} and {mirrored}").into());
        }
        run.branch = b;
        run.pair.same_rows().await?;
        Ok(run)
    }

    async fn commit(&self, r: i32, tables: Vec<TableChange>, namespaces: Option<Value>) -> Result<Outcome, Error> {
        let c = Commit {
            ref_id: r,
            operation: "update".into(),
            tables,
            app: None,
            namespaces,
        };
        self.pair.commit(&c).await
    }

    async fn change(&mut self, r: i32, table: usize, kind: &Kind) -> Result<TableChange, Error> {
        let t = Uuid::from_u128(TABLES[table]);
        let mut c = TableChange {
            table_id: t,
            ..TableChange::default()
        };
        self.n += 1;
        match kind {
            Kind::Prop(k) => c.definition = Some(json!({"properties": {format!("p{k}"): format!("v{}", self.n)}})),
            Kind::Append => c
                .adds
                .push(stored_file(t, format!("gs://lake/f{}.parquet", self.n), 10, 1, 0, 9)),
            Kind::Field(name) => {
                let sql = "select next_id($1, 'field')";
                let id: i32 = self.pair.on_reference.query_one(sql, &[&t]).await?.get(0);
                let p = &self.pair.on_protocol;
                p.batch_execute(&format!("set search_path = {}", quote_ident(&self.pair.protocol)))
                    .await?;
                let mirrored: i32 = p.query_one(sql, &[&t]).await?.get(0);
                p.batch_execute("reset search_path").await?;
                if id != mirrored {
                    return Err(format!("next_id drew {id} and {mirrored}").into());
                }
                c.definition = Some(json!({"fields": [{"field_id": id, "new": true, "position": 2 + id,
                                                       "name": format!("c{name}"), "type": "int"}]}));
            }
            Kind::Vector(inline) => {
                let row = self
                    .pair
                    .on_reference
                    .query_one(
                        "select f.file_id, d.begin_seq, coalesce(d.cardinality, 0)
                           from visible_files($1, $2, $3) f left join visible_dvs($1, $2, $3) d on d.file_id = f.file_id
                          order by f.file_id limit 1",
                        &[&r, &t, &LATEST],
                    )
                    .await?;
                let (fid, base_seq, cardinality): (i64, Option<i64>, i64) = (row.get(0), row.get(1), row.get(2));
                let cardinality = (cardinality + 1).min(10);
                // Each side deletes rows of its own, so that a union is larger than either.
                let lo: i64 = if r == MAIN { 5 } else { 0 };
                let blob: Option<Vec<u8>> = if *inline {
                    let sql =
                        "select dv_encode(array(select generate_series($1::bigint, $1::bigint + $2::bigint - 1)))";
                    Some(
                        self.pair
                            .on_reference
                            .query_one(sql, &[&lo, &cardinality.min(5)])
                            .await?
                            .get(0),
                    )
                } else {
                    None
                };
                c.vectors.push(VectorSet {
                    table_id: t,
                    file_id: fid,
                    base_seq,
                    path: blob.is_none().then(|| format!("gs://dv/{}.puffin", self.n)),
                    content_offset: blob.is_none().then_some(4),
                    content_size: blob.as_ref().map_or(40, |b| b.len() as i64),
                    cardinality: if *inline { cardinality.min(5) } else { cardinality },
                    inline: blob,
                });
            }
        }
        Ok(c)
    }

    /// The merged vectors a merge of the branch needs: one per file whose vector both sides changed.
    async fn supplied(&mut self) -> Result<Vec<VectorSet>, Error> {
        let rows = self
            .pair
            .on_reference
            .query(
                "select x.table_id, x.file_id,
                        greatest(x.cardinality, coalesce((version_at('deletion_vector', k, rb.parent_ref_id, $2)
                                                          ->> 'cardinality')::bigint, 0))
                   from deletion_vector x
                   join ref_base rb on rb.ref_id = x.ref_id and rb.end_seq = $3
                  cross join lateral (select jsonb_build_object('table_id', x.table_id, 'file_id', x.file_id) as k) kk
                  where x.ref_id = $1 and x.end_seq = $3
                    and version_at('deletion_vector', k, rb.parent_ref_id, rb.fork_seq)
                        is distinct from version_at('deletion_vector', k, rb.parent_ref_id, $2)",
                &[&self.branch, &LATEST, &OPEN],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                self.n += 1;
                VectorSet {
                    table_id: row.get(0),
                    file_id: row.get(1),
                    base_seq: None,
                    path: Some(format!("gs://dv/m{}.puffin", self.n)),
                    content_offset: Some(4),
                    content_size: 40,
                    cardinality: row.get(2),
                    inline: None,
                }
            })
            .collect())
    }

    async fn merge(&mut self, tables: u8, spaces: u8, split: bool, supply: bool) -> Result<String, Error> {
        let vectors = if supply { self.supplied().await? } else { vec![] };
        let picked: Vec<Uuid> = (0..4)
            .filter(|i| tables & (1 << i) != 0)
            .map(|i| Uuid::from_u128(TABLES[i]))
            .collect();
        let picked_spaces: Vec<Value> = (0..2)
            .filter(|i| spaces & (1 << i) != 0)
            .map(|i| json!([SPACES[i]]))
            .collect();
        let selective = tables != 0 || spaces != 0;
        let dvs: Vec<Value> = vectors
            .iter()
            .map(|v| {
                json!({"table_id": v.table_id, "file_id": v.file_id, "path": v.path, "content_offset": v.content_offset,
                       "content_size": v.content_size, "cardinality": v.cardinality, "inline": null})
            })
            .collect();
        let mut args = json!({"dvs": dvs, "split": split});
        if selective {
            args["only"] = json!({"tables": picked, "namespaces": picked_spaces});
        }
        let by_reference = called(
            self.pair
                .on_reference
                .query_one("select merge($1, $2)", &[&self.branch, &args])
                .await,
        )?;
        let sql = format!(
            "select commit_seq from {}.merge(branch => $1, vectors => $2, only_tables => $3, only_namespaces => $4, \
             split => $5)",
            quote_ident(&self.pair.protocol)
        );
        let only_tables: Option<Vec<Uuid>> = selective.then_some(picked);
        let only_spaces: Option<Value> = selective.then(|| json!(picked_spaces));
        let params: [&(dyn ToSql + Sync); 5] = [&self.branch, &vectors, &only_tables, &only_spaces, &split];
        let by_protocol = called(self.pair.on_protocol.query_one(&sql, &params).await)?;
        let state = |c: &Called| c.as_ref().err().map(|e| e[..5].to_string());
        let same = match (&by_reference, &by_protocol) {
            (Ok(a), Ok(b)) => a == b,
            _ => state(&by_reference) == state(&by_protocol),
        };
        if !same {
            return Err(format!(
                "merge {args}: the reference answered {by_reference:?} and the protocol {by_protocol:?}"
            )
            .into());
        }
        let answer = format!(
            "{} {}",
            if selective { "partial" } else { "full" },
            match &by_reference {
                Ok(Some(_)) => "ok".to_string(),
                Ok(None) => "null".to_string(),
                Err(e) => e[..5].to_string(),
            }
        );
        count(answer.clone());
        if by_reference.is_err() {
            self.align().await?;
        }
        self.pair.same_rows().await?;
        Ok(answer)
    }

    async fn align(&self) -> Result<(), Error> {
        let (p, r) = (quote_ident(&self.pair.protocol), quote_ident(&self.pair.reference));
        let mut sql = String::new();
        for s in ["commit_seq_gen", "file_id_gen", "ref_id_gen"] {
            sql += &format!("select setval('{p}.{s}', last_value, is_called) from {r}.{s};");
        }
        self.pair.on_protocol.batch_execute(&sql).await?;
        Ok(())
    }

    async fn apply(&mut self, s: &Step) -> Result<(), Error> {
        let b = self.branch;
        match s {
            Step::Change { on_branch, table, kind } => {
                let r = if *on_branch { b } else { MAIN };
                let c = self.change(r, *table, kind).await?;
                self.commit(r, vec![c], None).await?;
            }
            Step::Both { table, kind } => {
                let c = self.change(MAIN, *table, kind).await?;
                self.commit(MAIN, vec![c], None).await?;
                let c = self.change(b, *table, kind).await?;
                self.commit(b, vec![c], None).await?;
            }
            Step::Span { first, second } => {
                if first != second {
                    let one = self.change(b, *first, &Kind::Prop(9)).await?;
                    let two = self.change(b, *second, &Kind::Prop(9)).await?;
                    self.commit(b, vec![one, two], None).await?;
                }
            }
            Step::NsProp { on_branch, ns, key } => {
                self.n += 1;
                let spaces =
                    json!([{"namespace": [SPACES[*ns]], "properties": {format!("n{key}"): format!("v{}", self.n)}}]);
                self.commit(if *on_branch { b } else { MAIN }, vec![], Some(spaces))
                    .await?;
            }
            Step::Merge {
                tables,
                spaces,
                split,
                supply,
            } => {
                self.merge(*tables, *spaces, *split, *supply).await?;
            }
        }
        Ok(())
    }
}

#[test]
fn partial_merges_match_the_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cases = std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(48);
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    runner
        .run(&prop::collection::vec(step(), 1..24), |steps| {
            runtime
                .block_on(async {
                    let mut run = Run::new().await?;
                    for s in &steps {
                        run.apply(s).await?;
                    }
                    run.pair.drop().await
                })
                .map_err(|e: Error| TestCaseError::fail(format!("{e:?}")))
        })
        .unwrap();
    let tally = TALLY.lock().unwrap().take().unwrap_or_default();
    let mut lines: Vec<_> = tally.iter().collect();
    lines.sort();
    eprintln!("{lines:?}");
}

/// `merge_report` tries a merge without effect: it answers what the merge then does, and changes nothing.
#[tokio::test]
async fn a_merge_report_answers_what_the_merge_does() -> Result<(), Error> {
    let mut run = Run::new().await?;
    let b = run.branch;
    for (table, kind) in [(0, Kind::Prop(0)), (1, Kind::Append)] {
        let c = run.change(b, table, &kind).await?;
        run.commit(b, vec![c], None).await?;
    }
    run.apply(&Step::Span { first: 2, second: 3 }).await?;
    let p = quote_ident(&run.pair.protocol);
    let report = |tables: Vec<Uuid>| {
        format!(
            "select {p}.merge_report(branch => {b}, only_tables => array[{}]::uuid[])",
            tables.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", ")
        )
    };
    let t: Vec<Uuid> = TABLES.iter().map(|x| Uuid::from_u128(*x)).collect();
    // One commit changed t2 and t3: a selection that cuts it fails.
    let refused: Value = run
        .pair
        .on_protocol
        .query_one(&report(vec![t[0], t[2]]), &[])
        .await?
        .get(0);
    run.align().await?;
    run.pair.same_rows().await?;
    if refused["ok"] != json!(false) || refused["state"] != json!("40001") || refused["rule"] != json!("MRG-7") {
        return Err(format!("the report of a merge that cuts a commit: {refused}").into());
    }
    // With both, it goes through: the report names what is merged and what is rebased.
    let ok: Value = run
        .pair
        .on_protocol
        .query_one(&report(vec![t[0], t[2], t[3]]), &[])
        .await?
        .get(0);
    run.align().await?;
    run.pair.same_rows().await?;
    let tables = |v: &Value| {
        v.as_array()
            .map(|a| a.iter().map(|x| x.as_str().unwrap().to_string()).collect::<Vec<_>>())
    };
    if ok["ok"] != json!(true)
        || tables(&ok["merged"]) != Some(vec![t[0].to_string(), t[2].to_string(), t[3].to_string()])
        || tables(&ok["rebased"]) != Some(vec![t[1].to_string()])
    {
        return Err(format!("the report of a merge that goes through: {ok}").into());
    }
    // And the merge does it.
    run.merge(0b1101, 0, false, false).await?;
    run.pair.drop().await
}

/// What a short random run may not reach, each on a fresh pair: a selection of a table the branch did not change
/// (MRG-6), a split commit allowed by `split`, a vector of the rest both sides changed (united on the branch), and a
/// field both sides added under one name to a table of the rest (the rebase leaves it invalid, MRG-8).
#[tokio::test]
async fn partial_merges_the_random_run_may_miss() -> Result<(), Error> {
    let scenes: [(&[Step], u8, bool, &str); 4] = [
        (
            &[Step::Change {
                on_branch: true,
                table: 0,
                kind: Kind::Prop(0),
            }],
            0b0010,
            false,
            "partial 22023",
        ),
        (
            &[
                Step::Change {
                    on_branch: true,
                    table: 0,
                    kind: Kind::Prop(0),
                },
                Step::Span { first: 2, second: 3 },
            ],
            0b0101,
            true,
            "partial ok",
        ),
        (
            &[
                Step::Both {
                    table: 1,
                    kind: Kind::Vector(true),
                },
                Step::Change {
                    on_branch: true,
                    table: 0,
                    kind: Kind::Prop(0),
                },
            ],
            0b0001,
            false,
            "partial ok",
        ),
        (
            &[
                Step::Both {
                    table: 1,
                    kind: Kind::Field(0),
                },
                Step::Change {
                    on_branch: true,
                    table: 0,
                    kind: Kind::Prop(0),
                },
            ],
            0b0001,
            false,
            "partial 40001",
        ),
    ];
    for (i, (steps, tables, split, expected)) in scenes.iter().enumerate() {
        let mut run = Run::new().await?;
        for s in steps.iter() {
            run.apply(s).await?;
        }
        let answer = run.merge(*tables, 0, *split, false).await?;
        if answer != *expected {
            return Err(format!("scene {i}: the merge answered {answer}, not {expected}").into());
        }
        run.pair.drop().await?;
    }
    Ok(())
}
