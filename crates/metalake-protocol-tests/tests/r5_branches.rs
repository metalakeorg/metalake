//! R5: the protocol's branches and merge, against the reference. Random sequences of commits on random refs
//! (files, removals, vectors, properties, fields, renames, new tables), branches and tags (at a kept snapshot or not),
//! merges (with merged vectors supplied, short, or missing) and deletions; after every step both catalogs answer
//! alike and hold the same rows.

use metalake_protocol_tests::differential::{clone_catalog, Commit, Pair, TableChange};
use metalake_protocol_tests::records::{stored_file, VectorSet};
use metalake_protocol_tests::{connect, fresh_schema, install_protocol, install_reference, quote_ident, Error};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{json, Value};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Mutex;

/// What each call answered, across all cases (see the end of the test).
static TALLY: Mutex<Option<HashMap<String, u32>>> = Mutex::new(None);

fn count(what: String) {
    *TALLY
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(what)
        .or_default() += 1;
}

const LATEST: i64 = 9223372036854775806;
const OPEN: i64 = 9223372036854775807;

#[derive(Debug, Clone)]
enum Change {
    Append(u8),
    Remove(u16),
    Vector(u16, bool),
    Property(u8),
    AddField(u8),
    Rename(u8),
    Create(u8),
}

#[derive(Debug, Clone)]
enum Step {
    Commit {
        r: u8,
        table: u16,
        change: Change,
    },
    Branch {
        from: u8,
        tag: bool,
        at: Option<u8>,
    },
    Merge {
        r: u8,
        supply: u8,
    },
    Delete {
        r: u8,
    },
    /// The same kind of change on a branch and on its parent, then a merge: where the two meet, it conflicts.
    Both {
        r: u8,
        table: u16,
        change: Change,
        supply: u8,
    },
}

fn change() -> impl Strategy<Value = Change> {
    prop_oneof![
        4 => (1u8..4).prop_map(Change::Append),
        2 => any::<u16>().prop_map(Change::Remove),
        3 => (any::<u16>(), any::<bool>()).prop_map(|(p, i)| Change::Vector(p, i)),
        2 => (0u8..3).prop_map(Change::Property),
        1 => (0u8..4).prop_map(Change::AddField),
        1 => (0u8..4).prop_map(Change::Rename),
        1 => (0u8..4).prop_map(Change::Create),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        8 => (any::<u8>(), any::<u16>(), change()).prop_map(|(r, table, change)| Step::Commit { r, table, change }),
        3 => (any::<u8>(), prop::bool::weighted(0.2), prop::option::weighted(0.3, any::<u8>()))
            .prop_map(|(from, tag, at)| Step::Branch { from, tag, at }),
        3 => (any::<u8>(), 0u8..3).prop_map(|(r, supply)| Step::Merge { r, supply }),
        1 => any::<u8>().prop_map(|r| Step::Delete { r }),
        3 => (any::<u8>(), any::<u16>(), change(), 0u8..3)
            .prop_map(|(r, table, change, supply)| Step::Both { r, table, change, supply }),
    ]
}

/// The answer of a call that is not a commit: its value, or the SQLSTATE of its refusal.
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

fn state(c: &Called) -> Option<&str> {
    c.as_ref().err().map(|e| &e[..5])
}

struct Run {
    pair: Pair,
    tables: u128,
    files: i64,
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
            tables: 0,
            files: 0,
        };
        run.create(1, 9).await?;
        Ok(run)
    }

    async fn create(&mut self, r: i32, name: u8) -> Result<(), Error> {
        self.tables += 1;
        let definition = json!({
            "create": {"namespace": ["db"], "name": format!("t{name}"), "location": "gs://lake"},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                       {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "int", "required": false}],
            "cluster_key": [1],
        });
        let change = TableChange {
            table_id: Uuid::from_u128(0x8000 + self.tables),
            definition: Some(definition),
            ..TableChange::default()
        };
        self.pair
            .commit(&Commit {
                ref_id: r,
                operation: "update".into(),
                tables: vec![change],
                app: None,
                namespaces: None,
            })
            .await?;
        Ok(())
    }

    async fn refs(&self) -> Result<Vec<i32>, Error> {
        let rows = self
            .pair
            .on_reference
            .query("select ref_id from ref order by ref_id", &[])
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Calls a function on both catalogs, the reference's in its search_path and the protocol's qualified (`@p`), and
    /// checks that they answered alike and hold the same rows.
    async fn call(
        &self,
        reference: &str,
        protocol: &str,
        params: &[&(dyn ToSql + Sync)],
        extra: &[&(dyn ToSql + Sync)],
    ) -> Result<Called, Error> {
        let by_reference = called(self.pair.on_reference.query_one(reference, params).await)?;
        let protocol = protocol.replace("@p", &quote_ident(&self.pair.protocol));
        let all: Vec<&(dyn ToSql + Sync)> = params.iter().chain(extra).copied().collect();
        let by_protocol = called(self.pair.on_protocol.query_one(&protocol, &all).await)?;
        let same = match (&by_reference, &by_protocol) {
            (Ok(a), Ok(b)) => a == b,
            (Err(_), Err(_)) => state(&by_reference) == state(&by_protocol),
            _ => false,
        };
        if !same {
            return Err(format!(
                "{reference}: the reference answered {by_reference:?} and the protocol {by_protocol:?}"
            )
            .into());
        }
        let name = ["create_ref", "delete_ref", "merge"]
            .into_iter()
            .find(|f| reference.contains(f))
            .unwrap_or("");
        count(format!(
            "{name}: {}",
            state(&by_reference).unwrap_or(match by_reference {
                Ok(None) => "null",
                _ => "ok",
            })
        ));
        if by_reference.is_err() {
            let (p, r) = (quote_ident(&self.pair.protocol), quote_ident(&self.pair.reference));
            let mut sql = String::new();
            for s in ["commit_seq_gen", "file_id_gen", "ref_id_gen"] {
                sql += &format!("select setval('{p}.{s}', last_value, is_called) from {r}.{s};");
            }
            self.pair.on_protocol.batch_execute(&sql).await?;
        }
        self.pair.same_rows().await?;
        Ok(by_reference)
    }

    async fn apply(&mut self, s: &Step) -> Result<(), Error> {
        let refs = self.refs().await?;
        let pick = |x: &u8| refs[*x as usize % refs.len()];
        match s {
            Step::Commit { r, table, change } => {
                let r = pick(r);
                if let Change::Create(name) = change {
                    return self.create(r, *name).await;
                }
                let tables: Vec<Uuid> = self
                    .pair
                    .on_reference
                    .query(
                        "select table_id from visible_table_names($1, $2) order by table_id",
                        &[&r, &LATEST],
                    )
                    .await?
                    .iter()
                    .map(|x| x.get(0))
                    .collect();
                let Some(&t) = tables.get(*table as usize % tables.len().max(1)) else {
                    return Ok(());
                };
                let mut c = TableChange {
                    table_id: t,
                    ..TableChange::default()
                };
                match change {
                    Change::Append(n) => {
                        for _ in 0..*n {
                            self.files += 1;
                            let k = self.files * 10;
                            c.adds.push(stored_file(
                                t,
                                format!("gs://lake/f{}.parquet", self.files),
                                10,
                                1,
                                k,
                                k + 9,
                            ));
                        }
                    }
                    Change::Remove(p) | Change::Vector(p, _) => {
                        let seen: Vec<(i64, i64, Option<i64>, i64)> = self
                            .pair
                            .on_reference
                            .query(
                                "select f.file_id, f.record_count, d.begin_seq, coalesce(d.cardinality, 0)
                                   from visible_files($1, $2, $3) f
                                   left join visible_dvs($1, $2, $3) d on d.file_id = f.file_id order by f.file_id",
                                &[&r, &t, &LATEST],
                            )
                            .await?
                            .iter()
                            .map(|x| (x.get(0), x.get(1), x.get(2), x.get(3)))
                            .collect();
                        let Some(&(fid, records, base_seq, cardinality)) = seen.get(*p as usize % seen.len().max(1))
                        else {
                            return Ok(());
                        };
                        if let Change::Vector(_, inline) = change {
                            let cardinality = (cardinality + 1).min(records);
                            let blob: Option<Vec<u8>> = if *inline {
                                let sql = "select dv_encode(array(select generate_series(0, $1::bigint - 1)))";
                                Some(self.pair.on_reference.query_one(sql, &[&cardinality]).await?.get(0))
                            } else {
                                None
                            };
                            self.files += 1;
                            c.vectors.push(VectorSet {
                                table_id: t,
                                file_id: fid,
                                base_seq,
                                path: blob.is_none().then(|| format!("gs://dv/{}.puffin", self.files)),
                                content_offset: blob.is_none().then_some(4),
                                content_size: blob.as_ref().map_or(40, |b| b.len() as i64),
                                cardinality,
                                inline: blob,
                            });
                        } else {
                            c.removes.push(fid);
                        }
                    }
                    Change::Property(key) => {
                        self.files += 1;
                        c.definition = Some(json!({"properties": {format!("p{key}"): format!("v{}", self.files)}}));
                    }
                    Change::AddField(name) => {
                        let id: i32 = self.draw(t).await?;
                        let position: i32 = self
                            .pair
                            .on_reference
                            .query_one(
                                "select coalesce(max(position), 0) + 1 from visible_fields($1, $2, $3)
                                  where parent_field_id is null",
                                &[&r, &t, &LATEST],
                            )
                            .await?
                            .get(0);
                        c.definition = Some(json!({"fields": [{"field_id": id, "new": true, "position": position,
                                                               "name": format!("c{name}"), "type": "string"}]}));
                    }
                    Change::Rename(name) => {
                        c.definition = Some(json!({"rename": {"namespace": ["db"], "name": format!("t{name}")}}));
                    }
                    Change::Create(_) => unreachable!(),
                }
                self.pair
                    .commit(&Commit {
                        ref_id: r,
                        operation: "update".into(),
                        tables: vec![c],
                        app: None,
                        namespaces: None,
                    })
                    .await?;
                Ok(())
            }
            Step::Branch { from, tag, at } => {
                let from = pick(from);
                let at: Option<i64> = match at {
                    None => None,
                    Some(a) => {
                        // A snapshot of the parent, or now and then a sequence that is not one.
                        let rows = self
                            .pair
                            .on_reference
                            .query(
                                "select distinct snapshot_id from snapshot where ref_id = $1 order by 1",
                                &[&from],
                            )
                            .await?;
                        if rows.is_empty() || a % 5 == 0 {
                            Some(i64::from(*a) + 1)
                        } else {
                            Some(rows[*a as usize % rows.len()].get(0))
                        }
                    }
                };
                let kind = if *tag { "tag" } else { "branch" };
                let name = format!("b{}", refs.len());
                let _ = self
                    .call(
                        "select create_ref($1, $2, $3, $4)::bigint",
                        "select @p.create_ref(name => $1, parent => $2, kind => $3, at => $4)::bigint",
                        &[&name, &from, &kind, &at],
                        &[],
                    )
                    .await?;
                Ok(())
            }
            Step::Merge { r, supply } => {
                let b = pick(r);
                let mut vectors = Vec::new();
                if *supply > 0 {
                    // The files whose vector both sides changed, with a merged vector holding both, or one short.
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
                            &[&b, &LATEST, &OPEN],
                        )
                        .await?;
                    for row in rows {
                        self.files += 1;
                        let cardinality: i64 = row.get(2);
                        vectors.push(VectorSet {
                            table_id: row.get(0),
                            file_id: row.get(1),
                            base_seq: None,
                            path: Some(format!("gs://dv/m{}.puffin", self.files)),
                            content_offset: Some(4),
                            content_size: 40,
                            cardinality: if *supply == 2 { cardinality - 1 } else { cardinality },
                            inline: None,
                        });
                    }
                }
                let dvs: Vec<Value> = vectors
                    .iter()
                    .map(|v| {
                        json!({"table_id": v.table_id, "file_id": v.file_id, "path": v.path,
                               "content_offset": v.content_offset, "content_size": v.content_size,
                               "cardinality": v.cardinality, "inline": null})
                    })
                    .collect();
                let args = json!({"author": "test", "dvs": dvs});
                let _ = self.call(
                    "select merge($1, $2::jsonb)",
                    "select commit_seq from @p.merge(branch => $1, vectors => $3, author => $2::jsonb ->> 'author')",
                    &[&b, &args],
                    &[&vectors],
                )
                .await?;
                Ok(())
            }
            Step::Both {
                r,
                table,
                change,
                supply,
            } => {
                // A live branch with a parent, chosen among those there are.
                let branches: Vec<(i32, i32)> = self
                    .pair
                    .on_reference
                    .query(
                        "select x.ref_id, b.parent_ref_id from ref x join ref_base b on b.ref_id = x.ref_id
                          where b.end_seq = $1 and x.kind = 'branch' and not x.deleted order by 1",
                        &[&OPEN],
                    )
                    .await?
                    .iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .collect();
                let Some(&(b, p)) = branches.get(*r as usize % branches.len().max(1)) else {
                    return Ok(());
                };
                let index = |id: i32| refs.iter().position(|&x| x == id).unwrap() as u8;
                let (on_parent, on_branch) = (index(p), index(b));
                Box::pin(self.apply(&Step::Commit {
                    r: on_parent,
                    table: *table,
                    change: change.clone(),
                }))
                .await?;
                Box::pin(self.apply(&Step::Commit {
                    r: on_branch,
                    table: *table,
                    change: change.clone(),
                }))
                .await?;
                Box::pin(self.apply(&Step::Merge {
                    r: on_branch,
                    supply: *supply,
                }))
                .await
            }
            Step::Delete { r } => {
                let r = pick(r);
                let _ = self
                    .call(
                        "select 1::bigint from (select delete_ref($1)) x",
                        "select 1::bigint from (select @p.delete_ref($1)) x",
                        &[&r],
                        &[],
                    )
                    .await?;
                Ok(())
            }
        }
    }

    async fn draw(&self, t: Uuid) -> Result<i32, Error> {
        let sql = "select next_id($1, 'field')";
        let by_reference: i32 = self.pair.on_reference.query_one(sql, &[&t]).await?.get(0);
        // next_id is the reference's, and resolves through the catalog's search_path.
        let p = &self.pair.on_protocol;
        p.batch_execute(&format!("set search_path = {}", quote_ident(&self.pair.protocol)))
            .await?;
        let by_protocol: i32 = p.query_one(sql, &[&t]).await?.get(0);
        p.batch_execute("reset search_path").await?;
        if by_reference != by_protocol {
            return Err(format!("next_id drew {by_reference} and {by_protocol}").into());
        }
        Ok(by_reference)
    }
}

#[test]
fn branches_and_merges_match_the_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cases = std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(32);
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
    eprintln!("{lines:#?}");
    for what in [
        "create_ref: ok",
        "create_ref: 22023",
        "merge: ok",
        "merge: null",
        "merge: 22023",
        "merge: 40001",
        "delete_ref: ok",
        "delete_ref: 22023",
    ] {
        assert!(tally.contains_key(what), "no {what}");
    }
}
