//! R2: the protocol's commit of data, against the reference. Random sequences of commits on a scene the reference
//! sets up (two tables with files on `main`, a branch and a tag): appends, inline files and footers, removals,
//! deletion vectors, `replace`, `expected`, idempotency and intents, valid and faulty. After every commit both
//! catalogs must have answered alike and hold the same rows.

use std::collections::HashMap;

use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog};
use metalake_protocol_tests::records::{FileAdd, VectorSet, encode_long, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::json;
use tokio_postgres::Client;
use uuid::Uuid;

const LATEST: i64 = 9223372036854775806;
const MAIN: i32 = 1;
const BRANCH: i32 = 2;
const TAG: i32 = 3;

fn tables() -> [Uuid; 2] {
    [Uuid::from_u128(0x1001), Uuid::from_u128(0x1002)]
}

/// A file the reference would refuse, each for one rule the protocol must enforce too.
#[derive(Debug, Clone, Copy)]
enum Fault {
    RepeatedField,
    NanOnLong,
    NoClusterKey,
    NoSpec,
    NoSortOrder,
    FooterSize,
    InlineSize,
    PathAndInline,
    ExpiredIntent,
}

#[derive(Debug, Clone, Copy)]
enum VectorFault {
    None,
    StaleBase,
    Shrinks,
    TooMany,
    Missing,
}

#[derive(Debug, Clone)]
enum Act {
    Append {
        files: u8,
        inline: u8,
        footer: bool,
    },
    Remove {
        picks: Vec<u16>,
    },
    RemoveMissing,
    Vector {
        pick: u16,
        grow: u8,
        inline: bool,
        fault: VectorFault,
    },
    Replace {
        picks: Vec<u16>,
        off_by_one: bool,
    },
    Faulty(Fault),
}

#[derive(Debug, Clone)]
struct Step {
    on: i32,
    changes: Vec<(usize, Act)>,
    expect: Option<bool>,
    app: Option<(u8, bool)>,
    intent: bool,
}

fn act() -> impl Strategy<Value = Act> {
    prop_oneof![
        4 => (1u8..4, 0u8..2, any::<bool>()).prop_map(|(files, inline, footer)| Act::Append { files, inline, footer }),
        2 => prop::collection::vec(any::<u16>(), 1..3).prop_map(|picks| Act::Remove { picks }),
        1 => Just(Act::RemoveMissing),
        3 => (any::<u16>(), 0u8..4, any::<bool>(), prop_oneof![
                4 => Just(VectorFault::None), 1 => Just(VectorFault::StaleBase), 1 => Just(VectorFault::Shrinks),
                1 => Just(VectorFault::TooMany), 1 => Just(VectorFault::Missing)])
            .prop_map(|(pick, grow, inline, fault)| Act::Vector { pick, grow, inline, fault }),
        1 => (prop::collection::vec(any::<u16>(), 1..3), prop::bool::weighted(0.25))
            .prop_map(|(picks, off_by_one)| Act::Replace { picks, off_by_one }),
        1 => prop_oneof![
                Just(Fault::RepeatedField), Just(Fault::NanOnLong), Just(Fault::NoClusterKey), Just(Fault::NoSpec),
                Just(Fault::NoSortOrder), Just(Fault::FooterSize), Just(Fault::InlineSize), Just(Fault::PathAndInline),
                Just(Fault::ExpiredIntent)]
            .prop_map(Act::Faulty),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    (
        prop_oneof![6 => Just(MAIN), 3 => Just(BRANCH), 1 => Just(TAG)],
        act(),
        prop::option::weighted(0.3, act()),
        prop::option::weighted(0.2, any::<bool>()),
        prop::option::weighted(0.2, (0u8..2, prop::bool::weighted(0.3))),
        prop::bool::weighted(0.2),
        any::<bool>(),
    )
        .prop_map(|(on, first, second, expect, app, intent, first_table)| {
            let t = usize::from(!first_table);
            let mut changes = vec![(t, first)];
            if let Some(second) = second {
                // A replace stands alone, so that a faulty commit has one fault the reference meets first.
                if !matches!(changes[0].1, Act::Replace { .. }) && !matches!(second, Act::Replace { .. }) {
                    changes.push((1 - t, second));
                }
            }
            Step {
                on,
                changes,
                expect,
                app,
                intent,
            }
        })
}

/// A visible file of a ref and table, as the reference sees it now.
struct Visible {
    file_id: i64,
    records: i64,
    vector_seq: Option<i64>,
    vector_cardinality: i64,
}

async fn visible(client: &Client, r: i32, t: Uuid) -> Result<Vec<Visible>, Error> {
    let rows = client
        .query(
            "select file_id, record_count, (file_dv($1, $2, $3, file_id)).begin_seq,
                    coalesce((file_dv($1, $2, $3, file_id)).cardinality, 0)
               from visible_files($1, $2, $3) order by file_id",
            &[&r, &t, &LATEST],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| Visible {
            file_id: r.get(0),
            records: r.get(1),
            vector_seq: r.get(2),
            vector_cardinality: r.get(3),
        })
        .collect())
}

struct Run {
    pair: Pair,
    next: i64,
    versions: HashMap<(i32, String), i64>,
}

impl Run {
    fn path(&mut self) -> String {
        self.next += 1;
        format!("gs://lake/f{}.parquet", self.next)
    }

    fn file(&mut self, t: Uuid) -> FileAdd {
        self.next += 1;
        let k = self.next * 100;
        let mut f = stored_file(t, self.path(), 10 + self.next % 90, 1, k, k + 99);
        if t == tables()[0] {
            // The double field 2, with a NaN count, as Iceberg writes it.
            f.stat_field_ids.push(2);
            f.lower.push(Some(encode_long(k)));
            f.upper.push(Some(encode_long(k + 1)));
            f.null_count.push(Some(0));
            f.nan_count.push(Some(self.next % 3));
        }
        f
    }

    fn inline_file(&mut self, t: Uuid) -> FileAdd {
        let mut f = self.file(t);
        let content: Vec<u8> = (0..64 + self.next % 64).map(|i| i as u8).collect();
        f.path = None;
        f.file_size = content.len() as i64;
        f.content = Some(content);
        f
    }

    async fn vector_blob(&self, cardinality: i64) -> Result<Vec<u8>, Error> {
        Ok(self
            .pair
            .on_reference
            .query_one(
                "select dv_encode(array(select generate_series(0, $1::bigint - 1)))",
                &[&cardinality],
            )
            .await?
            .get(0))
    }

    async fn intent(&self, path: &str, t: Uuid, r: i32, expired: bool) -> Result<(), Error> {
        let at = if expired {
            "2000-01-01T00:00:00Z"
        } else {
            "2999-01-01T00:00:00Z"
        };
        self.pair
            .both(&format!("insert into intent values ('{path}', '{t}', {r}, '{at}')"))
            .await
    }

    /// The commit a step describes, resolved against the reference's current state.
    async fn resolve(&mut self, step: &Step) -> Result<Commit, Error> {
        let r = step.on;
        let mut operation = "append".to_string();
        let mut changes = Vec::new();
        for (i, act) in &step.changes {
            let t = tables()[*i];
            let mut change = TableChange {
                table_id: t,
                ..Default::default()
            };
            let seen = visible(&self.pair.on_reference, if r == TAG { MAIN } else { r }, t).await?;
            match act {
                Act::Append { files, inline, footer } => {
                    for n in 0..*files {
                        let mut f = if n < *inline { self.inline_file(t) } else { self.file(t) };
                        if *footer && n == 0 {
                            f.footer = Some(vec![7; f.footer_size as usize]);
                        }
                        change.adds.push(f);
                    }
                }
                Act::Remove { picks } => {
                    if seen.is_empty() {
                        change.adds.push(self.file(t));
                    }
                    let mut ids: Vec<i64> = picks
                        .iter()
                        .filter(|_| !seen.is_empty())
                        .map(|p| seen[*p as usize % seen.len()].file_id)
                        .collect();
                    ids.dedup();
                    ids.sort();
                    ids.dedup();
                    change.removes = ids;
                }
                Act::RemoveMissing => change.removes = vec![999_999_999],
                Act::Vector {
                    pick,
                    grow,
                    inline,
                    fault,
                } => {
                    if seen.is_empty() || matches!(fault, VectorFault::Missing) {
                        change.vectors.push(VectorSet {
                            table_id: t,
                            file_id: 999_999_999,
                            base_seq: None,
                            path: Some("gs://dv/x.puffin".into()),
                            content_offset: Some(4),
                            content_size: 40,
                            cardinality: 1,
                            inline: None,
                        });
                    } else {
                        let f = &seen[*pick as usize % seen.len()];
                        let cardinality = match fault {
                            VectorFault::TooMany => f.records + 1,
                            VectorFault::Shrinks if f.vector_cardinality > 0 => f.vector_cardinality - 1,
                            _ => (f.vector_cardinality + 1 + i64::from(*grow)).min(f.records),
                        };
                        let base_seq = match fault {
                            VectorFault::StaleBase => Some(f.vector_seq.unwrap_or(0) + 7),
                            _ => f.vector_seq,
                        };
                        let blob = if *inline {
                            Some(self.vector_blob(cardinality).await?)
                        } else {
                            None
                        };
                        self.next += 1;
                        change.vectors.push(VectorSet {
                            table_id: t,
                            file_id: f.file_id,
                            base_seq,
                            path: blob.is_none().then(|| format!("gs://dv/{}.puffin", self.next)),
                            content_offset: blob.is_none().then_some(4),
                            content_size: blob.as_ref().map_or(40, |b| b.len() as i64),
                            cardinality,
                            inline: blob,
                        });
                    }
                }
                Act::Replace { picks, off_by_one } => {
                    operation = "replace".into();
                    let mut ids: Vec<i64> = picks
                        .iter()
                        .filter(|_| !seen.is_empty())
                        .map(|p| seen[*p as usize % seen.len()].file_id)
                        .collect();
                    ids.sort();
                    ids.dedup();
                    let live: i64 = seen
                        .iter()
                        .filter(|f| ids.contains(&f.file_id))
                        .map(|f| f.records - f.vector_cardinality)
                        .sum();
                    let mut f = self.file(t);
                    f.record_count = live + i64::from(*off_by_one);
                    change.removes = ids;
                    change.adds.push(f);
                }
                Act::Faulty(fault) => {
                    let mut f = self.file(t);
                    match fault {
                        Fault::RepeatedField => {
                            f.stat_field_ids.push(1);
                            f.lower.push(None);
                            f.upper.push(None);
                            f.null_count.push(None);
                            f.nan_count.push(None);
                        }
                        Fault::NanOnLong => f.nan_count[0] = Some(1),
                        Fault::NoClusterKey => {
                            f.stat_field_ids = vec![];
                            f.lower = vec![];
                            f.upper = vec![];
                            f.null_count = vec![];
                            f.nan_count = vec![];
                        }
                        Fault::NoSpec => f.spec_id = 7,
                        Fault::NoSortOrder => f.sort_order_id = Some(5),
                        Fault::FooterSize => f.footer = Some(vec![1; 3]),
                        Fault::InlineSize => {
                            f = self.inline_file(t);
                            f.file_size += 1;
                        }
                        Fault::PathAndInline => {
                            f = self.inline_file(t);
                            f.path = Some(self.path());
                        }
                        Fault::ExpiredIntent => self.intent(f.path.as_deref().unwrap(), t, r, true).await?,
                    }
                    change.adds.push(f);
                }
            }
            changes.push(change);
        }
        if let Some(stale) = step.expect {
            let head: Option<i64> = self
                .pair
                .on_reference
                .query_opt(
                    "select snapshot_id from head where ref_id = $1 and table_id = $2",
                    &[&r, &changes[0].table_id],
                )
                .await?
                .map(|row| row.get(0));
            changes[0].expected = Some(if stale { Some(head.unwrap_or(0) + 1) } else { head });
        }
        let expired = step
            .changes
            .iter()
            .any(|(_, a)| matches!(a, Act::Faulty(Fault::ExpiredIntent)));
        if step.intent
            && !expired
            && let Some(path) = changes.iter().flat_map(|c| &c.adds).find_map(|f| f.path.clone())
        {
            self.intent(&path, changes[0].table_id, r, false).await?;
        }
        let app = step.app.map(|(n, replay)| {
            let key = (r, format!("app{n}"));
            let stored = *self.versions.get(&key).unwrap_or(&0);
            (key.1, if replay && stored > 0 { stored } else { stored + 1 })
        });
        Ok(Commit {
            ref_id: r,
            operation,
            tables: changes,
            app,
            namespaces: None,
        })
    }

    async fn apply(&mut self, step: &Step) -> Result<(), Error> {
        let commit = self.resolve(step).await?;
        if let (Outcome::Committed(_), Some((app, version))) = (self.pair.commit(&commit).await?, &commit.app) {
            let stored = self.versions.entry((commit.ref_id, app.clone())).or_insert(0);
            *stored = (*stored).max(*version);
        }
        Ok(())
    }
}

/// The scene: two tables created through the reference, files on `main`, a branch and a tag, copied into the
/// protocol's catalog.
async fn scene() -> Result<Pair, Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let on_protocol = connect().await?;
    let on_reference = connect().await?;
    install_protocol(&on_protocol, &protocol).await?;
    install_reference(&on_protocol, &reference).await?;
    on_reference
        .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
    let [t1, t2] = tables();
    for (t, name, fields, cluster) in [
        (
            t1,
            "t1",
            json!([{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                          {"field_id": 2, "new": true, "position": 2, "name": "x", "type": "double", "required": false}]),
            json!([1]),
        ),
        (
            t2,
            "t2",
            json!([{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}]),
            json!([1]),
        ),
    ] {
        on_reference
            .execute(
                "select commit(jsonb_build_object('ref', 1, 'author', 'scene', 'actor', 'scene', 'operation', 'create',
                   'tables', jsonb_build_array(jsonb_build_object('table_id', $1::uuid,
                     'create', jsonb_build_object('namespace', '[\"db\"]'::jsonb, 'name', $2::text, 'location', 'gs://lake'),
                     'fields', $3::jsonb, 'cluster_key', $4::jsonb))))",
                &[&t, &name, &fields, &cluster],
            )
            .await?;
    }
    let pair = Pair {
        protocol,
        reference,
        on_protocol,
        on_reference,
    };
    let mut run = Run {
        pair,
        next: 0,
        versions: HashMap::new(),
    };
    for t in tables() {
        let adds = vec![run.file(t), run.file(t), run.inline_file(t)];
        let commit = Commit {
            ref_id: MAIN,
            operation: "append".into(),
            tables: vec![TableChange {
                table_id: t,
                adds,
                ..Default::default()
            }],
            app: None,
            namespaces: None,
        };
        metalake_protocol_tests::differential::commit_reference(&run.pair.on_reference, &commit).await?;
    }
    run.pair.on_reference.execute("select create_ref('b', 1)", &[]).await?;
    run.pair
        .on_reference
        .execute("select create_ref('v', 1, 'tag')", &[])
        .await?;
    clone_catalog(&run.pair.on_protocol, &run.pair.reference, &run.pair.protocol).await?;
    run.pair.same_rows().await?;
    Ok(run.pair)
}

#[test]
fn commits_of_data_match_the_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let config = Config {
        cases: std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(24),
        ..Config::default()
    };
    let mut runner = TestRunner::new(config);
    runner
        .run(&prop::collection::vec(step(), 1..12), |steps| {
            runtime
                .block_on(async {
                    let pair = scene().await?;
                    let mut run = Run {
                        pair,
                        next: 1_000,
                        versions: HashMap::new(),
                    };
                    for step in &steps {
                        run.apply(step).await?;
                    }
                    run.pair.drop().await
                })
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))
        })
        .unwrap();
}
