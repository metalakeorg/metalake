//! R4: the protocol's `plan`, against the reference. The reference builds a random history (appends with bounds in
//! several layouts, removals, vectors, branches, merges, a field promoted from date to timestamp), the protocol's
//! catalog is cloned from it, and random reads (a ref, a sequence, a field and a range) must open the same files with
//! the same vectors as the reference's `prune` and `visible_dvs`. A second test checks with `EXPLAIN` that `plan` is
//! inlined into a prepared statement and reaches the files through the index of their bound range.

use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, clone_catalog, commit_reference};
use metalake_protocol_tests::records::{FileAdd, VectorSet, encode_long, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::json;
use tokio_postgres::Client;
use uuid::Uuid;

const LATEST: i64 = 9223372036854775806;
const T: Uuid = Uuid::from_u128(0x7000);
const DAY: i64 = 86_400_000_000;

fn encode_int32(v: i32) -> Vec<u8> {
    ((v as u32) ^ (1 << 31)).to_be_bytes().to_vec()
}

#[derive(Debug, Clone)]
enum Step {
    /// Files on a ref: `layout` orders their statistics ([k, d], [d, k], [k] or none), bounds from the seeds.
    Append {
        r: u8,
        files: Vec<(u8, u16, u8, u8, u8)>,
        inline: bool,
        footer: bool,
    },
    Remove {
        r: u8,
        pick: u16,
    },
    Vector {
        r: u8,
        pick: u16,
        inline: bool,
    },
    Branch {
        from: u8,
    },
    Merge {
        r: u8,
    },
    Promote,
    /// Drops `d`, whose bounds files keep: a read on it no longer prunes.
    DropD,
}

fn step() -> impl Strategy<Value = Step> {
    let file = (0u8..4, 0u16..1000, any::<u8>(), 0u8..100, any::<u8>());
    prop_oneof![
        5 => (any::<u8>(), prop::collection::vec(file, 1..4), prop::bool::weighted(0.2), prop::bool::weighted(0.2))
            .prop_map(|(r, files, inline, footer)| Step::Append { r, files, inline, footer }),
        2 => (any::<u8>(), any::<u16>()).prop_map(|(r, pick)| Step::Remove { r, pick }),
        2 => (any::<u8>(), any::<u16>(), any::<bool>()).prop_map(|(r, pick, inline)| Step::Vector { r, pick, inline }),
        2 => any::<u8>().prop_map(|from| Step::Branch { from }),
        1 => any::<u8>().prop_map(|r| Step::Merge { r }),
        1 => Just(Step::Promote),
        1 => Just(Step::DropD),
    ]
}

#[derive(Debug, Clone)]
struct Read {
    r: u8,
    at: Option<u8>,
    field: Option<i32>,
    lo: Option<u16>,
    hi: Option<u16>,
}

fn read() -> impl Strategy<Value = Read> {
    let field =
        prop_oneof![4 => Just(Some(1)), 3 => Just(Some(3)), 1 => Just(Some(2)), 1 => Just(Some(999)), 1 => Just(None)];
    let end = prop::option::weighted(0.8, 0u16..1100);
    (
        any::<u8>(),
        prop::option::weighted(0.3, any::<u8>()),
        field,
        end.clone(),
        end,
    )
        .prop_map(|(r, at, field, lo, hi)| Read { r, at, field, lo, hi })
}

struct Scene {
    reference: String,
    protocol: String,
    on_reference: Client,
    on_protocol: Client,
    refs: Vec<i32>,
    seqs: Vec<i64>,
    promoted: bool,
    files: i64,
}

impl Scene {
    async fn new() -> Result<Self, Error> {
        let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
        let on_protocol = connect().await?;
        let on_reference = connect().await?;
        install_protocol(&on_protocol, &protocol).await?;
        install_reference(&on_protocol, &reference).await?;
        on_reference
            .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
            .await?;
        let mut scene = Scene {
            reference,
            protocol,
            on_reference,
            on_protocol,
            refs: vec![1],
            seqs: vec![],
            promoted: false,
            files: 0,
        };
        let create = json!({
            "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                       {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "int", "required": false},
                       {"field_id": 3, "new": true, "position": 3, "name": "d", "type": "date", "required": false}],
            "cluster_key": [1],
        });
        scene
            .commit(
                1,
                TableChange {
                    table_id: T,
                    definition: Some(create),
                    ..TableChange::default()
                },
            )
            .await?;
        if scene.seqs.is_empty() {
            return Err("the table was not created".into());
        }
        Ok(scene)
    }

    /// Commits through the reference; a refusal is part of a random history and leaves it as it was.
    async fn commit(&mut self, r: i32, change: TableChange) -> Result<(), Error> {
        let c = Commit {
            ref_id: r,
            operation: "update".into(),
            tables: vec![change],
            app: None,
            namespaces: None,
        };
        if let Outcome::Committed(seq) = commit_reference(&self.on_reference, &c).await?.outcome {
            self.seqs.push(seq);
        }
        Ok(())
    }

    async fn visible(&self, r: i32) -> Result<Vec<(i64, i64, Option<i64>, i64)>, Error> {
        let rows = self
            .on_reference
            .query(
                "select f.file_id, f.record_count, d.begin_seq, coalesce(d.cardinality, 0)
                   from visible_files($1, $2, $3) f left join visible_dvs($1, $2, $3) d on d.file_id = f.file_id
                  order by f.file_id",
                &[&r, &T, &LATEST],
            )
            .await?;
        Ok(rows.iter().map(|x| (x.get(0), x.get(1), x.get(2), x.get(3))).collect())
    }

    fn file(&mut self, (layout, k, k_span, d, d_span): (u8, u16, u8, u8, u8)) -> FileAdd {
        self.files += 1;
        let (k_lo, k_hi) = (i64::from(k), i64::from(k) + i64::from(k_span));
        let mut f = stored_file(T, format!("gs://lake/f{}.parquet", self.files), 100, 1, k_lo, k_hi);
        let (d_lo, d_hi) = (i32::from(d), i32::from(d) + i32::from(d_span % 30));
        let (d_lower, d_upper) = if self.promoted {
            (
                encode_long(i64::from(d_lo) * DAY),
                encode_long(i64::from(d_hi) * DAY + DAY - 1),
            )
        } else {
            (encode_int32(d_lo), encode_int32(d_hi))
        };
        let (k_lower, k_upper) = (f.lower[0].clone(), f.upper[0].clone());
        let (ids, lower, upper) = match layout {
            0 => (vec![1, 3], vec![k_lower, Some(d_lower)], vec![k_upper, Some(d_upper)]),
            1 => (vec![3, 1], vec![Some(d_lower), k_lower], vec![Some(d_upper), k_upper]),
            2 => (vec![1], vec![k_lower], vec![k_upper]),
            _ => (vec![], vec![], vec![]),
        };
        let n = ids.len();
        f.stat_field_ids = ids;
        f.lower = lower;
        f.upper = upper;
        f.null_count = vec![Some(0); n];
        f.nan_count = vec![None; n];
        f
    }

    fn pick_ref(&self, r: u8) -> i32 {
        self.refs[r as usize % self.refs.len()]
    }

    async fn apply(&mut self, s: &Step) -> Result<(), Error> {
        match s {
            Step::Append {
                r,
                files,
                inline,
                footer,
            } => {
                let r = self.pick_ref(*r);
                let mut change = TableChange {
                    table_id: T,
                    ..TableChange::default()
                };
                for (n, seed) in files.iter().enumerate() {
                    let mut f = self.file(*seed);
                    if *inline && n == 0 {
                        let content: Vec<u8> = (0..64 + self.files % 64).map(|i| i as u8).collect();
                        f.path = None;
                        f.file_size = content.len() as i64;
                        f.content = Some(content);
                    }
                    if *footer && n == 0 {
                        f.footer = Some(vec![7; f.footer_size as usize]);
                    }
                    change.adds.push(f);
                }
                self.commit(r, change).await
            }
            Step::Remove { r, pick } => {
                let r = self.pick_ref(*r);
                let seen = self.visible(r).await?;
                if seen.is_empty() {
                    return Ok(());
                }
                let fid = seen[*pick as usize % seen.len()].0;
                self.commit(
                    r,
                    TableChange {
                        table_id: T,
                        removes: vec![fid],
                        ..TableChange::default()
                    },
                )
                .await
            }
            Step::Vector { r, pick, inline } => {
                let r = self.pick_ref(*r);
                let seen = self.visible(r).await?;
                if seen.is_empty() {
                    return Ok(());
                }
                let (fid, records, base_seq, cardinality) = seen[*pick as usize % seen.len()];
                let cardinality = (cardinality + 1).min(records);
                let blob: Option<Vec<u8>> = if *inline {
                    let sql = "select dv_encode(array(select generate_series(0, $1::bigint - 1)))";
                    Some(self.on_reference.query_one(sql, &[&cardinality]).await?.get(0))
                } else {
                    None
                };
                self.files += 1;
                let vector = VectorSet {
                    table_id: T,
                    file_id: fid,
                    base_seq,
                    path: blob.is_none().then(|| format!("gs://dv/{}.puffin", self.files)),
                    content_offset: blob.is_none().then_some(4),
                    content_size: blob.as_ref().map_or(40, |b| b.len() as i64),
                    cardinality,
                    inline: blob,
                };
                self.commit(
                    r,
                    TableChange {
                        table_id: T,
                        vectors: vec![vector],
                        ..TableChange::default()
                    },
                )
                .await
            }
            Step::Branch { from } => {
                let from = self.pick_ref(*from);
                let name = format!("b{}", self.refs.len());
                let id: i32 = self
                    .on_reference
                    .query_one("select create_ref($1, $2)", &[&name, &from])
                    .await?
                    .get(0);
                self.refs.push(id);
                Ok(())
            }
            Step::Merge { r } => {
                let r = self.pick_ref(*r);
                if r != 1 {
                    // A merge with nothing to bring answers NULL.
                    if let Ok(row) = self.on_reference.query_one("select merge($1)", &[&r]).await {
                        self.seqs.extend(row.get::<_, Option<i64>>(0));
                    }
                }
                Ok(())
            }
            Step::DropD => {
                let fields = json!({"drop_fields": [3]});
                self.commit(
                    1,
                    TableChange {
                        table_id: T,
                        definition: Some(fields),
                        ..TableChange::default()
                    },
                )
                .await
            }
            Step::Promote => {
                if self.promoted {
                    return Ok(());
                }
                let fields = json!({"fields": [{"field_id": 3, "new": false, "position": 3, "name": "d",
                                                "type": "timestamp", "required": false}]});
                let before = self.seqs.len();
                self.commit(
                    1,
                    TableChange {
                        table_id: T,
                        definition: Some(fields),
                        ..TableChange::default()
                    },
                )
                .await?;
                self.promoted = self.seqs.len() > before;
                Ok(())
            }
        }
    }

    /// The encoded end of a range on field `f`, in its current type.
    fn end(&self, f: Option<i32>, v: Option<u16>) -> Option<Vec<u8>> {
        let v = v?;
        Some(match f {
            Some(3) if self.promoted => encode_long(i64::from(v % 140) * DAY + i64::from(v) * 1_000_000),
            Some(3) => encode_int32(i32::from(v % 140)),
            _ => encode_long(i64::from(v)),
        })
    }

    async fn compare(&self, q: &Read) -> Result<(), Error> {
        let r = self.pick_ref(q.r);
        let at = q.at.map_or(LATEST, |a| self.seqs[a as usize % self.seqs.len()]);
        let (lo, hi) = (self.end(q.field, q.lo), self.end(q.field, q.hi));
        let by_reference: String = self
            .on_reference
            .query_one(
                "select coalesce(json_agg(x order by x.file_id)::text, '[]') from (
                   select f.file_id, f.path, f.spec_id, f.partition, f.record_count, f.file_size, f.footer_size,
                          f.first_row_id, i.content, ff.footer, d.begin_seq as vector_begin_seq, d.path as vector_path,
                          d.content_offset as vector_offset, d.content_size as vector_size,
                          d.cardinality as vector_cardinality, d.inline as vector_inline
                     from prune($1, $2, $3, $4, $5, $6) f
                     left join visible_dvs($1, $2, $3) d on d.file_id = f.file_id
                     left join inline_file i on i.table_id = $2 and i.file_id = f.file_id
                     left join file_footer ff on ff.table_id = $2 and ff.file_id = f.file_id) x",
                &[&r, &T, &at, &q.field, &lo, &hi],
            )
            .await?
            .get(0);
        let sql = format!(
            // `begin_seq` is the protocol's, for who lists files, and `listed_files` is held to the reference apart.
            "select coalesce(json_agg(x order by x.file_id)::text, '[]') from              (select file_id, path, spec_id, partition, record_count, file_size, footer_size, first_row_id, content,
                     footer, vector_begin_seq, vector_path, vector_offset, vector_size, vector_cardinality, vector_inline
                from {}.plan($1, $2, $3, $4, $5, $6)) x",
            quote_ident(&self.protocol)
        );
        let by_protocol: String = self
            .on_protocol
            .query_one(&sql, &[&r, &T, &at, &q.field, &lo, &hi])
            .await?
            .get(0);
        if by_reference != by_protocol {
            return Err(format!(
                "{q:?} on ref {r} at {at} (promoted: {}): the reference opens {by_reference} and the protocol {by_protocol}",
                self.promoted
            )
            .into());
        }
        Ok(())
    }

    async fn drop(self) -> Result<(), Error> {
        self.on_protocol
            .batch_execute(&format!(
                "drop schema {} cascade; drop schema {} cascade",
                quote_ident(&self.protocol),
                quote_ident(&self.reference)
            ))
            .await?;
        Ok(())
    }
}

#[test]
fn plans_match_reference() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cases = std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(48);
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    let input = (
        prop::collection::vec(step(), 1..24),
        prop::collection::vec(read(), 1..24),
    );
    runner
        .run(&input, |(steps, reads)| {
            runtime
                .block_on(async {
                    let mut scene = Scene::new().await?;
                    for s in &steps {
                        scene.apply(s).await?;
                    }
                    clone_catalog(&scene.on_protocol, &scene.reference, &scene.protocol).await?;
                    for q in &reads {
                        scene.compare(q).await?;
                    }
                    scene.drop().await
                })
                .map_err(|e: Error| TestCaseError::fail(format!("{e:?}")))
        })
        .unwrap();
}

/// `plan` is inlined into a prepared statement, and its generic plan, the one a connection caches, reaches the files
/// through `data_file_bound`. Sequential scans are disabled, since a table of a few rows is read faster without an
/// index: the test checks that the index serves the query, not that the planner prefers it at this size.
#[tokio::test]
async fn plan_inlined_with_index_scan() -> Result<(), Error> {
    let mut scene = Scene::new().await?;
    for k in 0..20u16 {
        scene
            .apply(&Step::Append {
                r: 0,
                files: vec![(0, k * 10, 9, 0, 0)],
                inline: false,
                footer: false,
            })
            .await?;
    }
    clone_catalog(&scene.on_protocol, &scene.reference, &scene.protocol).await?;
    let p = quote_ident(&scene.protocol);
    scene
        .on_protocol
        .batch_execute(&format!(
            "set enable_seqscan = off; set plan_cache_mode = force_generic_plan;
             prepare planned (int, uuid, bigint, int, bytea, bytea) as select * from {p}.plan($1, $2, $3, $4, $5, $6)"
        ))
        .await?;
    let lines: Vec<String> = scene
        .on_protocol
        .query(
            "explain execute planned(1, '00000000-0000-0000-0000-000000007000', 9223372036854775806, 1, \
             '\\x8000000000000032', '\\x8000000000000037')",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let text = lines.join("\n");
    if text.contains("Function Scan on plan") {
        return Err(format!("plan is not inlined:\n{text}").into());
    }
    if !text.contains("data_file_bound") {
        return Err(format!("plan does not use data_file_bound:\n{text}").into());
    }
    scene
        .on_protocol
        .batch_execute("deallocate planned; reset enable_seqscan; reset plan_cache_mode")
        .await?;
    scene.drop().await
}
