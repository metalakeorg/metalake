//! Preliminary study of the implementation (ADR 0003 §8), against a v0 catalog loaded with `proto.sql` and
//! `setup.sql`:
//!
//! - 8.1: how a batch of files enters the protocol: parallel arrays, JSON, or binary `COPY` and a call;
//! - 8.2: one call against the same statements sent one by one and pipelined, alone and under concurrency;
//! - 8.3: a planned file list streamed into Arrow batches.
//!
//! Usage: `metalake-previo <conninfo> <files in table 0> <small tables> <seconds per run>`. Each result is one
//! `result<TAB>...` line.

use std::time::{Duration, Instant};

use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use futures::future::BoxFuture;
use futures::{future, pin_mut, StreamExt};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls, Statement};
use uuid::Uuid;

type Error = Box<dyn std::error::Error + Send + Sync>;

const LATEST: i64 = 9223372036854775806;
const BATCH: usize = 8192;

/// The bound encoding of a `long` (STA-5).
fn encode_long(v: i64) -> Vec<u8> {
    ((v as u64) ^ (1 << 63)).to_be_bytes().to_vec()
}

fn table(n: u32) -> Uuid {
    Uuid::parse_str(&format!("00000000-0000-0000-0000-{n:012}")).unwrap()
}

/// The files of one commit, as the core would hold them after reading their footers.
struct Files {
    paths: Vec<String>,
    record_counts: Vec<i64>,
    sizes: Vec<i64>,
    footers: Vec<i32>,
    lowers: Vec<Vec<u8>>,
    uppers: Vec<Vec<u8>>,
}

impl Files {
    fn new(count: usize, seed: u64) -> Files {
        let k = (seed.wrapping_mul(6364136223846793005) >> 20) as i64;
        Files {
            paths: (0..count).map(|i| format!("gs://bench/{seed}-{i}.parquet")).collect(),
            record_counts: vec![1000; count],
            sizes: vec![1_000_000; count],
            footers: vec![10_000; count],
            lowers: (0..count as i64).map(|i| encode_long(k + i * 10)).collect(),
            uppers: (0..count as i64).map(|i| encode_long(k + i * 10 + 9)).collect(),
        }
    }

    fn json(&self) -> serde_json::Value {
        let hex = |b: &Vec<u8>| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        serde_json::Value::Array(
            (0..self.paths.len())
                .map(|i| {
                    serde_json::json!({"path": self.paths[i], "rc": self.record_counts[i], "size": self.sizes[i],
                        "footer": self.footers[i], "lo": hex(&self.lowers[i]), "hi": hex(&self.uppers[i])})
                })
                .collect(),
        )
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Arrays,
    Json,
    Copy,
    Statements,
    Pipelined,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Arrays => "call, arrays",
            Shape::Json => "call, JSON",
            Shape::Copy => "COPY and a call",
            Shape::Statements => "statements, one by one",
            Shape::Pipelined => "statements, pipelined",
        }
    }
}

/// A connection with every statement of the study prepared.
struct Conn {
    client: Client,
    arrays: Statement,
    json: Statement,
    staged: Statement,
    copy: Statement,
    lock_ref: Statement,
    lock_head: Statement,
    insert_commit: Statement,
    insert_files: Statement,
    insert_snapshot: Statement,
    update_head: Statement,
}

impl Conn {
    async fn open(conninfo: &str) -> Result<Conn, Error> {
        let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection: {e}");
            }
        });
        client
            .batch_execute("create temp table staged (path text, rc bigint, size bigint, footer int, lo bytea, hi bytea)")
            .await?;
        Ok(Conn {
            arrays: client.prepare("select p_append($1, 1, $2, $3, $4, $5, $6, $7)").await?,
            json: client.prepare("select p_append_json($1, 1, $2)").await?,
            staged: client.prepare("select p_append_staged($1, 1)").await?,
            copy: client.prepare("copy staged from stdin binary").await?,
            lock_ref: client.prepare("select 1 from ref where ref_id = 1 for key share").await?,
            lock_head: client.prepare("select 1 from head where ref_id = 1 and table_id = $1 for update").await?,
            insert_commit: client
                .prepare(
                    "insert into commit values (nextval('commit_seq_gen'), 1, pg_current_xact_id(), null, 'bench', \
                     'bench', 'append', null, null, null, null)",
                )
                .await?,
            insert_files: client
                .prepare(
                    "insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, \
                     record_count, file_size, footer_size, stat_field_ids, lower, upper, null_count, nan_count) \
                     select $1, nextval('file_id_gen'), 1, currval('commit_seq_gen'), x.path, 0, '[]', x.rc, x.size, \
                     x.footer, '{1}', array[x.lo], array[x.hi], '{0}', '{NULL}' \
                     from unnest($2::text[], $3::bigint[], $4::bigint[], $5::int[], $6::bytea[], $7::bytea[]) \
                     x(path, rc, size, footer, lo, hi)",
                )
                .await?,
            insert_snapshot: client
                .prepare(
                    "insert into snapshot select h.table_id, currval('commit_seq_gen'), 1, h.snapshot_id, \
                     p.schema_seq, p.spec_id, p.sort_order_id, '{\"operation\": \"append\"}' \
                     from head h join snapshot p on p.table_id = h.table_id and p.snapshot_id = h.snapshot_id \
                     where h.ref_id = 1 and h.table_id = $1",
                )
                .await?,
            update_head: client
                .prepare("update head set snapshot_id = currval('commit_seq_gen') where ref_id = 1 and table_id = $1")
                .await?,
            client,
        })
    }

    async fn commit(&self, shape: Shape, t: Uuid, f: &Files) -> Result<(), Error> {
        let c = &self.client;
        match shape {
            Shape::Arrays => {
                c.query_one(&self.arrays, &[&t, &f.paths, &f.record_counts, &f.sizes, &f.footers, &f.lowers, &f.uppers])
                    .await?;
            }
            Shape::Json => {
                c.query_one(&self.json, &[&t, &f.json()]).await?;
            }
            Shape::Copy => {
                let sink = c.copy_in(&self.copy).await?;
                let types = [Type::TEXT, Type::INT8, Type::INT8, Type::INT4, Type::BYTEA, Type::BYTEA];
                let writer = BinaryCopyInWriter::new(sink, &types);
                pin_mut!(writer);
                for i in 0..f.paths.len() {
                    let row: [&(dyn ToSql + Sync); 6] =
                        [&f.paths[i], &f.record_counts[i], &f.sizes[i], &f.footers[i], &f.lowers[i], &f.uppers[i]];
                    writer.as_mut().write(&row).await?;
                }
                writer.finish().await?;
                c.query_one(&self.staged, &[&t]).await?;
            }
            Shape::Statements => {
                c.batch_execute("begin").await?;
                c.query(&self.lock_ref, &[]).await?;
                c.query(&self.lock_head, &[&t]).await?;
                c.execute(&self.insert_commit, &[]).await?;
                c.execute(&self.insert_files, &[&t, &f.paths, &f.record_counts, &f.sizes, &f.footers, &f.lowers, &f.uppers])
                    .await?;
                c.execute(&self.insert_snapshot, &[&t]).await?;
                c.execute(&self.update_head, &[&t]).await?;
                c.batch_execute("commit").await?;
            }
            Shape::Pipelined => {
                // tokio-postgres pipelines requests issued together on one connection, in the order they are polled.
                let by_table: [&(dyn ToSql + Sync); 1] = [&t];
                let files: [&(dyn ToSql + Sync); 7] =
                    [&t, &f.paths, &f.record_counts, &f.sizes, &f.footers, &f.lowers, &f.uppers];
                let steps: Vec<BoxFuture<'_, Result<u64, tokio_postgres::Error>>> = vec![
                    Box::pin(c.execute("begin", &[])),
                    Box::pin(c.execute(&self.lock_ref, &[])),
                    Box::pin(c.execute(&self.lock_head, &by_table)),
                    Box::pin(c.execute(&self.insert_commit, &[])),
                    Box::pin(c.execute(&self.insert_files, &files)),
                    Box::pin(c.execute(&self.insert_snapshot, &by_table)),
                    Box::pin(c.execute(&self.update_head, &by_table)),
                    Box::pin(c.execute("commit", &[])),
                ];
                for step in future::join_all(steps).await {
                    step?;
                }
            }
        }
        Ok(())
    }
}

fn result(fields: &[String]) {
    println!("result\t{}", fields.join("\t"));
}

fn micros(d: Duration) -> String {
    format!("{:.0} us", d.as_secs_f64() * 1e6)
}

fn percentiles(mut samples: Vec<Duration>) -> String {
    samples.sort();
    let at = |p: f64| samples[((samples.len() - 1) as f64 * p) as usize];
    let mean = samples.iter().sum::<Duration>() / samples.len() as u32;
    format!("mean={} p50={} p99={}", micros(mean), micros(at(0.5)), micros(at(0.99)))
}

/// 8.1 and 8.2 · the latency of one client committing `files` files to table 1, `n` times.
async fn latency(conn: &Conn, shape: Shape, files: usize, n: usize, seed: &mut u64) -> Result<(), Error> {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        *seed += 1;
        let f = Files::new(files, *seed);
        let start = Instant::now();
        conn.commit(shape, table(1), &f).await?;
        samples.push(start.elapsed());
    }
    result(&[format!("commit of {files} files"), shape.name().into(), percentiles(samples)]);
    Ok(())
}

/// 8.2 · commits/s of `clients` connections, each committing 4 files to a table among the first `tables`.
async fn throughput(conninfo: &str, shape: Shape, clients: usize, tables: u32, secs: u64) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for id in 0..clients {
        let conninfo = conninfo.to_string();
        tasks.push(tokio::spawn(async move {
            let conn = Conn::open(&conninfo).await?;
            let (mut done, mut seed) = (0u64, (id as u64 + 1) << 40);
            while Instant::now() < deadline {
                seed += 1;
                let t = table(1 + (seed.wrapping_mul(2654435761) % tables as u64) as u32);
                conn.commit(shape, t, &Files::new(4, seed)).await?;
                done += 1;
            }
            Ok::<u64, Error>(done)
        }));
    }
    let mut total = 0;
    for task in tasks {
        total += task.await??;
    }
    let label = if tables == 1 { "1 hot table".to_string() } else { format!("{tables} tables") };
    result(&[
        format!("commits/s, {clients} clients, {label}"),
        shape.name().into(),
        format!("{:.0}/s ({total} commits)", total as f64 / secs as f64),
    ]);
    Ok(())
}

/// 8.3 · a planned file list of table 0 on main, streamed into Arrow batches of `BATCH` rows.
async fn scan(client: &Client, label: &str, sql: &Statement, params: &[&(dyn ToSql + Sync)]) -> Result<(), Error> {
    let start = Instant::now();
    let rows = client.query_raw(sql, params.iter().copied()).await?;
    pin_mut!(rows);
    let (mut first, mut total, mut batches, mut ended) = (None, 0usize, 0usize, false);
    while !ended {
        let (mut ids, mut paths, mut counts) = (Int64Builder::new(), StringBuilder::new(), Int64Builder::new());
        let (mut lowers, mut uppers) = (BinaryBuilder::new(), BinaryBuilder::new());
        let mut n = 0;
        while n < BATCH {
            // A finished RowStream answers a further poll with an error: stop at its end.
            let Some(row) = rows.next().await else {
                ended = true;
                break;
            };
            let row = row?;
            ids.append_value(row.get::<_, i64>(0));
            paths.append_value(row.get::<_, &str>(1));
            counts.append_value(row.get::<_, i64>(2));
            lowers.append_option(row.get::<_, Option<&[u8]>>(3));
            uppers.append_option(row.get::<_, Option<&[u8]>>(4));
            n += 1;
        }
        if n == 0 {
            break;
        }
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("file_id", std::sync::Arc::new(ids.finish())),
            ("path", std::sync::Arc::new(paths.finish())),
            ("record_count", std::sync::Arc::new(counts.finish())),
            ("lower", std::sync::Arc::new(lowers.finish())),
            ("upper", std::sync::Arc::new(uppers.finish())),
        ];
        let batch = RecordBatch::try_from_iter(columns)?;
        first.get_or_insert(start.elapsed());
        total += batch.num_rows();
        batches += 1;
    }
    let elapsed = start.elapsed();
    result(&[
        format!("scan, {label}"),
        format!("files={total} batches={batches}"),
        format!("first batch={}", first.map(micros).unwrap_or_default()),
        format!("all={} ({:.0} files/s)", micros(elapsed), total as f64 / elapsed.as_secs_f64()),
    ]);
    Ok(())
}

const PLAN: &str = "with recursive c (level, ref_id, seq) as (
    select 0, $1::int, $2::bigint
    union all
    select c.level + 1, b.parent_ref_id, b.fork_seq
      from c join ref_base b on b.ref_id = c.ref_id and b.begin_seq <= c.seq and c.seq < b.end_seq),
  hidden as (
    select x.file_id, min(c.level) as level
      from tombstone x join c on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     where x.table_id = $3
     group by x.file_id)
  select f.file_id, f.path, f.record_count, f.lower[1], f.upper[1]
    from c
    join data_file f on f.table_id = $3 and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
    PRUNE
    left join hidden h on h.file_id = f.file_id and h.level < c.level
   where h.file_id is null";

#[tokio::main]
async fn main() -> Result<(), Error> {
    let outcome = run().await;
    if outcome.is_err() {
        // Let a failed connection task report its error before the runtime stops.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    outcome
}

async fn run() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let conninfo = &args[1];
    let files_in_large: i64 = args[2].parse()?;
    let tables: u32 = args[3].parse()?;
    let secs: u64 = args[4].parse()?;
    let conn = Conn::open(conninfo).await?;
    let version: String = conn.client.query_one("show server_version", &[]).await?.get(0);
    result(&[format!("postgres {version}"), format!("files={files_in_large} tables={tables} secs={secs}")]);

    let mut seed = 1u64;
    let shapes = [Shape::Arrays, Shape::Json, Shape::Copy, Shape::Statements, Shape::Pipelined];
    let only_scan = args.get(5).map(String::as_str) == Some("scan");
    for shape in shapes.into_iter().filter(|_| !only_scan) {
        latency(&conn, shape, 4, 50, &mut seed).await?; // warm up
    }
    for shape in shapes.into_iter().filter(|_| !only_scan) {
        latency(&conn, shape, 4, 2000, &mut seed).await?;
    }
    for shape in [Shape::Arrays, Shape::Json, Shape::Copy].into_iter().filter(|_| !only_scan) {
        latency(&conn, shape, 10_000, 10, &mut seed).await?;
    }
    for shape in [Shape::Arrays, Shape::Pipelined, Shape::Statements].into_iter().filter(|_| !only_scan) {
        throughput(conninfo, shape, 8, 1, secs).await?;
        throughput(conninfo, shape, 8, tables, secs).await?;
    }

    let reader = Conn::open(conninfo).await?.client;
    let pruned = reader
        .prepare(&PLAN.replace(
            "PRUNE",
            "and bound_range(f.lower[1], f.upper[1]) && bound_range($4, $5) and f.lower[1] <= $5 and f.upper[1] >= $4",
        ))
        .await?;
    let full = reader.prepare(&PLAN.replace("PRUNE", "")).await?;
    let (lo, hi) = (encode_long(files_in_large / 4 * 10), encode_long(files_in_large / 4 * 10 + 79_999));
    let t0 = table(0);
    for _ in 0..3 {
        scan(&reader, "pruned, ~8,000 files", &pruned, &[&1i32, &LATEST, &t0, &lo, &hi]).await?;
    }
    for _ in 0..2 {
        scan(&reader, "full list", &full, &[&1i32, &LATEST, &t0]).await?;
    }
    Ok(())
}
