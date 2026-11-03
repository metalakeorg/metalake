//! R1: does a batch of files cost more as an array of records (`file_add[]`) than as parallel arrays (ADR 0003
//! §8.1)? Both feed the same minimal append, on the same Postgres, alternating. `synchronous_commit = off`, so the
//! comparison is of the call, not of the disk.
//!
//!   DATABASE_URL=... cargo run --release --example r1_records

use std::time::{Duration, Instant};

use metalake_protocol_tests::records::{FileAdd, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_reference, render};

const MEASURE: &str = "
create function m_records(t uuid, files file_add[]) returns bigint language plpgsql as $$
declare s bigint;
begin
  perform 1 from ref where ref_id = 1 for key share;
  perform 1 from head where ref_id = 1 and table_id = t for update;
  s := nextval('commit_seq_gen');
  insert into commit values (s, 1, pg_current_xact_id(), null, 'bench', 'bench', 'append', null, null, null, null);
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, sort_order_id, record_count,
                         file_size, footer_size, first_row_id, stat_field_ids, lower, upper, null_count, nan_count,
                         value_count)
  select t, coalesce(f.file_id, nextval('file_id_gen')), 1, s, f.path, f.spec_id, f.partition, f.sort_order_id,
         f.record_count, f.file_size, f.footer_size, f.first_row_id, f.stat_field_ids, f.lower, f.upper, f.null_count,
         f.nan_count, f.value_count
    from unnest(files) f;
  update head set snapshot_id = s where ref_id = 1 and table_id = t;
  return s;
end $$;

create function m_arrays(t uuid, paths text[], record_counts bigint[], sizes bigint[], footers int[], lowers bytea[],
                         uppers bytea[]) returns bigint language plpgsql as $$
declare s bigint;
begin
  perform 1 from ref where ref_id = 1 for key share;
  perform 1 from head where ref_id = 1 and table_id = t for update;
  s := nextval('commit_seq_gen');
  insert into commit values (s, 1, pg_current_xact_id(), null, 'bench', 'bench', 'append', null, null, null, null);
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                         footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  select t, nextval('file_id_gen'), 1, s, x.path, 0, '[]', x.rc, x.size, x.footer, '{1}', array[x.lo], array[x.hi],
         '{0}', '{NULL}'
    from unnest(paths, record_counts, sizes, footers, lowers, uppers) x(path, rc, size, footer, lo, hi);
  update head set snapshot_id = s where ref_id = 1 and table_id = t;
  return s;
end $$;
";

fn mean(samples: &[Duration]) -> f64 {
    samples.iter().map(Duration::as_secs_f64).sum::<f64>() / samples.len() as f64 * 1e6
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let client = connect().await?;
    let schema = fresh_schema("r1");
    install_reference(&client, &schema).await?;
    let types = std::fs::read_to_string(metalake_protocol_tests::repository().join("sql/v0/02-types.sql"))?;
    client
        .batch_execute(&format!(
            "set search_path = \"{schema}\"; {}; {MEASURE}",
            render(&types, &schema)
        ))
        .await?;
    let t = uuid::Uuid::new_v4();
    client
        .execute(
            "select commit(jsonb_build_object('ref', 1, 'author', 'r1', 'actor', 'r1', 'operation', 'append',
               'tables', jsonb_build_array(jsonb_build_object('table_id', $1::uuid,
                 'create', jsonb_build_object('namespace', '[\"r1\"]'::jsonb, 'name', 't', 'location', 'gs://r1'),
                 'fields', '[{\"field_id\": 1, \"new\": true, \"position\": 1, \"name\": \"k\", \"type\": \"long\",
                             \"required\": true}]'::jsonb, 'cluster_key', '[1]'::jsonb))))",
            &[&t],
        )
        .await?;
    client.batch_execute("set synchronous_commit = off").await?;
    let records = client.prepare("select m_records($1, $2)").await?;
    let arrays = client.prepare("select m_arrays($1, $2, $3, $4, $5, $6, $7)").await?;

    let mut seed = 0i64;
    for (files, rounds) in [(4usize, 2000usize), (10_000, 10)] {
        let (mut by_records, mut by_arrays) = (Vec::new(), Vec::new());
        for round in 0..rounds + rounds / 10 {
            seed += 1;
            let batch: Vec<FileAdd> = (0..files as i64)
                .map(|i| {
                    stored_file(
                        t,
                        format!("gs://r1/{seed}-{i}.parquet"),
                        1000,
                        1,
                        seed * 100_000 + i * 10,
                        seed * 100_000 + i * 10 + 9,
                    )
                })
                .collect();
            let paths: Vec<String> = batch.iter().map(|f| f.path.clone().unwrap()).collect();
            let counts: Vec<i64> = batch.iter().map(|f| f.record_count).collect();
            let sizes: Vec<i64> = batch.iter().map(|f| f.file_size).collect();
            let footers: Vec<i32> = batch.iter().map(|f| f.footer_size).collect();
            let lowers: Vec<Vec<u8>> = batch.iter().map(|f| f.lower[0].clone().unwrap()).collect();
            let uppers: Vec<Vec<u8>> = batch.iter().map(|f| f.upper[0].clone().unwrap()).collect();
            let start = Instant::now();
            client.query_one(&records, &[&t, &batch]).await?;
            let r = start.elapsed();
            let start = Instant::now();
            client
                .query_one(&arrays, &[&t, &paths, &counts, &sizes, &footers, &lowers, &uppers])
                .await?;
            let a = start.elapsed();
            if round >= rounds / 10 {
                by_records.push(r);
                by_arrays.push(a);
            }
        }
        let (r, a) = (mean(&by_records), mean(&by_arrays));
        println!(
            "result\t{files} files\trecords={r:.0} us\tarrays={a:.0} us\trecords/arrays={:.2}",
            r / a
        );
    }
    Ok(())
}
