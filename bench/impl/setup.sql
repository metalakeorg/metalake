-- Preliminary study of the implementation: :n files on main of table 0, file i holding keys [10 i, 10 i + 9], and
-- :tables small tables. Loaded after spec/v0 (schema, then reference) and proto.sql.
select bench_table(0);
select bench_table(n) from generate_series(1, :tables) n;
select setval('commit_seq_gen', (select last_value from commit_seq_gen) + 1);
insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                       footer_size, stat_field_ids, lower, upper, null_count, nan_count)
select bench_id(0), i, 1, (select last_value from commit_seq_gen), 'gs://bench/t0/' || i || '.parquet', 0, '[]',
       1000, 1000000, 10000, '{1}', array[encode_long(i * 10)], array[encode_long(i * 10 + 9)], '{0}', '{NULL}'
  from generate_series(1, :n) i;
select setval('file_id_gen', :n + 1);
vacuum analyze;
