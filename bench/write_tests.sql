-- MetaLake · preliminary study 2.3: the commit end to end, scenario by scenario. Empty database, after schema.sql,
-- merge.sql and write.sql.
\set ON_ERROR_STOP 1
set client_min_messages = warning;
insert into ref values (1, 'main', null, null);

do $$
declare c1 bigint; c2 bigint; n int;
begin
  -- W1 · one commit: the files get in, their intents are deleted, the version is recorded.
  perform register_intent(1, 1, array['gs://l/a.parquet', 'gs://l/b.parquet'], '1 hour');
  c1 := commit_write(1, 1, 'flink-job-7', 1,
    '[{"path":"gs://l/a.parquet","row_count":10,"min":1,"max":9},{"path":"gs://l/b.parquet","row_count":10,"min":10,"max":19}]');
  if c1 <= 0 then raise exception 'W1 did not apply'; end if;
  if (select count(*) from visible(1, 1, c1)) <> 2 then raise exception 'W1: % files', (select count(*) from visible(1, 1, c1)); end if;
  if exists (select 1 from intent) then raise exception 'W1: intents left over'; end if;

  -- W2 · the response was lost and the writer retries the same version: no duplicates, returns the earlier commit.
  c2 := commit_write(1, 1, 'flink-job-7', 1,
    '[{"path":"gs://l/a.parquet","row_count":10,"min":1,"max":9},{"path":"gs://l/b.parquet","row_count":10,"min":10,"max":19}]');
  if c2 <> -c1 then raise exception 'W2: returned % (expected %)', c2, -c1; end if;
  if (select count(*) from o1_file where ref = 1) <> 2 then raise exception 'W2 duplicated files'; end if;

  -- W6 · a version older than the recorded one is a no-op (reprocessing an old checkpoint).
  if commit_write(1, 1, 'flink-job-7', 0, '[]') >= 0 then raise exception 'W6 applied an old version'; end if;

  -- W4 · uploaded and the commit never arrives: orphan intent; once expired, it shows up; what was committed never does.
  perform register_intent(1, 1, array['gs://l/c.parquet'], '-1 second');
  perform register_intent(1, 1, array['gs://l/d.parquet'], '-1 second');
  perform commit_write(1, 1, 'other', 1, '[{"path":"gs://l/d.parquet","row_count":1,"min":1,"max":1}]');
  if (select array_agg(h) from orphans(now()) h) <> array['gs://l/c.parquet'] then
    raise exception 'W4: orphans %', (select array_agg(h) from orphans(now()) h);
  end if;

  raise warning 'write: W1, W2, W4 and W6, OK';
end $$;
