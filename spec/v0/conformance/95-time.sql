-- Time and changes (§14).

create function cm(tables jsonb, r int default 1) returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', 'append', 'tables', tables));
$$;
create function fj(id bigint) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', 'gs://lake/t/' || id || '.parquet', 'record_count', 1,
    'file_size', 1000, 'footer_size', 100);
$$;
create function mk(name text) returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', '["db"]'::jsonb, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true}]'::jsonb)));
  return t;
end $$;
create function add(t uuid, id bigint, r int default 1) returns bigint language sql as $$
  select cm(jsonb_build_array(jsonb_build_object('table_id', t, 'add', jsonb_build_array(fj(id)))), r);
$$;
create function files(r int, t uuid, s bigint) returns bigint[] language sql as $$
  select coalesce(array_agg(file_id order by file_id), '{}') from visible_files(r, t, s);
$$;
create function conn(name text) returns void language sql as $$
  select public.dblink_connect(name, 'dbname=postgres host=/tmp user=postgres options=-csearch_path=' || current_schema());
$$;

-- case: commit timestamps are tracked, and the filler copies them
-- rules: TT-1
select expect(current_setting('track_commit_timestamp') = 'on', 'track_commit_timestamp');
select mk('t') as t \gset
select add(:'t', 1) as s \gset
select expect((select committed_at is null from commit where commit_seq = :s), 'not filled yet');
select expect(committed_time(:s) = (select pg_xact_commit_timestamp(xid32(xid)) from commit where commit_seq = :s), 'read from the transaction');
select expect(fill_committed_at() = 2, 'two commits filled');
select expect((select committed_at = pg_xact_commit_timestamp(xid32(xid)) from commit where commit_seq = :s), 'the same timestamp');
select expect(fill_committed_at() = 0, 'nothing left to fill');

-- case: the catalog's order is commit time, not commit_seq
-- rules: TT-2
select mk('t1') as t1, mk('t2') as t2 \gset
select conn('slow');
select public.dblink_exec('slow', 'begin');
select a from public.dblink('slow', format('select add(%L, 1)', :'t1')) as x(a bigint) \gset
select add(:'t2', 2) as b \gset
select public.dblink_exec('slow', 'commit');
select fill_committed_at();
select expect(:a < :b, 'a drew the lower commit_seq');
select expect((select array_agg(commit_seq order by committed_at, commit_seq) from commit where commit_seq in (:a, :b)) = array[:b, :a]::bigint[],
  'b committed first, so it comes first');

-- case: a read as of a time takes, per table, the last snapshot by then, through the base in force then
-- rules: TT-3
select mk('t1') as t1, mk('t2') as t2 \gset
select add(:'t1', 1) as a1 \gset
select add(:'t2', 2) as b1 \gset
select add(:'t1', 3) as a2 \gset
select add(:'t2', 4) as b2 \gset
select create_ref('b', 1) as b \gset
select add(:'t1', 5, :b) as o1 \gset
select merge(:b) as m \gset
-- The history, at fixed times: t2's b2 drew a later commit_seq than t1's a2 but committed earlier.
update commit set committed_at = '2026-01-01 09:00Z' where commit_seq < :a1;
update commit set committed_at = '2026-01-01 10:00Z' where commit_seq = :a1;
update commit set committed_at = '2026-01-01 10:01Z' where commit_seq = :b1;
update commit set committed_at = '2026-01-01 10:05Z' where commit_seq = :a2;
update commit set committed_at = '2026-01-01 10:03Z' where commit_seq = :b2;
update commit set committed_at = '2026-01-01 10:10Z' where commit_seq = (select created_seq from ref where ref_id = :b);
update commit set committed_at = '2026-01-01 10:20Z' where commit_seq = :o1;
update commit set committed_at = '2026-01-01 10:30Z' where commit_seq = :m;
select expect(as_of_time(1, :'t1', '2026-01-01 10:04Z') = :a1 and as_of_time(1, :'t2', '2026-01-01 10:04Z') = :b2,
  'at 10:04, t1 before a2 and t2 with b2: one sequence could not say both');
select expect(as_of_time(:b, :'t1', '2026-01-01 10:09Z') is null, 'b did not exist at 10:09');
select expect(files(:b, :'t1', as_of_time(:b, :'t1', '2026-01-01 10:15Z')) = '{1,3}', 'at 10:15, b reads main as of its fork');
select expect(files(:b, :'t1', as_of_time(:b, :'t1', '2026-01-01 10:25Z')) = '{1,3,5}', 'at 10:25, b''s own commit');
select expect(files(1, :'t1', as_of_time(1, :'t1', '2026-01-01 10:25Z')) = '{1,3}', 'which main does not have at 10:25');
select expect(files(:b, :'t1', as_of_time(:b, :'t1', '2026-01-01 10:35Z')) = '{1,3,5}'
  and as_of_time(:b, :'t1', '2026-01-01 10:35Z') = :m, 'at 10:35, after the merge, b reads main as of the merge');
select expect_error(format('select as_of_time(1, %L, now())', :'t1'), '22023', 'a time less than a second ago');
select expect(as_of_time(1, :'t1', now() - interval '2 seconds') = :m, 'two seconds ago');

-- case: the change feed delivers every commit once, in order, with writers and a slow commit running
-- rules: CHG-1
select mk('t') as t, mk('u') as u \gset
create procedure writer(t uuid, w int, n int) language plpgsql as $$
begin
  for i in 1..n loop
    perform add(t, w * 1000 + i);
    commit;
    perform pg_sleep(0.03);
  end loop;
end $$;
-- A commit on another table that draws its commit_seq early and commits late.
create function slow(t uuid) returns bigint language plpgsql as $$
declare s bigint := add(t, 999999);
begin
  perform pg_sleep(1.2);
  return s;
end $$;
create table delivered (commit_seq bigint, committed_at timestamptz, n serial);
create procedure poll(rounds int, pause float) language plpgsql as $$
declare
  cur_at timestamptz := '-infinity';
  cur_seq bigint := 0;
begin
  for i in 1..rounds loop
    select coalesce(max(committed_at), '-infinity') into cur_at from delivered;
    select coalesce(max(d.commit_seq) filter (where d.committed_at = cur_at), 0) into cur_seq from delivered d;
    insert into delivered (commit_seq, committed_at) select c.commit_seq, c.committed_at from changes(cur_at, cur_seq) c;
    perform pg_sleep(pause);
  end loop;
end $$;
select conn('s');
select public.dblink_send_query('s', format('select slow(%L)', :'u'));
select conn('w' || g) from generate_series(1, 6) g;
select public.dblink_send_query('w' || g, format('call writer(%L, %s, 20)', :'t', g)) from generate_series(1, 6) g;
call poll(25, 0.1);
select x from generate_series(1, 6) g, public.dblink_get_result('w' || g) as r(x text);
select x from public.dblink_get_result('s') as r(x bigint);
select pg_sleep(1.1);
call poll(3, 0);
select expect((select count(*) from delivered) = (select count(*) from commit), 'every commit');
select expect((select count(distinct commit_seq) from delivered) = (select count(*) from delivered), 'once');
select expect(not exists (select 1 from (select committed_at, commit_seq, lag(committed_at) over (order by n) as pa,
                                                lag(commit_seq) over (order by n) as ps from delivered) x
                          where (committed_at, commit_seq) < (pa, ps)), 'in catalog order');
select expect(exists (select 1 from commit x, commit y where x.commit_seq < y.commit_seq
                        and committed_time(x.commit_seq) > committed_time(y.commit_seq)),
  'and the slow commit drew an earlier commit_seq than commits that committed before it');
