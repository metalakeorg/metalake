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

-- case: the filler goes in batches, oldest first, and a visible commit without a time is refused, never invented
-- rules: TT-1
select mk('t') as t \gset
select add(:'t', 1) as a \gset
select add(:'t', 2) as b \gset
update commit set committed_at = null;
select expect(fill_committed_at(1) = 1, 'one per batch');
select expect((select committed_at is not null from commit where commit_seq = (select min(commit_seq) from commit))
              and (select committed_at is null from commit where commit_seq = :b), 'the oldest first');
select expect(fill_committed_at() > 0, 'then the rest');
select expect(not exists (select 1 from commit where committed_at is null), 'all of them');
-- A commit whose transaction left no timestamp: one that rolled back stands for one Postgres has discarded.
begin;
select pg_current_xact_id() as dead \gset
rollback;
update commit set committed_at = null, xid = :'dead' where commit_seq = :b;
select expect_error('select fill_committed_at()', '0A000', 'the filler');
select expect_error($$select * from changes('-infinity', 0)$$, '0A000', 'the feed');

-- case: a read as of a time older than what the ref keeps is refused
-- rules: TT-3
select mk('t') as t \gset
select add(:'t', 1) as s1 \gset
select pg_sleep(0.05);
select add(:'t', 2) as s2 \gset
select pg_sleep(1.1);
select expect(as_of_time(1, :'t', committed_time(:s1)) = :s1, 'kept: every snapshot');
select set_retention(1, min_snapshots => 1);
select expect_error(format('select as_of_time(1, %L, %L)', :'t', committed_time(:s1)), '22023', 'below the floor');
select expect(as_of_time(1, :'t', committed_time(:s2)) = :s2, 'from the floor up');

-- case: the feed of one table delivers only the commits that made a snapshot of it
-- rules: CHG-1
select mk('t') as t \gset
select mk('u') as u \gset
select add(:'t', 1), add(:'u', 2), add(:'t', 3), add(:'u', 4);
select pg_sleep(1.1);
select expect((select array_agg(c.commit_seq order by c.commit_seq) from changes('-infinity', 0, 1000, :'u') c)
              = (select array_agg(distinct s.snapshot_id order by s.snapshot_id) from snapshot s where s.table_id = :'u'),
  'u''s commits only');
select expect((select count(*) from changes('-infinity', 0, 1000)) = (select count(*) from commit), 'and without a table, all');

-- case: the changes of a table between two retained reads are the files added, removed, and whose vector changed
-- rules: CHG-2
select mk('t') as t \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(1), fj(2))))) as s1 \gset
select add(:'t', 3) as s2 \gset
select commit(jsonb_build_object('ref', 1, 'author', 'ana', 'actor', 'test', 'operation', 'delete',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t', 'remove', '[2]'::jsonb)))) as s3 \gset
select commit(jsonb_build_object('ref', 1, 'author', 'ana', 'actor', 'test', 'operation', 'delete',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t',
    'dvs', '[{"file_id": 1, "base_seq": null, "path": "gs://lake/t/dv.puffin", "content_offset": 4, "content_size": 40, "cardinality": 1}]'::jsonb)))) as s4 \gset
select expect((select array_agg(change || ':' || file_id order by change, file_id) from table_changes(1, :'t', :s1, :s4))
              = '{added:3,removed:2,vector:1}', 'added 3, removed 2, a vector on 1');
select expect((select vector_before is null and vector_after ->> 'path' = 'gs://lake/t/dv.puffin'
                 from table_changes(1, :'t', :s1, :s4) where change = 'vector'), 'with the vector at each read');
select expect_error(format('select * from table_changes(1, %L, %s, %s)', :'t', :s4, :s1), '22023', 'not in order');
select set_retention(1, min_snapshots => 1);
select expect_error(format('select * from table_changes(1, %L, %s, %s)', :'t', :s1, :s4), '22023', 'a read no longer kept');

-- case: a commit's changes are those of its read against the one before, with what opens each file and its vectors
-- rules: CHG-3
select mk('t') as t \gset
select mk('u') as u \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(1), fj(2),
  jsonb_build_object('file_id', 3, 'inline', repeat('ab', 50), 'record_count', 1, 'file_size', 50, 'footer_size', 10,
                     'footer', repeat('cd', 10)))))) as s1 \gset
select expect((select array_agg(change || ':' || file_id order by file_id) from commit_changes(1, :'t', :s1))
              = '{added:1,added:2,added:3}', 'what it added');
select expect((select path = 'gs://lake/t/1.parquet' and record_count = 1 and file_size = 1000 and footer_size = 100
                      and content is null and spec_id = 0 and vector_before is null and vector_after is null
                 from commit_changes(1, :'t', :s1) where file_id = 1), 'a stored file, as a reader opens it');
select expect((select path is null and length(content) = 50 and length(footer) = 10
                 from commit_changes(1, :'t', :s1) where file_id = 3), 'an inline file, with its bytes');
select add(:'u', 9) as other \gset
select expect(not exists (select 1 from commit_changes(1, :'t', :other)), 'a commit of another table changed nothing here');
select commit(jsonb_build_object('ref', 1, 'author', 'ana', 'actor', 'test', 'operation', 'delete',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t',
    'dvs', '[{"file_id": 1, "base_seq": null, "path": "gs://lake/t/dv.puffin", "content_offset": 4, "content_size": 40, "cardinality": 1}]'::jsonb)))) as s2 \gset
select expect((select count(*) = 1 and bool_and(change = 'vector' and file_id = 1 and vector_before is null
                                                and vector_after ->> 'path' = 'gs://lake/t/dv.puffin')
                 from commit_changes(1, :'t', :s2)), 'a vector, with the file it applies to');
select commit(jsonb_build_object('ref', 1, 'author', 'ana', 'actor', 'test', 'operation', 'delete',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t', 'remove', '[1]'::jsonb)))) as s3 \gset
select expect((select count(*) = 1 and bool_and(change = 'removed' and file_id = 1 and path = 'gs://lake/t/1.parquet'
                                                and vector_before ->> 'path' = 'gs://lake/t/dv.puffin' and vector_after is null)
                 from commit_changes(1, :'t', :s3)), 'a file removed, with the vector its live rows need');
select commit(jsonb_build_object('ref', 1, 'author', 'ana', 'actor', 'test', 'operation', 'replace',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t', 'remove', '[2]'::jsonb, 'add', jsonb_build_array(fj(4)))))) as s4 \gset
select expect((select array_agg(change || ':' || file_id order by file_id) from commit_changes(1, :'t', :s4))
              = '{removed:2,added:4}', 'a replace states what it did, and its operation lets a reader skip it');
select expect((select array_agg(change || ':' || file_id order by file_id) from commit_changes(1, :'t', :s4))
              = (select array_agg(change || ':' || file_id order by file_id) from table_changes(1, :'t', :s4 - 1, :s4)),
  'the same as the changes of its two reads');
select set_retention(1, min_snapshots => 1);
select expect_error(format('select * from commit_changes(1, %L, %s)', :'t', :s2), '22023', 'a read no longer kept');
select expect_error(format('select * from commit_changes(99, %L, %s)', :'t', :s4), '22023', 'a ref that does not exist');

-- case: a file a branch inherits leaves it by a tombstone, and a new base brings what the parent changed
-- rules: CHG-3
select mk('t') as t \gset
select add(:'t', 1) as s1 \gset
select create_ref('b', 1) as b \gset
select add(:'t', 2) as s2 \gset
select add(:'t', 9, :b) as s3 \gset
select commit(jsonb_build_object('ref', :b, 'author', 'ana', 'actor', 'test', 'operation', 'delete',
  'tables', jsonb_build_array(jsonb_build_object('table_id', :'t', 'remove', '[1]'::jsonb)))) as s4 \gset
select expect((select array_agg(change || ':' || file_id) from commit_changes(:b, :'t', :s4)) = '{removed:1}',
  'a tombstone removes the file on the branch');
select expect(not exists (select 1 from commit_changes(1, :'t', :s4)), 'and not on its parent');
select merge(:b) as m \gset
select expect((select array_agg(change || ':' || file_id order by file_id) from commit_changes(1, :'t', :m))
              = '{removed:1,added:9}', 'the merge, on the parent');
select expect((select array_agg(change || ':' || file_id) from commit_changes(:b, :'t', :m)) = '{added:2}',
  'on the branch, what its new base brings, though the branch wrote nothing and has no snapshot at the merge');
