-- Commits (§9). Concurrent cases open real sessions with dblink, each in this case's schema.

create function cm(tables jsonb, extra jsonb default '{}', r int default 1) returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', 'append', 'tables', tables) || extra);
$$;
-- A data file with statistics for field 1, a long: bounds lo and hi.
create function fj(id bigint, lo bigint default 0, hi bigint default 9) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', 'gs://lake/t/' || id || '.parquet', 'record_count', 10,
    'file_size', 1000, 'footer_size', 100, 'stat_field_ids', '[1]'::jsonb,
    'lower', jsonb_build_array(encode(encode_bound('long', lo::text), 'hex')),
    'upper', jsonb_build_array(encode(encode_bound('long', hi::text), 'hex')), 'null_count', '[0]'::jsonb, 'nan_count', '[null]'::jsonb);
$$;
-- Creates db.<name> with fields 1 id long (required) and 2 v string; returns its id.
create function mk(name text, r int default 1) returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', '["db"]'::jsonb, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true},
                {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "string"}]'::jsonb)), '{}', r);
  return t;
end $$;
create function files(r int, t uuid) returns bigint[] language sql as $$
  select coalesce(array_agg(file_id order by file_id), '{}') from visible_files(r, t, 9223372036854775806);
$$;
create function conn(name text) returns void language sql as $$
  select public.dblink_connect(name, 'dbname=postgres host=/tmp user=postgres options=-csearch_path=' || current_schema());
$$;

-- case: a commit writes its files, its commit row, a snapshot per table and the head
-- rules: CMT-3, CMT-8, CMT-1
select mk('t') as t \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(1), fj(2, 10, 19))))) as s1 \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(3)), 'remove', '[1]'::jsonb)),
          '{"operation": "overwrite", "message": "m", "definition": "git:abc"}') as s2 \gset
select expect(files(1, :'t') = '{2,3}', 'the files');
select expect((select ref_id = 1 and author = 'ana' and operation = 'overwrite' and message = 'm' and definition = 'git:abc'
                  and xid is not null and committed_at is null from commit where commit_seq = :s2), 'the commit row');
select expect((select begin_seq from data_file where file_id = 3) = :s2 and (select end_seq from data_file where file_id = 1) = :s2,
  'added files begin, removed own files end, at the commit');
select expect((select array_agg(snapshot_id order by snapshot_id) from snapshot where table_id = :'t') = array[:s1 - 1, :s1, :s2]::bigint[]
  and (select parent_snapshot_id from snapshot where snapshot_id = :s2) = :s1
  and (select parent_snapshot_id from snapshot where snapshot_id = :s1 - 1) is null, 'a chain of snapshots');
select expect((select schema_seq = :s1 - 1 and spec_id = 0 and sort_order_id = 0
                  and summary = '{"operation": "overwrite", "added-data-files": 1, "deleted-data-files": 1, "added-records": 10, "deleted-records": 10, "changed-vectors": 0}'
                 from snapshot where snapshot_id = :s2), 'schema_seq, spec, sort order and summary');
select expect((select snapshot_id from head where ref_id = 1 and table_id = :'t') = :s2, 'the head');

-- case: a commit adds keys of its own to a summary, and those of the snapshot prevail
-- rules: CMT-12, CMT-1
select mk('t') as t \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(1)),
          'summary', '{"engine": "spark", "epoch": "7", "operation": "delete", "added-data-files": "9"}'::jsonb))) as s1 \gset
select expect((select summary = '{"operation": "append", "added-data-files": 1, "deleted-data-files": 0, "added-records": 10,
                                  "deleted-records": 0, "changed-vectors": 0, "engine": "spark", "epoch": "7"}'
                 from snapshot where snapshot_id = :s1), 'its keys are kept, and the snapshot''s prevail');
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'summary', '{"epoch": "8"}'::jsonb))) as s2 \gset
select expect((select parent_snapshot_id = :s1 and summary ->> 'epoch' = '8' from snapshot where snapshot_id = :s2),
  'keys alone touch the table');

-- case: what a commit adds to a summary is an object of strings of a bounded size
-- rules: CMT-12
select mk('t') as t \gset
select expect_error(format($$select cm('[{"table_id": "%s", "summary": [1]}]')$$, :'t'), '22023', 'not an object');
select expect_error(format($$select cm('[{"table_id": "%s", "summary": {"a": 1}}]')$$, :'t'), '22023', 'not a string');
select expect_error(format($$select cm(jsonb_build_array(jsonb_build_object('table_id', '%s',
                       'summary', jsonb_build_object('a', repeat('x', 65536)))))$$, :'t'), '22023', 'too large');
select expect((select count(*) from snapshot where table_id = :'t') = 1, 'a refused commit writes no snapshot');

-- case: the snapshot at a sequence is found through the chain, and only since the base in force
-- rules: CMT-2
select mk('t') as t \gset
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(1))))) as s1 \gset
select branch(2, 'b', :s1 + 1, 1, :s1);
select setval('commit_seq_gen', :s1 + 1);
select expect(snapshot_at(2, :'t', 9223372036854775806) = :s1, 'b inherits main''s snapshot at the fork');
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(2))))) as m1 \gset
select expect(snapshot_at(2, :'t', 9223372036854775806) = :s1, 'not main''s later one');
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(3)))), '{}', 2) as b1 \gset
select expect(snapshot_at(2, :'t', 9223372036854775806) = :b1 and snapshot_at(2, :'t', :b1 - 1) = :s1, 'b''s own, and before it');
select expect((select snapshot_id from head where ref_id = 2 and table_id = :'t') = :b1, 'the head is the current snapshot');
-- A merge-like re-base: b's base closes at m and reopens on main at m, where main has a snapshot.
select cm(jsonb_build_array(jsonb_build_object('table_id', :'t', 'add', jsonb_build_array(fj(4))))) as m \gset
update ref_base set end_seq = :m where ref_id = 2;
insert into ref_base values (2, :m, 9223372036854775807, 1, :m);
select expect(snapshot_at(2, :'t', 9223372036854775806) = :m, 'after the re-base, main''s snapshot, not b''s older own');
select expect(snapshot_at(2, :'t', :m - 1) = :b1, 'before it, b''s own');

-- case: a refused commit leaves nothing behind
-- rules: CMT-3
select mk('t') as t \gset
select last_value as before from commit_seq_gen \gset
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s],
  "fields": [{"field_id": %s, "new": true, "position": 3, "name": "r", "type": "int", "required": true}]}]')$$,
  :'t', fj(1), next_id(:'t', 'field')), '22023', 'a required field without defaults, with a file');
select expect(files(1, :'t') = '{}' and (select count(*) from commit) = 1
  and (select count(*) from field where field_id = 3) = 0 and (select count(*) from snapshot) = 1, 'nothing written');
select expect((select last_value from commit_seq_gen) > :before, 'its sequence number is lost: a gap');

-- case: tags and deleted refs take no commits, and a commit waits for the ref lock a branch creation takes
-- rules: CMT-4
select mk('t') as t \gset
insert into ref (ref_id, name, kind, created_seq, deleted) values (5, 'v1', 'tag', 1, false), (6, 'gone', 'branch', 1, true);
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s]}]', '{}', 5)$$, :'t', fj(1)), '22023', 'a tag');
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s]}]', '{}', 6)$$, :'t', fj(1)), '22023', 'a deleted ref');
select conn('holder');
select public.dblink_exec('holder', 'begin');
select * from public.dblink('holder', 'select 1 from ref where ref_id = 1 for update') as l(x int);
select last_value as before from commit_seq_gen \gset
set lock_timeout = '300ms';
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s]}]')$$, :'t', fj(1)), '55P03', 'blocked by FOR UPDATE on main');
reset lock_timeout;
select expect((select last_value from commit_seq_gen) = :before, 'blocked before drawing its commit_seq');
select public.dblink_exec('holder', 'rollback');
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(1))::jsonb) > 0, 'free again');

-- case: concurrent writers on one table lose no commit and keep one chain of snapshots
-- rules: CMT-4, CMT-1
select mk('t') as t \gset
create procedure many(t uuid, w int, n int) language plpgsql as $$
begin
  for i in 1..n loop
    perform cm(jsonb_build_array(jsonb_build_object('table_id', t, 'add', jsonb_build_array(fj(w * 1000 + i)))));
    commit;
  end loop;
end $$;
select conn('w' || g) from generate_series(1, 8) g;
select public.dblink_send_query('w' || g, format('call many(%L, %s, 25)', :'t', g)) from generate_series(1, 8) g;
select x from generate_series(1, 8) g, public.dblink_get_result('w' || g) as r(x text);
select expect(cardinality(files(1, :'t')) = 200, '200 files');
select expect((select count(*) from snapshot where table_id = :'t') = 201, '201 snapshots');
select expect(not exists (
  select 1 from (select snapshot_id, parent_snapshot_id, lag(snapshot_id) over (order by snapshot_id) as prev
                   from snapshot where table_id = :'t') x
   where parent_snapshot_id is distinct from prev), 'each snapshot''s parent is the one before it');

-- case: an idempotent commit applies once, however often and however concurrently it is retried
-- rules: CMT-5
select mk('t') as t \gset
select cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(1))::jsonb, '{"app_id": "job-7", "app_version": 1}') as c1 \gset
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(2))::jsonb, '{"app_id": "job-7", "app_version": 1}') = :c1,
  'a retry returns the first commit');
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s]}]', '{"app_id": "job-7", "app_version": 0}')$$, :'t', fj(3)),
  '22023', 'a version that is not positive');
select expect(files(1, :'t') = '{1}', 'no duplicate');
select cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(4))::jsonb, '{"app_id": "job-7", "app_version": 3}') as c3 \gset
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(5))::jsonb, '{"app_id": "job-7", "app_version": 2}') = :c3,
  'an older version is a no-op that returns the stored commit');
select expect(files(1, :'t') = '{1,4}', 'only versions 1 and 3 applied');
create table attempt (seq bigint);
create procedure retry(t uuid, v int, n int) language plpgsql as $$
begin
  for i in 1..n loop
    insert into attempt select cm(jsonb_build_array(jsonb_build_object('table_id', t, 'add', jsonb_build_array(fj(100 * v)))),
                                  jsonb_build_object('app_id', 'job-8', 'app_version', v));
    commit;
  end loop;
end $$;
select conn('r' || g) from generate_series(1, 40) g;
-- Version 1 arrives on a fresh app_id; version 2 when version 1 is stored.
select public.dblink_send_query('r' || g, format('call retry(%L, 1, 4)', :'t')) from generate_series(1, 40) g;
select x from generate_series(1, 40) g, public.dblink_get_result('r' || g) as r(x text);
-- One more read per connection ends the first query before the next is sent.
select x from generate_series(1, 40) g, public.dblink_get_result('r' || g) as r(x text);
select expect(bool_and(s = 1), 'the second round was sent') from (
  select public.dblink_send_query('r' || g, format('call retry(%L, 2, 4)', :'t')) as s from generate_series(1, 40) g) x;
select x from generate_series(1, 40) g, public.dblink_get_result('r' || g) as r(x text);
select expect((select count(*) = 320 and count(distinct seq) = 2 from attempt), (select format('320 attempts, two commits: %s attempts, %s commits', count(*), count(distinct seq)) from attempt));
select expect((select count(*) from commit where app_id = 'job-8') = 2 and files(1, :'t') = '{1,4,100,200}', 'each version applied once');

-- case: an expected snapshot that is no longer current is a conflict
-- rules: CMT-6
select mk('t') as t \gset
select snapshot_id as seen from head where table_id = :'t' \gset
select cm(format('[{"table_id": "%s", "expected_snapshot": %s, "add": [%s]}]', :'t', :seen, fj(1))::jsonb) as now \gset
select expect_error(format($$select cm('[{"table_id": "%s", "expected_snapshot": %s, "add": [%s]}]')$$, :'t', :seen, fj(2)),
  '40001', 'a stale writer');
select expect(cm(format('[{"table_id": "%s", "expected_snapshot": %s, "add": [%s]}]', :'t', :now, fj(2))::jsonb) > :now, 'refreshed');
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(3))::jsonb) > 0, 'a blind append is rebased by definition');

-- case: every conflict of the table, with its SQLSTATE
-- rules: CMT-7
select mk('t') as t \gset
select mk('u') as u \gset
select cm(format('[{"table_id": "%s", "add": [%s, %s]}]', :'t', fj(1), fj(2))::jsonb);
select cm(format('[{"table_id": "%s", "remove": [1]}]', :'t')::jsonb);
select expect_error(format($$select cm('[{"table_id": "%s", "remove": [1]}]')$$, :'t'), '40001', 'removing a removed file');
select expect_error(format($$select cm('[{"table_id": "%s", "remove": [99]}]')$$, :'t'), '40001', 'removing an unknown file');
select expect_error(format($$select cm('[{"table_id": "%s", "fields": [{"field_id": %s, "new": true, "position": 3, "name": "v", "type": "int"}]}]')$$,
  :'t', next_id(:'t', 'field')), '40001', 'adding a field named like a sibling');
select expect_error(format($$select cm('[{"table_id": "%s", "fields": [{"field_id": 9, "position": 9, "name": "z", "type": "int"}]}]')$$, :'t'),
  '40001', 'changing a field that is not there');
select expect_error(format($$select cm('[{"table_id": "%s", "fields": [{"field_id": 1, "position": 1, "name": "id", "type": "int", "required": true}]}]')$$, :'t'),
  '40001', 'long to int');
select expect_error(format($$select cm('[{"table_id": "%s", "drop_fields": [9]}]')$$, :'t'), '40001', 'dropping a field that is not there');
select expect_error(format($$select cm('[{"table_id": "%s", "create": {"namespace": ["db"], "name": "u", "location": "x"}}]')$$, gen_random_uuid()),
  '40001', 'creating a taken name');
select expect_error(format($$select cm('[{"table_id": "%s", "rename": {"namespace": ["db"], "name": "u"}}]')$$, :'t'), '40001', 'renaming to a taken name');
select cm(format('[{"table_id": "%s", "drop": true}]', :'u')::jsonb);
select expect_error(format($$select cm('[{"table_id": "%s", "add": [%s]}]')$$, :'u', fj(3)), '40001', 'writing to a dropped table');
do $$ begin perform conflict('x'); exception when others then perform expect(sqlerrm = 'metalake conflict: x', 'the message prefix'); end $$;
-- Eight sessions remove the same file at once: one wins.
create table outcome (ok boolean);
create procedure try_remove(t uuid) language plpgsql as $$
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t, 'remove', '[2]'::jsonb)));
  insert into outcome values (true);
exception when serialization_failure then
  insert into outcome values (false);
end $$;
select conn('x' || g) from generate_series(1, 8) g;
select public.dblink_send_query('x' || g, format('call try_remove(%L)', :'t')) from generate_series(1, 8) g;
select x from generate_series(1, 8) g, public.dblink_get_result('x' || g) as r(x text);
select expect((select count(*) filter (where ok) = 1 and count(*) = 8 from outcome), 'one of eight removes the file');

-- case: removing closes owned rows and tombstones inherited ones; changes add versions in the ref only
-- rules: CMT-8
select mk('t') as t \gset
select cm(format('[{"table_id": "%s", "add": [%s, %s]}]', :'t', fj(1), fj(2))::jsonb) as s1 \gset
select branch(2, 'b', :s1 + 1, 1, :s1);
select setval('commit_seq_gen', :s1 + 1);
select cm(format('[{"table_id": "%s", "add": [%s], "remove": [1],
  "fields": [{"field_id": 2, "position": 2, "name": "w", "type": "string"}], "properties": {"k": "v"}}]', :'t', fj(3))::jsonb, '{}', 2) as b1 \gset
select cm(format('[{"table_id": "%s", "remove": [3], "properties": {"k": null}}]', :'t')::jsonb, '{}', 2) as b2 \gset
select expect((select ref_id = 2 and begin_seq = :b1 from tombstone where file_id = 1), 'a tombstone for the inherited file');
select expect((select end_seq from data_file where file_id = 1) = 9223372036854775807, 'main''s row untouched');
select expect((select end_seq from data_file where file_id = 3) = :b2 and not exists (select 1 from tombstone where file_id = 3),
  'b''s own file closed, with no tombstone');
select expect(files(2, :'t') = '{2}' and files(1, :'t') = '{1,2}', 'what each ref sees');
select expect((select count(*) from field where field_id = 2 and ref_id = 2 and name = 'w' and begin_seq = :b1) = 1
  and (select name from visible_fields(1, :'t', 9223372036854775806) where field_id = 2) = 'v', 'the rename is b''s');
select expect((select count(*) from visible_properties(2, :'t', 9223372036854775806)) = 0
  and (select count(*) from table_property where key = 'k') = 2, 'a property set, then removed with a dropped version');

-- case: a commit that would leave a table invalid is refused; created tables take their ids from the commit
-- rules: CMT-9
select mk('t') as t \gset
select expect((select last_field_id = 2 and last_spec_id = 0 and last_partition_field_id = 999 and last_sort_order_id = 0
                 from table_counter where table_id = :'t'), 'counters start after the created ids');
select expect((select fields = '[]' from default_spec(1, :'t', 9223372036854775806) where spec_id = 0), 'spec 0, unpartitioned');
select expect_error(format($$select cm('[{"table_id": "%s", "fields": [{"field_id": 7, "new": true, "position": 3, "name": "x", "type": "int"}]}]')$$, :'t'),
  '22023', 'a field id that was not drawn');
select expect_error(format($$select cm('[{"table_id": "%s", "fields": [{"field_id": %s, "new": true, "position": 3, "name": "x", "type": "varchar"}]}]')$$,
  :'t', next_id(:'t', 'field')), '22023', 'an unknown type');
select cm(format('[{"table_id": "%s", "cluster_key": [1]}]', :'t')::jsonb);
select expect_error(format($$select cm('[{"table_id": "%s", "add": [{"path": "p", "record_count": 1, "file_size": 1, "footer_size": 1}]}]')$$, :'t'),
  '22023', 'a file without statistics for the cluster key');
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(1))::jsonb) > 0, 'with them');
select expect_error(format($$select cm('[{"table_id": "%s", "add": [{"path": "p", "spec_id": 3, "record_count": 1, "file_size": 1, "footer_size": 1, "stat_field_ids": [1], "lower": [null], "upper": [null], "null_count": [1], "nan_count": [null]}]}]')$$, :'t'),
  '22023', 'a spec the table does not have');
-- A file committed earlier is not checked again: one written directly with a partition of the wrong arity.
insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                       footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  values (:'t', 500, 1, 1, 'p', 0, '[7, 8]', 1, 1, 1, '{1}', '{null}', '{null}', '{0}', '{null}');
select expect(cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(2))::jsonb) > 0, 'an earlier invalid file does not block a commit');
select cm(format('[{"table_id": "%s", "partition_spec": [{"source_ids": [1], "field_id": %s, "name": "b", "transform": "bucket[8]"}]}]',
  :'t', next_id(:'t', 'partition_field'))::jsonb);
select expect((select spec_id = 1 from default_spec(1, :'t', 9223372036854775806)), 'a new spec takes the next id');

-- case: a commit deletes the intents of its files and notifies listeners
-- rules: CMT-10
select mk('t') as t \gset
select register_intent('gs://lake/t/1.parquet', :'t', 1, '1 hour'), register_intent('gs://lake/t/other.parquet', :'t', 1, '1 hour');
select conn('listener');
select public.dblink_exec('listener', 'listen metalake');
select cm(format('[{"table_id": "%s", "add": [%s]}]', :'t', fj(1))::jsonb) as s \gset
select expect((select array_agg(path) from intent) = '{gs://lake/t/other.parquet}', 'its intent is gone, the other stays');
select expect((select bool_or(notify_name = 'metalake' and extra = current_schema() || ':' || :s)
                 from public.dblink_get_notify('listener')), 'NOTIFY metalake, <schema>:<commit_seq>');

-- case: a sequence becomes stable once every commit that could hold a lower commit_seq has finished
-- rules: VIS-11
select mk('t1') as t1, mk('t2') as t2 \gset
-- A commit on t1 draws its commit_seq and stays open.
select conn('slow');
select public.dblink_exec('slow', 'begin');
select a from public.dblink('slow', format($$select cm('[{"table_id": "%s", "add": [%s]}]')$$, :'t1', fj(1))) as x(a bigint) \gset
-- A later commit on t2 finishes first.
select cm(format('[{"table_id": "%s", "add": [%s]}]', :'t2', fj(2))::jsonb) as b \gset
select expect(:b > :a, 'the later commit has the greater commit_seq');
select seq as l, horizon as x from stable_point() \gset
select expect(:l >= :b, 'the candidate covers both');
select expect(files(1, :'t1') = '{}' and not is_stable(:'x'), 'not stable while the commit on t1 is open');
select public.dblink_exec('slow', 'commit');
select expect(is_stable(:'x'), 'stable once it has finished');
select expect((select coalesce(array_agg(file_id), '{}') from visible_files(1, :'t1', :l)) = '{1}',
  'and a read at it now sees the earlier commit, which it would not have before');
