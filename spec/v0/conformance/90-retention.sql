-- Retention and collection (§13).

create function cm(tables jsonb, r int default 1, op text default 'append') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', op, 'tables', tables));
$$;
create function fj(id bigint, n bigint default 10) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', 'gs://lake/t/' || id || '.parquet', 'record_count', n,
    'file_size', 1000, 'footer_size', 100);
$$;
create function mk(name text, r int default 1) returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', '["db"]'::jsonb, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true}]'::jsonb)), r);
  return t;
end $$;
create function tj(t uuid, entry jsonb) returns jsonb language sql as $$
  select jsonb_build_array(jsonb_build_object('table_id', t) || entry);
$$;
-- Replaces file `old` by file `new` on ref r.
create function swap(t uuid, old bigint, new bigint, r int default 1) returns bigint language sql as $$
  select cm(tj(t, jsonb_build_object('add', jsonb_build_array(fj(new)), 'remove', jsonb_build_array(old))), r);
$$;
create function files(r int, t uuid, s bigint default 9223372036854775806) returns bigint[] language sql as $$
  select coalesce(array_agg(file_id order by file_id), '{}') from visible_files(r, t, s);
$$;
-- What retained reads return, table by table: at LATEST and at every snapshot each live ref keeps.
create function image() returns table (ref_id int, seq bigint, table_id uuid, snapshot bigint, files text, fields text, dvs text)
language sql as $$
  select rr.ref_id, rr.seq, t.table_id, snapshot_at(rr.ref_id, t.table_id, rr.seq),
    (select string_agg(file_id::text, ',' order by file_id) from visible_files(rr.ref_id, t.table_id, rr.seq)),
    (select string_agg(field_id || ':' || name || ':' || type, ',' order by field_id) from visible_fields(rr.ref_id, t.table_id, rr.seq)),
    (select string_agg(file_id || ':' || cardinality, ',' order by file_id) from visible_dvs(rr.ref_id, t.table_id, rr.seq))
    from (select r.ref_id, 9223372036854775806::bigint as seq from ref r where not r.deleted
          union select r.ref_id, k from ref r, kept_snapshots(r.ref_id) k where not r.deleted) rr, tbl t;
$$;
create function conn(name text) returns void language sql as $$
  select public.dblink_connect(name, 'dbname=postgres host=/tmp user=postgres options=-csearch_path=' || current_schema());
$$;

-- case: a ref keeps every snapshot, or those its retention criteria select
-- rules: RET-1
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))) as s1 \gset
select swap(:'t', 1, 2) as s2 \gset
select swap(:'t', 2, 3) as s3 \gset
select swap(:'t', 3, 4) as s4 \gset
select expect((select count(*) from kept_snapshots(1) k) = 5, 'no retention: all five');
select set_retention(1, min_snapshots => 2);
select expect((select array_agg(k order by k) from kept_snapshots(1) k) = array[:s3, :s4]::bigint[], 'the latest two');
select set_retention(1, min_seq => :s2);
select expect((select array_agg(k order by k) from kept_snapshots(1) k) = array[:s2, :s3, :s4]::bigint[], 'from min_seq');
select set_retention(1, min_seq => :s4, min_snapshots => 3);
select expect((select array_agg(k order by k) from kept_snapshots(1) k) = array[:s2, :s3, :s4]::bigint[], 'either criterion keeps');
select set_retention(1, max_age => '1 hour');
select expect((select count(*) from kept_snapshots(1) k) = 5, 'all of them are recent');
select set_retention(1, max_age => '0 seconds');
select pg_sleep(0.01);
select expect((select count(*) from kept_snapshots(1) k) = 0, 'none is younger than 0 seconds');
select set_retention(1);
select expect((select count(*) from kept_snapshots(1) k) = 5, 'a row that sets nothing keeps all');

-- case: retained reads, their reach points, and hidden refs
-- rules: RET-2
select mk('t') as t \gset
select create_ref('b', 1) as b \gset
select create_ref('c', :b) as c \gset
select set_retention(1, min_snapshots => 0), set_retention(:b, min_snapshots => 0), set_retention(:c, min_snapshots => 0);
select expect((select array_agg(distinct ref_id order by ref_id) from retained_ranges()) = array[1, :b, :c], 'every live ref reads');
select delete_ref(:b);
select expect(:b not in (select ref_id from retained_ranges()) and :b in (select ref_id from reach_ranges()),
  'b is deleted but hidden: c reads through it');
select delete_ref(:c);
select expect(:b not in (select ref_id from reach_ranges()) and :c not in (select ref_id from reach_ranges()), 'then nothing reaches either');

-- case: collecting removes exactly what no retained read reaches, and changes no retained read
-- rules: RET-3
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1), fj(10))))) as s1 \gset
select swap(:'t', 1, 2) as s2 \gset
select create_ref('b', 1) as b \gset
select swap(:'t', 2, 3) as s3 \gset
select swap(:'t', 3, 4) as s4 \gset
select cm(tj(:'t', '{"fields": [{"field_id": 1, "position": 1, "name": "key", "type": "long", "required": true}]}'::jsonb));
-- b removes 10 and adds 20, then is merged; c is created, writes, and is deleted.
select swap(:'t', 10, 20, :b);
select merge(:b);
select create_ref('c', 1) as c \gset
select swap(:'t', 4, 5, :c);
select delete_ref(:c);
-- main keeps its last two snapshots: the field change, which still has 10, and the merge.
select set_retention(1, min_snapshots => 2), set_retention(:b, min_snapshots => 0);
create temp table before as select * from image();
select expire() > 0 as collected \gset
select expect(:'collected', 'something was collected');
select expect(not exists (select * from before except select * from image()) and not exists (select * from image() except select * from before),
  'every retained read returns what it returned before');
select expect((select array_agg(distinct file_id order by file_id) from data_file) = '{4,10,20}',
  'only the rows of files someone still reads: 10 for main''s kept snapshot before the merge');
select expect((select count(*) from ref r where not exists (select 1 from commit c where c.commit_seq = r.created_seq)) = 1,
  'every ref''s creation commit stays, except main''s, which has none');
select expect(not exists (select 1 from data_file where ref_id = :c) and not exists (select 1 from ref_base where ref_id = :c)
  and exists (select 1 from ref where ref_id = :c), 'the deleted ref''s rows are gone; its name stays');
select expect((select count(*) from field where field_id = 1) = 1, 'the old version of field 1 is gone');
select expect(not exists (select 1 from snapshot s where (s.table_id, s.snapshot_id) not in (select * from needed_snapshots())),
  'only snapshots a retained read uses');
select expect(not exists (select 1 from commit c where c.commit_seq in (:s1, :s2, :s3)), 'their commits went with them');
select expect(expire() = 0, 'a second expiring finds nothing');

-- case: objects go only when nothing refers to them any more
-- rules: RET-4
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1), fj(2), fj(3),
  jsonb_build_object('file_id', 4, 'inline', repeat('ab', 50), 'record_count', 1, 'file_size', 50, 'footer_size', 10,
                     'footer', repeat('cd', 10))))));
select cm(tj(:'t', '{"dvs": [{"file_id": 1, "base_seq": null, "path": "gs://lake/t/dv.puffin", "content_offset": 4, "content_size": 40, "cardinality": 1},
                             {"file_id": 2, "base_seq": null, "path": "gs://lake/t/dv.puffin", "content_offset": 44, "content_size": 40, "cardinality": 1}]}'::jsonb), 1, 'delete');
-- b forks now, sees 3, and gets merged later: 3 then has two rows.
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(9)))), :b);
select cm(tj(:'t', '{"remove": [1, 4]}'::jsonb), 1, 'delete');
-- After the merge b reads main at the merge, which no longer has 1 or 4.
select merge(:b);
select set_retention(1, min_snapshots => 0), set_retention(:b, min_snapshots => 0);
select expire();
select expect((select array_agg(path order by path) from deletion_queue) = '{gs://lake/t/1.parquet}',
  'file 1 only: the Puffin file still holds file 2''s vector, and file 9 has a live row on main');
select expect(not exists (select 1 from inline_file where file_id = 4) and not exists (select 1 from file_footer where file_id = 4),
  'the inline file and its footer went with its last row');
select cm(tj(:'t', '{"remove": [2]}'::jsonb), 1, 'delete');
select expire();
select expect((select array_agg(path order by path) from deletion_queue) = '{gs://lake/t/1.parquet}',
  'b, merged and idle, still reads main at the merge, where file 2 and its vector are');
select delete_ref(:b);
select expire();
select expect((select array_agg(path || ':' || kind order by path) from deletion_queue)
              = '{gs://lake/t/1.parquet:data,gs://lake/t/2.parquet:data,gs://lake/t/dv.puffin:puffin}',
  'once b is deleted, file 2 goes, and the Puffin file with it');

-- case: a collection blocks forks and retention changes, never commits
-- rules: RET-5
select mk('t') as t \gset
select conn('collector');
select public.dblink_exec('collector', 'begin');
select * from public.dblink('collector', 'select lock_for_collection()') as x(v text);
set lock_timeout = '300ms';
select expect_error('select create_ref(''b'', 1)', '55P03', 'a fork waits');
select expect_error('select set_retention(1, min_snapshots => 1)', '55P03', 'a retention change waits');
reset lock_timeout;
select expect(cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))) > 0, 'a commit does not');
select public.dblink_exec('collector', 'commit');
select expect(create_ref('b', 1) > 0, 'after the collection, the fork goes ahead');

-- case: a ref is created only at a snapshot its parent keeps
-- rules: RET-6
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))) as s1 \gset
select swap(:'t', 1, 2) as s2 \gset
select expect(create_ref('old', 1, 'branch', :s1) > 0, 'a kept snapshot');
select set_retention(1, min_snapshots => 1);
select expect_error(format('select create_ref(''older'', 1, ''branch'', %s)', :s1), '22023', 'a snapshot main no longer keeps');
select expect(create_ref('recent', 1, 'branch', :s2) > 0, 'its latest');

-- case: orphans come from expired intents nothing refers to, and an expired intent cannot be committed
-- rules: RET-7
select mk('t') as t \gset
select register_intent('gs://lake/t/1.parquet', :'t', 1, '-1 second'), register_intent('gs://lake/t/2.parquet', :'t', 1, '-1 second'),
       register_intent('gs://lake/t/3.parquet', :'t', 1, '1 hour');
insert into intent values ('gs://lake/t/5.parquet', :'t', 1, now() - interval '1 second');
insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size, footer_size)
  values (:'t', 5, 1, 1, 'gs://lake/t/5.parquet', 0, '[]', 1, 1, 1);
select expect((select array_agg(o order by o) from orphans() o) = '{gs://lake/t/1.parquet,gs://lake/t/2.parquet}',
  'expired and unreferenced: 1 and 2; not 3, still valid, nor 5, committed');
insert into deletion_queue values ('gs://lake/t/2.parquet', 'puffin', now());
select expect((select array_agg(o order by o) from orphans() o) = '{gs://lake/t/1.parquet}',
  'a queued object waits for its grace, whatever its intent');
delete from deletion_queue where path = 'gs://lake/t/2.parquet';
select expect_error(format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))), '22023',
  'a commit of a file whose intent expired');
select expect(cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3))))) > 0, 'one whose intent is valid');

-- case: the floor keeps every sequence from it up, also between two snapshots where a commit wrote none
-- rules: RET-1, RET-2
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))) as s1 \gset
select swap(:'t', 1, 2) as s2 \gset
-- A namespace created and dropped between two snapshots of main: commits of namespaces only write no snapshot.
select commit('{"ref": 1, "author": "ana", "actor": "test", "operation": "update", "tables": [],
                "namespaces": [{"namespace": ["n"], "create": true}]}') as n1 \gset
select commit('{"ref": 1, "author": "ana", "actor": "test", "operation": "update", "tables": [],
                "namespaces": [{"namespace": ["n"], "drop": true}]}') as n2 \gset
select swap(:'t', 2, 3) as s3 \gset
select set_retention(1, min_snapshots => 2);
select expect(retention_floor(1) = :s2, 'the floor: the second latest snapshot');
select set_retention(1, min_snapshots => 2, min_seq => :s1);
select expect(retention_floor(1) = :s1, 'the least of the criteria');
select set_retention(1, min_snapshots => 2);
select expire();
select expect(exists (select 1 from namespace where namespace = '{n}' and begin_seq = :n1),
  'the namespace''s row, valid only between two kept snapshots, stays: the reads in between are retained');
select expect(not exists (select 1 from data_file where file_id = 1), 'file 1, closed below the floor, goes');
select expect(exists (select 1 from commit where commit_seq = :n1) and exists (select 1 from commit where commit_seq = :n2),
  'and the commits of namespaces only, within the retention, stay for the feed');
select expect(not exists (select 1 from commit where commit_seq = :s1), 'a commit below the floor without a snapshot goes');
select set_retention(1, min_snapshots => 0);
select expect(retention_floor(1) = 9223372036854775807 and not exists (select 1 from kept_snapshots(1)),
  'keeping no snapshot: the floor is OPEN, and only LATEST is read');

-- case: expiring and a commit that adds a path exclude each other on that path
-- rules: RET-5
select mk('t') as t \gset
select conn('expirer');
select public.dblink_exec('expirer', 'begin');
select * from public.dblink('expirer', 'select path_lock(''gs://lake/t/9.parquet'', true)') as x(v text);
set lock_timeout = '300ms';
select expect_error(format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(9))))), '55P03',
  'a commit of a path being queued waits');
reset lock_timeout;
select expect(cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(8))))) > 0, 'another path does not');
select public.dblink_exec('expirer', 'commit');
select public.dblink_exec('expirer', 'begin');
select * from public.dblink('expirer', format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(7)))))) as x(v text);
set lock_timeout = '300ms';
select expect_error('select path_lock(''gs://lake/t/7.parquet'', true)', '55P03', 'expiring waits for a commit adding the path');
reset lock_timeout;
select public.dblink_exec('expirer', 'commit');
-- Expiring itself: file 1 is collectable while a commit in flight adds a file under its path; expiring waits.
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select cm(tj(:'t', '{"remove": [1]}'::jsonb), 1, 'delete');
select set_retention(1, min_snapshots => 0);
select public.dblink_exec('expirer', 'begin');
select * from public.dblink('expirer', format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(
  fj(5) || '{"path": "gs://lake/t/1.parquet"}'::jsonb))))) as x(v text);
set lock_timeout = '300ms';
select expect_error('select expire()', '55P03', 'expiring waits rather than queue a path being committed');
reset lock_timeout;
select public.dblink_exec('expirer', 'commit');
select expire();
select expect(not exists (select 1 from deletion_queue), 'and then queues nothing: the path is referenced again');

-- case: a commit of a path in the deletion queue is refused
-- rules: RET-8
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select cm(tj(:'t', '{"remove": [1]}'::jsonb), 1, 'delete');
select set_retention(1, min_snapshots => 0);
select expire();
select expect((select array_agg(path) from deletion_queue) = '{gs://lake/t/1.parquet}', 'file 1 is queued');
select expect_error(format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(
  fj(2) || '{"path": "gs://lake/t/1.parquet"}'::jsonb)))), '22023', 'another file under the queued path');
select expect(cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3))))) > 0, 'a new path');

-- case: a queued object is swept only once its grace has passed, and sweeping again is harmless
-- rules: RET-9
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select cm(tj(:'t', '{"remove": [1]}'::jsonb), 1, 'delete');
select set_retention(1, min_snapshots => 0);
select expire();
select expect(collect_grace() = interval '1 day', 'a day unless set');
select expect(not exists (select 1 from sweep_due()), 'nothing is due within the grace');
select expect(sweep_done('{gs://lake/t/1.parquet}') = 0, 'an object within its grace stays queued');
update meta set value = '0 seconds' where key = 'collect_grace';
select expect((select array_agg(p) from sweep_due() p) = '{gs://lake/t/1.parquet}', 'due once the grace has passed');
select expect(sweep_done('{gs://lake/t/1.parquet}') = 1, 'swept');
select expect(not exists (select 1 from deletion_queue), 'and gone from the queue');
select expect(sweep_done('{gs://lake/t/1.parquet}') = 0, 'and sweeping it again does nothing');

-- case: migrating a catalog of revision 0.1 creates the queue and its indexes, and changes no row
-- rules: LAY-5, RET-1
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select set_retention(1, min_snapshots => 1);
-- The catalog as 0.1 left it.
drop table deletion_queue;
drop index data_file_closed, data_file_path;
delete from meta where key = 'collect_grace';
update meta set value = '1' where key = 'writer_version';
create temp table rows_before as select (select count(*) from data_file) f, (select count(*) from snapshot) s,
  (select count(*) from commit) c;
select expect(migrate() is null, 'no commit: nothing to migrate in rows');
select expect((select value from meta where key = 'writer_version') = '3' and collect_grace() = interval '1 day',
  'writer_version 3, and the grace by default');
select expect(to_regclass('deletion_queue') is not null and to_regclass('data_file_closed') is not null,
  'the queue and the indexes');
select expect((select (f, s, c) from rows_before) = ((select count(*) from data_file), (select count(*) from snapshot),
  (select count(*) from commit)), 'no row changed');
select expect(cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(2))))) > 0, 'and it takes commits');
