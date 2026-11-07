-- Branches and merges (§12).

create function cm(tables jsonb, r int default 1, op text default 'append') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', op, 'tables', tables));
$$;
create function fj(id bigint, n bigint default 10) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', 'gs://lake/t/' || id || '.parquet', 'record_count', n,
    'file_size', 1000, 'footer_size', 100);
$$;
create function dvj(id bigint, positions bigint[], base bigint default null) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'base_seq', base, 'cardinality', (select count(distinct p) from unnest(positions) p),
    'inline', encode(dv_encode(positions), 'hex'));
$$;
create function mk(name text, r int default 1) returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', '["db"]'::jsonb, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true},
                {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "string"}]'::jsonb)), r);
  return t;
end $$;
-- One table entry of a commit.
create function tj(t uuid, entry jsonb) returns jsonb language sql as $$
  select jsonb_build_array(jsonb_build_object('table_id', t) || entry);
$$;
create function files(r int, t uuid, s bigint default 9223372036854775806) returns bigint[] language sql as $$
  select coalesce(array_agg(file_id order by file_id), '{}') from visible_files(r, t, s);
$$;
create function dv_of(r int, t uuid, id bigint) returns bigint[] language sql as $$
  select array(select dv_positions(inline) order by 1) from visible_dvs(r, t, 9223372036854775806) where file_id = id;
$$;
create function cols(r int, t uuid) returns text language sql as $$
  select string_agg(field_id || ':' || name || ':' || type, ' ' order by field_id) from visible_fields(r, t, 9223372036854775806);
$$;
create function conn(name text) returns void language sql as $$
  select public.dblink_connect(name, 'dbname=postgres host=/tmp user=postgres options=-csearch_path=' || current_schema());
$$;

-- case: a branch sees its parent at the fork, and waits for the parent's commits in flight
-- rules: BR-1
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1))))) as s1 \gset
select create_ref('b', 1) as b \gset
select expect((select kind = 'branch' and created_seq > :s1 from ref where ref_id = :b)
  and (select parent_ref_id = 1 and fork_seq = :s1 and begin_seq = (select created_seq from ref where ref_id = :b)
         from ref_base where ref_id = :b)
  and (select operation = 'branch' from commit where commit_seq = (select created_seq from ref where ref_id = :b)), 'the rows');
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(2)))));
select expect(files(:b, :'t') = '{1}' and files(1, :'t') = '{1,2}', 'b sees main at the fork, not after');
select create_ref('old', 1, 'branch', :s1 - 1) as old \gset
select expect(files(:old, :'t') = '{}', 'a branch of an earlier sequence');
select expect_error('select create_ref(''future'', 1, ''branch'', 999999)', '22023', 'a sequence not drawn yet');
-- A commit on main stays open: creating a branch waits for it.
select conn('slow');
select public.dblink_exec('slow', 'begin');
select a from public.dblink('slow', format('select cm(%L::jsonb)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3)))))) as x(a bigint) \gset
set lock_timeout = '300ms';
select expect_error('select create_ref(''c'', 1)', '55P03', 'blocked by the commit in flight');
reset lock_timeout;
select public.dblink_exec('slow', 'commit');
select create_ref('c', 1) as c \gset
select expect((select fork_seq from ref_base where ref_id = :c) >= :a and files(:c, :'t') = '{1,2,3}', 'then it sees that commit');

-- case: a tag sees its ref at the fork, takes no commits and is never merged
-- rules: BR-2
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select create_ref('v1', 1, 'tag') as v \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(2)))));
select expect(files(:v, :'t') = '{1}' and (select kind from ref where ref_id = :v) = 'tag', 'a fixed view');
select expect_error(format('select cm(%L::jsonb, %s)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3)))), :v), '22023', 'a commit');
select expect_error(format('select merge(%s)', :v), '22023', 'a merge');

-- case: main and protected refs are not deleted, and names are never reused
-- rules: BR-3
select create_ref('b', 1) as b \gset
select create_ref('keep', 1) as k \gset
update ref set protected = true where ref_id = :k;
select expect_error('select delete_ref(1)', '22023', 'main');
select expect_error(format('select delete_ref(%s)', :k), '22023', 'a protected ref');
select delete_ref(:b);
select expect((select deleted from ref where ref_id = :b), 'deleted');
select expect_error('select create_ref(''b'', 1)', '23505', 'the name of a deleted ref');

-- case: a branch merges into its parent in one commit of the parent; with nothing to merge, nothing happens
-- rules: MRG-1
select mk('t') as t \gset
select create_ref('b', 1) as b \gset
select expect(merge(:b) is null and (select count(*) from commit where operation = 'merge') = 0, 'nothing to merge');
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))), :b);
-- A commit on b stays open: the merge waits for it.
select conn('slow');
select public.dblink_exec('slow', 'begin');
select a from public.dblink('slow', format('select cm(%L::jsonb, %s)', tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(2)))), :b)) as x(a bigint) \gset
select last_value as before from commit_seq_gen \gset
set lock_timeout = '300ms';
select expect_error(format('select merge(%s)', :b), '55P03', 'blocked by b''s commit in flight');
reset lock_timeout;
select expect((select last_value from commit_seq_gen) = :before, 'blocked before drawing its commit_seq');
select public.dblink_exec('slow', 'commit');
select merge(:b, '{"author": "bea", "message": "ship it"}') as m \gset
select expect((select ref_id = 1 and operation = 'merge' and author = 'bea' and message = 'ship it' from commit where commit_seq = :m),
  'one commit of main');
select expect(files(1, :'t') = '{1,2}', 'both of b''s files');
select expect_error('select merge(1)', '22023', 'main has no parent');

-- case: files: both sides add, a removal joins the parent, a double removal conflicts, a file b added and removed
-- rules: MRG-2, MRG-3
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1), fj(2), fj(3)))));
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(4)))));
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(5)), 'remove', '[1]'::jsonb)), :b);
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(6)))), :b);
select cm(tj(:'t', jsonb_build_object('remove', '[6]'::jsonb)), :b);
select merge(:b);
select expect(files(1, :'t') = '{2,3,4,5}', 'main: its 4, b''s 5, without 1; 6 never existed for main');
select create_ref('b2', 1) as b2 \gset
select cm(tj(:'t', jsonb_build_object('remove', '[2]'::jsonb)), :b2);
select cm(tj(:'t', jsonb_build_object('remove', '[2]'::jsonb)));
select expect_error(format('select merge(%s)', :b2), '40001', 'both removed 2');
select expect(files(1, :'t') = '{3,4,5}' and files(:b2, :'t') = '{3,4,5}', 'a failed merge changes nothing');

-- case: vectors: one side, both sides inline (union), against a removal, and on b's own new file
-- rules: MRG-3, MRG-5
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1), fj(2), fj(3), fj(4)))));
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(1, '{0}'), dvj(2, '{1}')))), :b, 'delete');
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(2, '{2}')))), 1, 'delete');
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(9)), 'dvs', jsonb_build_array(dvj(9, '{3}')))), :b, 'delete');
select merge(:b);
select expect(dv_of(1, :'t', 1) = '{0}', 'b''s vector where main kept the old one');
select expect(dv_of(1, :'t', 2) = '{1,2}', 'the union where both deleted rows of 2');
select expect(dv_of(1, :'t', 9) = '{3}', 'a vector on b''s own file comes with it');
select expect((select (summary ->> 'changed-vectors')::int > 0 from snapshot s where s.table_id = :'t'
                order by s.snapshot_id desc limit 1), 'the merge counts the vectors it opened');
select create_ref('b2', 1) as b2 \gset
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(3, '{5}')))), :b2, 'delete');
select cm(tj(:'t', jsonb_build_object('remove', '[3]'::jsonb)), 1, 'delete');
select expect_error(format('select merge(%s)', :b2), '40001', 'rows deleted on b2, file removed on main');
select create_ref('b3', 1) as b3 \gset
select cm(tj(:'t', jsonb_build_object('remove', '[4]'::jsonb)), :b3, 'delete');
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(4, '{6}')))), 1, 'delete');
select expect_error(format('select merge(%s)', :b3), '40001', 'file removed on b3, rows deleted on main');

-- case: a file the branch removes takes the parent's own vector of it out with it
-- rules: MRG-4, DV-1
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(1, '{0}')))), 1, 'delete');
select create_ref('b', 1) as b \gset
select cm(tj(:'t', '{"remove": [1]}'::jsonb), :b, 'delete');
select merge(:b) as m \gset
select expect((select end_seq from deletion_vector where ref_id = 1 and file_id = 1) = :m, 'main''s vector of 1 closes at the merge');

-- case: when a vector in a Puffin file changed on both sides, the caller supplies the merged one
-- rules: MRG-5
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(1, '{5}')))), :b, 'delete');
select cm(tj(:'t', '{"dvs": [{"file_id": 1, "base_seq": null, "path": "gs://lake/t/a.puffin", "content_offset": 4,
  "content_size": 40, "cardinality": 2}]}'::jsonb), 1, 'delete');
select expect_error(format('select merge(%s)', :b), '40001', 'no merged vector given');
select expect_error(format('select merge(%s, %L)', :b, jsonb_build_object('dvs', jsonb_build_array(jsonb_build_object('table_id', :'t',
  'file_id', 1, 'path', 'gs://lake/t/m.puffin', 'content_offset', 4, 'content_size', 44, 'cardinality', 1)))), '40001',
  'a merged vector smaller than one side');
select expect_error(format('select merge(%s, %L)', :b, jsonb_build_object('dvs', jsonb_build_array(
  jsonb_build_object('table_id', :'t', 'file_id', 1, 'cardinality', 3, 'inline', encode(dv_encode('{7,8,9}'), 'hex'))))),
  '40001', 'a merged vector without b''s deleted row 5');
select merge(:b, jsonb_build_object('dvs', jsonb_build_array(jsonb_build_object('table_id', :'t', 'file_id', 1,
  'path', 'gs://lake/t/m.puffin', 'content_offset', 4, 'content_size', 44, 'cardinality', 3))));
select expect((select path = 'gs://lake/t/m.puffin' and cardinality = 3 from visible_dvs(1, :'t', 9223372036854775806)), 'the supplied vector');

-- case: a parent that compacted a file the branch deleted rows of is a conflict
-- rules: MRG-3
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('dvs', jsonb_build_array(dvj(1, '{0}')))), :b, 'delete');
select cm(tj(:'t', jsonb_build_object('remove', '[1]'::jsonb, 'add', jsonb_build_array(fj(2)))), 1, 'replace');
select expect_error(format('select merge(%s)', :b), '40001', 'the compaction would resurrect the deleted row');

-- case: nested branches merge level by level; a file of the grandparent is removed through both
-- rules: MRG-2, MRG-4
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1), fj(2)))));
select create_ref('b', 1) as b \gset
select create_ref('c', :b) as c \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3)), 'remove', '[1]'::jsonb)), :c);
select merge(:c);
select expect((select count(*) from tombstone where ref_id = :b and file_id = 1 and end_seq = 9223372036854775807) = 1
  and files(:b, :'t') = '{2,3}' and files(1, :'t') = '{1,2}', 'b inherits 1, so it gets a tombstone');
select merge(:b);
select expect(files(1, :'t') = '{2,3}' and (select end_seq < 9223372036854775807 from data_file where file_id = 1 and ref_id = 1),
  'main owns 1, so its row closes');
select expect(files(:c, :'t') = '{2,3}' and files(:b, :'t') = '{2,3}', 'both children now see main');
-- A key at two levels of the parent's chain: b renames v, c forks b, b renames it again, c renames it too.
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "w", "type": "string"}]}'::jsonb), :b);
select create_ref('c2', :b) as c2 \gset
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "w2", "type": "string"}]}'::jsonb), :b);
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "z", "type": "string"}]}'::jsonb), :c2);
select expect_error(format('select merge(%s)', :c2), '40001', 'b changed the field after c2''s fork, below main''s version');

-- case: after a merge the branch is an empty child of its parent, and its past reads are unchanged
-- rules: MRG-4
select mk('t') as t \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(1)))));
select create_ref('b', 1) as b \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(2)), 'remove', '[1]'::jsonb,
  'fields', '[{"field_id": 2, "position": 2, "name": "w", "type": "string"}]'::jsonb)), :b) as b1 \gset
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fj(3)))));
create temp table before as select s, files(:b, :'t', s) as f from generate_series(0, :b1 + 1) s;
select merge(:b) as m \gset
select expect(not exists (select 1 from before where f <> files(:b, :'t', s)), 'every read of b below the merge is unchanged');
select expect(files(:b, :'t') = '{2,3}' and files(1, :'t') = '{2,3}' and cols(:b, :'t') = cols(1, :'t'), 'b sees main');
select expect((select count(*) from data_file where ref_id = :b and end_seq = 9223372036854775807)
            + (select count(*) from tombstone where ref_id = :b and end_seq = 9223372036854775807)
            + (select count(*) from field where ref_id = :b and end_seq = 9223372036854775807) = 0, 'b has no live rows');
select expect((select array_agg(array[begin_seq, end_seq, fork_seq] order by begin_seq) from ref_base where ref_id = :b)
  = (select array[array[created_seq, :m, fork_seq], array[:m, 9223372036854775807, :m]]
       from ref, ref_base where ref.ref_id = :b and ref_base.ref_id = :b and ref_base.end_seq = :m), 're-based at the merge');
select expect((select snapshot_id from head where ref_id = :b and table_id = :'t') = :m
  and (select snapshot_id from head where ref_id = 1 and table_id = :'t') = :m, 'heads');
select expect((select summary ->> 'operation' = 'merge' and (summary ->> 'added-data-files')::int = 1
                  and (summary ->> 'deleted-data-files')::int = 1 and (summary ->> 'deleted-records')::int = 10
                 from snapshot where snapshot_id = :m), 'the merge snapshot');

-- case: schema keys: one side, the same change, different changes, and two fields with one name
-- rules: MRG-3
select mk('t') as t \gset
select create_ref('b', 1) as b \gset
select cm(tj(:'t', format('{"fields": [{"field_id": %s, "new": true, "position": 3, "name": "x", "type": "int"}]}', next_id(:'t', 'field'))::jsonb), :b);
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "w", "type": "string"}]}'::jsonb), :b);
select merge(:b);
select expect(cols(1, :'t') = '1:id:long 2:w:string 3:x:int', 'b''s new field and rename');
select create_ref('b2', 1) as b2 \gset
select cm(tj(:'t', '{"fields": [{"field_id": 3, "position": 3, "name": "y", "type": "long"}]}'::jsonb), :b2);
select cm(tj(:'t', '{"fields": [{"field_id": 3, "position": 3, "name": "y", "type": "long"}]}'::jsonb));
select merge(:b2);
select expect(cols(1, :'t') = '1:id:long 2:w:string 3:y:long', 'the same change on both sides');
select create_ref('b3', 1) as b3 \gset
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "p", "type": "string"}]}'::jsonb), :b3);
select cm(tj(:'t', '{"fields": [{"field_id": 2, "position": 2, "name": "q", "type": "string"}]}'::jsonb));
select expect_error(format('select merge(%s)', :b3), '40001', 'two renames of one field');
select create_ref('b4', 1) as b4 \gset
select cm(tj(:'t', format('{"fields": [{"field_id": %s, "new": true, "position": 4, "name": "z", "type": "int"}]}', next_id(:'t', 'field'))::jsonb), :b4);
select cm(tj(:'t', format('{"fields": [{"field_id": %s, "new": true, "position": 5, "name": "z", "type": "string"}]}', next_id(:'t', 'field'))::jsonb));
select expect_error(format('select merge(%s)', :b4), '40001', 'two new fields named z');

-- case: partition specs, properties and table names
-- rules: MRG-3
select mk('t') as t \gset
select create_ref('b', 1) as b \gset
select cm(tj(:'t', format('{"partition_spec": [{"source_ids": [1], "field_id": %s, "name": "b", "transform": "bucket[4]"}],
  "properties": {"owner": "ana"}}', next_id(:'t', 'partition_field'))::jsonb), :b);
select cm(tj(:'t', '{"properties": {"tier": "gold"}}'::jsonb));
select merge(:b);
select expect((select spec_id from default_spec(1, :'t', 9223372036854775806)) = 1
  and (select string_agg(key || '=' || value, ' ' order by key) from visible_properties(1, :'t', 9223372036854775806)) = 'owner=ana tier=gold',
  'b''s spec; properties of different keys from both');
select create_ref('b2', 1) as b2 \gset
select cm(tj(:'t', '{"properties": {"owner": "bea"}}'::jsonb), :b2);
select cm(tj(:'t', '{"properties": {"owner": "cal"}}'::jsonb));
select expect_error(format('select merge(%s)', :b2), '40001', 'one property set differently');
select create_ref('b3', 1) as b3 \gset
select cm(tj(:'t', format('{"partition_spec": [{"source_ids": [1], "field_id": %s, "name": "c", "transform": "bucket[8]"}]}', next_id(:'t', 'partition_field'))::jsonb), :b3);
select cm(tj(:'t', '{"partition_spec": []}'::jsonb));
select expect_error(format('select merge(%s)', :b3), '40001', 'two new partition specs');
select create_ref('b4', 1) as b4 \gset
select mk('n', :b4) as n \gset
select cm(tj(:'n', jsonb_build_object('add', jsonb_build_array(fj(7)))), :b4);
select cm(tj(:'t', '{"rename": {"namespace": ["db"], "name": "t2"}}'::jsonb), :b4);
select merge(:b4);
select expect(table_by_name(1, '{db}', 'n', 9223372036854775806) = :'n' and files(1, :'n') = '{7}'
  and table_by_name(1, '{db}', 't2', 9223372036854775806) = :'t', 'a table created on b, and a rename, reach main');
select create_ref('b5', 1) as b5 \gset
select mk('dup', :b5);
select mk('dup');
select expect_error(format('select merge(%s)', :b5), '40001', 'both created db.dup');
select create_ref('b6', 1) as b6 \gset
select cm(tj(:'n', jsonb_build_object('add', jsonb_build_array(fj(8)))), :b6);
select cm(tj(:'n', '{"drop": true}'::jsonb));
select expect_error(format('select merge(%s)', :b6), '40001', 'a write to a table main dropped');
