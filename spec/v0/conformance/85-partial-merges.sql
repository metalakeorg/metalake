-- Partial merges (revision 0.1: §12.2 MRG-6 to MRG-8).

create function ct(r int, entries jsonb, spaces jsonb default '[]') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'a', 'actor', 'a', 'operation', 'update',
                                   'tables', entries, 'namespaces', spaces));
$$;
create function create_in(t uuid, ns jsonb, name text) returns jsonb language sql as $$
  select jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', ns, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}]'::jsonb,
    'cluster_key', '[1]'::jsonb);
$$;
create function prop(t uuid, k text, v text) returns jsonb language sql as $$
  select jsonb_build_object('table_id', t, 'properties', jsonb_build_object(k, v));
$$;
create function file_of(t uuid, path text) returns jsonb language sql as $$
  select jsonb_build_object('table_id', t, 'add', jsonb_build_array(jsonb_build_object(
    'path', path, 'spec_id', 0, 'partition', '[]'::jsonb, 'record_count', 10, 'file_size', 100, 'footer_size', 10,
    'stat_field_ids', '[1]'::jsonb, 'lower', jsonb_build_array(encode(encode_long(0), 'hex')),
    'upper', jsonb_build_array(encode(encode_long(9), 'hex')), 'null_count', '[0]'::jsonb, 'nan_count', '[null]'::jsonb)));
$$;
-- A vector on file f of t: inline, deleting rows lo .. hi, or in a Puffin file.
create function dv_of(t uuid, f bigint, lo int, hi int, inline boolean, base bigint default null) returns jsonb language sql as $$
  select jsonb_build_object('table_id', t, 'dvs', jsonb_build_array(case when inline
    then jsonb_build_object('file_id', f, 'base_seq', base, 'cardinality', hi - lo + 1,
                            'inline', encode(dv_encode(array(select generate_series(lo, hi)::bigint)), 'hex'))
    else jsonb_build_object('file_id', f, 'base_seq', base, 'path', 'gs://dv/' || f || '-' || lo || '.puffin',
                            'content_offset', 4, 'content_size', 40, 'cardinality', hi - lo + 1) end));
$$;
create function pval(r int, t uuid, k text) returns text language sql as $$
  select value from visible_properties(r, t, 9223372036854775806) where key = k;
$$;

-- Two tables of main, db.t1 and db.t2, each with one file.
select ct(1, jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000f1', '["db"]', 't1'),
                               create_in('00000000-0000-0000-0000-0000000000f2', '["db"]', 't2')));
select ct(1, jsonb_build_array(file_of('00000000-0000-0000-0000-0000000000f1', 'gs://lake/a'),
                               file_of('00000000-0000-0000-0000-0000000000f2', 'gs://lake/b')));
select '00000000-0000-0000-0000-0000000000f1' as t1, '00000000-0000-0000-0000-0000000000f2' as t2 \gset
select file_id as f2 from data_file where path = 'gs://lake/b' \gset

-- case: a selection by table merges it; the rest stays on the branch, rebased onto the merge with a snapshot of its own
-- rules: MRG-6 MRG-8
select create_ref('b', 1) as b \gset
select ct(:b, jsonb_build_array(prop(:'t1', 'p', 'b')));
select ct(:b, jsonb_build_array(prop(:'t2', 'p', 'b'))) as before \gset
select ct(1, jsonb_build_array(prop(:'t2', 'q', 'main')));
select merge(:b, jsonb_build_object('only', jsonb_build_object('tables', jsonb_build_array(:'t1')))) as m \gset
select expect(pval(1, :'t1', 'p') = 'b' and pval(1, :'t2', 'p') is null, 'main has t1''s change, not t2''s');
select expect(pval(:b, :'t2', 'p') = 'b' and pval(:b, :'t2', 'q') = 'main', 'the branch: its t2 on top of main at the merge');
select expect((select value from visible_properties(:b, :'t2', :before) where key = 'q') is null,
              'its reads below the merge are unchanged');
select expect((select fork_seq from ref_base where ref_id = :b and end_seq = 9223372036854775807) = :m, 'rebased at m');
select expect((select summary ->> 'operation' = 'rebase' and parent_snapshot_id = snapshot_at(1, :'t2', :m)
                 from snapshot where table_id = :'t2' and snapshot_id = :m and ref_id = :b),
              'a rebase snapshot of t2 on the branch, child of main''s');
select expect((select snapshot_id from head where ref_id = :b and table_id = :'t2') = :m, 'the branch''s head of t2');
select expect(snapshot_at(:b, :'t1', 9223372036854775806) = :m and
              (select ref_id from snapshot where table_id = :'t1' and snapshot_id = :m) = 1,
              'the branch sees t1 through main''s merge');
select merge(:b);
select expect(pval(1, :'t2', 'p') = 'b' and pval(1, :'t2', 'q') = 'main', 'a later merge brings the rest');

-- case: a selection by namespace takes its tables, namespaces and properties; a selected table brings its namespace
-- rules: MRG-6
select ct(1, '[]', '[{"namespace": ["sales"], "create": true}]');
select ct(1, jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000e1', '["sales"]', 's1')));
select create_ref('b', 1) as b \gset
select ct(:b, jsonb_build_array(prop('00000000-0000-0000-0000-0000000000e1', 'p', 'b')),
          '[{"namespace": ["sales"], "properties": {"comment": "S"}}]');
select ct(:b, jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000e2', '["fin", "gl"]', 'f1')),
          '[{"namespace": ["fin"], "create": true, "properties": {"comment": "F"}}, {"namespace": ["fin", "gl"], "create": true}]');
select merge(:b, '{"only": {"namespaces": [["sales"]]}}');
select expect(pval(1, '00000000-0000-0000-0000-0000000000e1', 'p') = 'b'
              and iceberg_namespace(1, '{sales}') -> 'properties' = '{"comment": "S"}', 'sales, its table and its property');
select expect(iceberg_namespace(1, '{fin}') is null, 'fin stays on the branch');
select merge(:b, '{"only": {"tables": ["00000000-0000-0000-0000-0000000000e2"]}}');
select expect(names(1, 9223372036854775806) like '%fin.gl.f1%' and namespace_exists(1, '{fin}', 9223372036854775806)
              and namespace_exists(1, '{fin,gl}', 9223372036854775806), 'a table brings its namespace and its parent');
select expect(iceberg_namespace(1, '{fin}') -> 'properties' = '{}', 'but not their properties');
select expect_error(format('select merge(%s, ''{"only": {"tables": ["00000000-0000-0000-0000-000000000099"]}}'')', :b),
                    '22023', 'a selection that names nothing the branch changed');

-- case: a commit that changed both sides of the selection fails the merge, unless it may be split
-- rules: MRG-7
select create_ref('b', 1) as b \gset
select ct(:b, jsonb_build_array(prop(:'t1', 'r', 'b'), prop(:'t2', 'r', 'b')));
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :b, :'t1'), '40001', 'the commit spans the cut');
select expect(pval(1, :'t1', 'r') is null, 'nothing merged');
select merge(:b, jsonb_build_object('only', jsonb_build_object('tables', jsonb_build_array(:'t1')), 'split', true));
select expect(pval(1, :'t1', 'r') = 'b' and pval(1, :'t2', 'r') is null and pval(:b, :'t2', 'r') = 'b', 'split');

-- case: a rest that conflicts with the parent fails the whole merge
-- rules: MRG-8
select create_ref('b', 1) as b \gset
select ct(:b, jsonb_build_array(prop(:'t1', 's', 'b')));
select ct(:b, jsonb_build_array(prop(:'t2', 's', 'b')));
select ct(1, jsonb_build_array(prop(:'t2', 's', 'main')));
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :b, :'t1'), '40001',
                    'a property of the rest changed on both sides');
select expect(pval(1, :'t1', 's') is null, 'nothing merged');
-- A table of the rest whose name main gave meanwhile to another table.
select create_ref('c', 1) as c \gset
select ct(:c, jsonb_build_array(prop(:'t1', 'u', 'c')));
select ct(:c, jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000c1', '["db"]', 'x')));
select ct(1, jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000c2', '["db"]', 'x')));
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :c, :'t1'), '40001',
                    'the rebased branch would have two tables named db.x');
-- Both sides add a field named c to t2, with different ids: the rebased t2 would have two.
select create_ref('d', 1) as d \gset
select ct(:d, jsonb_build_array(prop(:'t1', 'y', 'd')));
select ct(:d, jsonb_build_array(jsonb_build_object('table_id', :'t2', 'fields', jsonb_build_array(jsonb_build_object(
  'field_id', next_id(:'t2', 'field'), 'new', true, 'position', 2, 'name', 'c', 'type', 'int')))));
select ct(1, jsonb_build_array(jsonb_build_object('table_id', :'t2', 'fields', jsonb_build_array(jsonb_build_object(
  'field_id', next_id(:'t2', 'field'), 'new', true, 'position', 2, 'name', 'c', 'type', 'int')))));
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :d, :'t1'), '40001',
                    'the rebased t2 would have two fields named c');
-- Both sides set one property of namespace db, which is not selected.
select create_ref('e', 1) as e \gset
select ct(:e, jsonb_build_array(prop(:'t1', 'z', 'e')), '[{"namespace": ["db"], "properties": {"k": "e"}}]');
select ct(1, '[]', '[{"namespace": ["db"], "properties": {"k": "main"}}]');
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :e, :'t1'), '40001',
                    'a property of a namespace of the rest changed on both sides');

-- case: vectors of the rest that both sides changed are united on the branch, or the merge fails
-- rules: MRG-8 MRG-5
select create_ref('b', 1) as b \gset
select ct(:b, jsonb_build_array(prop(:'t1', 'v', 'b')));
select ct(:b, jsonb_build_array(dv_of(:'t2', :f2, 0, 2, true)));
select ct(1, jsonb_build_array(dv_of(:'t2', :f2, 5, 6, true)));
select merge(:b, jsonb_build_object('only', jsonb_build_object('tables', jsonb_build_array(:'t1')))) as m \gset
select expect((select cardinality = 5 and begin_seq = :m from file_dv(:b, :'t2', 9223372036854775806, :f2)),
              'the union of both, written on the branch at m');
select expect((select cardinality from file_dv(1, :'t2', 9223372036854775806, :f2)) = 2, 'main keeps its own');
select create_ref('c', 1) as c \gset
select ct(:c, jsonb_build_array(prop(:'t1', 'w', 'c')));
select ct(:c, jsonb_build_array(dv_of(:'t2', :f2, 0, 6, false, (file_dv(:c, :'t2', 9223372036854775806, :f2)).begin_seq)));
select ct(1, jsonb_build_array(dv_of(:'t2', :f2, 5, 7, false, (file_dv(1, :'t2', 9223372036854775806, :f2)).begin_seq)));
select expect_error(format('select merge(%s, ''{"only": {"tables": ["%s"]}}'')', :c, :'t1'), '40001',
                    'vectors in files on both sides, and no merged vector');
select register_intent('gs://dv/late.puffin', :'t2', :c, '-1 second'), register_intent('gs://dv/merged.puffin', :'t2', :c, '1 hour');
select expect_error(format('select merge(%s, %L::jsonb)', :c, jsonb_build_object('only', jsonb_build_object('tables', jsonb_build_array(:'t1')),
             'dvs', jsonb_build_array(jsonb_build_object('table_id', :'t2', 'file_id', :f2, 'path', 'gs://dv/late.puffin',
                                                         'content_offset', 4, 'content_size', 40, 'cardinality', 8)))),
                    '22023', 'a merged vector in a Puffin file whose intent expired');
select merge(:c, jsonb_build_object('only', jsonb_build_object('tables', jsonb_build_array(:'t1')),
             'dvs', jsonb_build_array(jsonb_build_object('table_id', :'t2', 'file_id', :f2, 'path', 'gs://dv/merged.puffin',
                                                         'content_offset', 4, 'content_size', 40, 'cardinality', 8))));
select expect(not exists (select 1 from intent where path = 'gs://dv/merged.puffin'), 'its intent went with the merge');
select expect((select cardinality = 8 and path = 'gs://dv/merged.puffin'
                 from file_dv(:c, :'t2', 9223372036854775806, :f2)), 'the supplied vector, on the branch');
