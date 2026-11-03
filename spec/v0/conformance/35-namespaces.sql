-- Namespaces and the writer's version (revision 0.1: §6 NS-1 to NS-5, LAY-5, §18).

create function ns_commit(r int, spaces jsonb, tables jsonb default '[]') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'a', 'actor', 'a', 'operation', 'update',
                                   'namespaces', spaces, 'tables', tables));
$$;
-- A table entry that creates table t named `name` in namespace ns.
create function create_in(t uuid, ns jsonb, name text) returns jsonb language sql as $$
  select jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', ns, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}]'::jsonb,
    'cluster_key', '[1]'::jsonb);
$$;
create function spaces(r int, s bigint default 9223372036854775806) returns text language sql as $$
  select coalesce(string_agg(array_to_string(namespace, '.'), ' ' order by namespace), '') from visible_namespaces(r, s);
$$;
create function props(r int, ns text[], s bigint default 9223372036854775806) returns jsonb language sql as $$
  select iceberg_namespace(r, ns) -> 'properties' where s = 9223372036854775806
  union all
  select coalesce(jsonb_object_agg(p.key, p.value), '{}') from visible_namespace_properties(r, ns, s) p
   where s <> 9223372036854775806;
$$;

-- case: a namespace is created with properties, which change and go, and holds tables
-- rules: NS-1 NS-4
select ns_commit(1, '[{"namespace": ["sales"], "create": true,
                       "properties": {"comment": "Sales", "location": "gs://lake/sales"}}]') as s1 \gset
select expect(spaces(1) = 'db sales', 'db and sales exist');
select expect(props(1, '{sales}') = '{"comment": "Sales", "location": "gs://lake/sales"}', 'its properties');
select ns_commit(1, '[{"namespace": ["sales"], "properties": {"comment": null, "kind": "foreign"}}]');
select expect(props(1, '{sales}') = '{"kind": "foreign", "location": "gs://lake/sales"}', 'one removed, one added');
select expect(props(1, '{sales}', :s1) = '{"comment": "Sales", "location": "gs://lake/sales"}', 'as they were at s1');
select ns_commit(1, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000a1', '["sales"]', 'orders')));
select expect(names(1, 9223372036854775806) like '%sales.orders%', 'a table in sales');
select expect_error($$select ns_commit(1, '[{"namespace": [], "create": true}]')$$, '22023', 'the empty array');
select expect_error($$select ns_commit(1, '[{"namespace": ["a", ""], "create": true}]')$$, '22023', 'an empty level');
select expect_error($$select ns_commit(1, '[{"namespace": ["n2"], "create": true}, {"namespace": ["n2"], "properties": {"k": "v"}}]')$$,
                    '22023', 'one namespace named twice in a commit');
select expect_error($$select ns_commit(1, '[{"namespace": ["Sales"], "properties": {"k": "v"}}]')$$, '22023',
                    'names compare exactly: Sales is not sales');

-- case: a table needs its namespace and a namespace its parent; a namespace exists once on a ref
-- rules: NS-2
select expect_error($$select ns_commit(1, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000b1', '["nope"]', 't')))$$,
                    '22023', 'a table in a namespace that never existed');
select expect_error($$select ns_commit(1, '[{"namespace": ["a", "b"], "create": true}]')$$, '22023', 'no parent');
select ns_commit(1, '[{"namespace": ["a"], "create": true}, {"namespace": ["a", "b"], "create": true}]',
                 jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000b2', '["a", "b"]', 't')));
select expect(spaces(1) = 'a a.b db', 'a parent, its child and a table, in one commit');
select expect_error($$select ns_commit(1, '[{"namespace": ["a"], "create": true}]')$$, '40001', 'created twice');
select expect_error($$select ns_commit(1, '[{"namespace": ["x"], "drop": true}]')$$, '22023', 'dropping what never existed');
select expect_error($$select commit('{"ref": 1, "author": "a", "actor": "a", "operation": "update",
    "tables": [{"table_id": "00000000-0000-0000-0000-0000000000b2", "rename": {"namespace": ["nope"], "name": "t"}}]}')$$,
  '22023', 'a rename into a namespace that never existed');
select ns_commit(1, '[{"namespace": ["gone"], "create": true}]');
select ns_commit(1, '[{"namespace": ["gone"], "drop": true}]');
select expect_error($$select ns_commit(1, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000b3', '["gone"]', 't')))$$,
                    '40001', 'a table in a dropped namespace: the drop won');
select expect_error($$select ns_commit(1, '[{"namespace": ["gone"], "properties": {"k": "v"}}]')$$, '40001',
                    'properties of a dropped namespace');
select expect_error($$select ns_commit(1, '[{"namespace": ["gone", "c"], "create": true}]')$$, '40001',
                    'a child of a dropped namespace');

-- case: a namespace is dropped only when nothing is under it, and may be created again
-- rules: NS-3
select ns_commit(1, '[{"namespace": ["e"], "create": true}, {"namespace": ["p"], "create": true},
                      {"namespace": ["p", "c"], "create": true}]',
                 jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000c1', '["e"]', 't')));
select expect_error($$select ns_commit(1, '[{"namespace": ["e"], "drop": true}]')$$, '40001', 'a table under it');
select expect_error($$select ns_commit(1, '[{"namespace": ["p"], "drop": true}]')$$, '40001', 'a namespace under it');
select ns_commit(1, '[{"namespace": ["e"], "drop": true}]',
                 '[{"table_id": "00000000-0000-0000-0000-0000000000c1", "drop": true}]');
select expect(spaces(1) = 'db p p.c', 'its table dropped in the same commit, e goes');
select ns_commit(1, '[{"namespace": ["p", "c"], "drop": true}]');
select ns_commit(1, '[{"namespace": ["p"], "drop": true}]');
select ns_commit(1, '[{"namespace": ["e"], "create": true}]');
select expect(spaces(1) = 'db e', 'e created again; p and its child gone, child first');

-- case: branches see and merge namespaces as versioned keys, and a merge keeps the namespace rules
-- rules: VIS-9 NS-2 NS-5
select ns_commit(1, '[{"namespace": ["shared"], "create": true, "properties": {"k": "v0"}}]');
select create_ref('b', 1) as b \gset
select ns_commit(:b, '[{"namespace": ["br"], "create": true, "properties": {"k": "b"}}]');
select expect(spaces(1) = 'db shared' and spaces(:b) = 'br db shared', 'main does not see the branch''s namespace');
select merge(:b);
select expect(spaces(1) = 'br db shared' and props(1, '{br}') = '{"k": "b"}', 'the merge brings it and its property');
-- Both sides change one property: a conflict.
select create_ref('c', 1) as c \gset
select ns_commit(:c, '[{"namespace": ["shared"], "properties": {"k": "c"}}]');
select ns_commit(1, '[{"namespace": ["shared"], "properties": {"k": "main"}}]');
select expect_error(format('select merge(%s)', :c), '40001', 'a property changed on both sides');
-- The namespace itself differs: main creates q, the branch creates and drops it.
select create_ref('q', 1) as q \gset
select ns_commit(1, '[{"namespace": ["q"], "create": true}]');
select ns_commit(:q, '[{"namespace": ["q"], "create": true}]');
select ns_commit(:q, '[{"namespace": ["q"], "drop": true}]');
select expect_error(format('select merge(%s)', :q), '40001', 'a namespace created on one side and dropped on the other');
-- The branch puts a table in a namespace its parent dropped: a conflict.
select create_ref('d', 1) as d \gset
select ns_commit(:d, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000d1', '["br"]', 't')));
select ns_commit(1, '[{"namespace": ["br"], "drop": true}]');
select expect_error(format('select merge(%s)', :d), '40001', 'a table in a namespace dropped on the parent');

-- case: a writer refuses a catalog whose writer_version it does not know, and one of version 0 until migrated
-- rules: LAY-5
select expect((select value from meta where key = 'writer_version') = '2', 'a new catalog is at writer_version 2');
update meta set value = '3' where key = 'writer_version';
select expect_error($$select ns_commit(1, '[{"namespace": ["w"], "create": true}]')$$, '0A000', 'writer_version 3');
select expect_error($$select create_ref('w', 1)$$, '0A000', 'nor a branch');
delete from meta where key = 'writer_version';
select expect_error($$select ns_commit(1, '[{"namespace": ["w"], "create": true}]')$$, '0A000', 'version 0, not migrated');
select expect(open_catalog() is not null, 'a reader is not affected');

-- case: migrating a catalog of version 0 creates the namespaces of its tables, on main and on branches that do not see them
-- rules: NS-5
select ns_commit(1, '[{"namespace": ["only"], "create": true}, {"namespace": ["only", "here"], "create": true}]');
select ns_commit(1, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000e1', '["db"]', 'm')));
select create_ref('b', 1) as b \gset
select ns_commit(:b, '[]', jsonb_build_array(create_in('00000000-0000-0000-0000-0000000000e2', '["only", "here"]', 'x'))) as before \gset
-- The catalog as version 0 left it: no namespaces, no writer_version.
delete from namespace_property; delete from namespace; delete from meta where key = 'writer_version';
select migrate() as m \gset
select expect((select value from meta where key = 'writer_version') = '2', 'writer_version 2');
select expect((select operation from commit where commit_seq = :m) = 'migrate', 'a migrate commit on main');
select expect(spaces(1) = 'db', 'main: the namespace of its table');
select expect(spaces(:b) = 'db only only.here', 'the branch: what it does not see of main, and its own table''s, with its parent');
select expect(names(:b, :before) like '%only.here.x%' and spaces(:b, :before) = '',
              'history before the migration keeps its tables without namespaces, and reads it as before');
select expect(migrate() is null, 'a second migration does nothing');
select ns_commit(:b, '[{"namespace": ["only", "here"], "properties": {"k": "v"}}]');

-- case: the projection lists namespaces under a parent and loads one with its properties
-- rules: NS-1
select ns_commit(1, '[{"namespace": ["a"], "create": true, "properties": {"comment": "A"}},
                      {"namespace": ["a", "b"], "create": true}, {"namespace": ["a", "c"], "create": true}]');
select expect((select array_agg(x::text order by x::text) from iceberg_namespaces(1, '{}') x) = '{"[\"a\"]","[\"db\"]"}',
              'the top level');
select expect((select array_agg(x::text order by x::text) from iceberg_namespaces(1, '{a}') x)
              = '{"[\"a\", \"b\"]","[\"a\", \"c\"]"}', 'the children of a');
select expect(iceberg_namespace(1, '{a}') = '{"namespace": ["a"], "properties": {"comment": "A"}}', 'a and its properties');
select expect(iceberg_namespace(1, '{z}') is null, 'a namespace that does not exist');
