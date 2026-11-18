-- Ownership (§13.4), and what it binds: collection (RET-4), a path once per table (CMT-9), the version (§18).

create function cm(tables jsonb, r int default 1, op text default 'append') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', op, 'tables', tables));
$$;
create function tj(t uuid, entry jsonb) returns jsonb language sql as $$
  select jsonb_build_array(jsonb_build_object('table_id', t) || entry);
$$;
-- A file at `path`: under the table's location, or anywhere else, as a Hive table's files are.
create function fp(id bigint, path text) returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', path, 'record_count', 10, 'file_size', 1000, 'footer_size', 100);
$$;
-- A table at gs://lake/<name>, which owns that location alone.
create function mk(name text) returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', jsonb_build_object('namespace', '["db"]'::jsonb, 'name', name, 'location', 'gs://lake/' || name),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true}]'::jsonb)));
  return t;
end $$;
create function adds(t uuid, files jsonb, r int default 1) returns bigint language sql as $$
  select cm(tj(t, jsonb_build_object('add', files)), r);
$$;
create function queued() returns text[] language sql as $$
  select coalesce(array_agg(path order by path), '{}') from deletion_queue;
$$;

-- case: a table owns what lies under its location or an adopted prefix, at a / of the path
-- rules: OWN-1
select mk('t') as t \gset
select expect(owns(:'t', 'gs://lake/t/a.parquet') and owns(:'t', 'gs://lake/t/d/b.parquet'), 'under its location');
select expect(not owns(:'t', 'gs://lake/t2/a.parquet'), 'a prefix ends at a / of the path: t is not t2');
select expect(not owns(:'t', 'gs://hive/ventas/dt=1/a.parquet'), 'elsewhere: referenced');
select adds(:'t', jsonb_build_array(fp(1, 'gs://hive/ventas/dt=1/a.parquet')));
select cm(tj(:'t', '{"own": ["gs://hive/ventas/"]}'::jsonb));
select expect(owns(:'t', 'gs://hive/ventas/dt=1/a.parquet'), 'under an adopted prefix, written with a trailing /');
select expect(not owns(:'t', 'gs://hive/ventas2/a.parquet') and not owns(:'t', 'gs://hive/other/a.parquet'),
  'only under it');
select mk('u') as u \gset
select expect(not owns(:'u', 'gs://hive/ventas/dt=1/a.parquet'), 'a prefix is one table''s');

-- case: a commit adds owned prefixes of a store the table holds files on, at its sequence, and none goes
-- rules: OWN-2
select mk('t') as t \gset
select adds(:'t', jsonb_build_array(fp(1, 's3://old/hive/a.parquet')));
select cm(tj(:'t', '{"own": ["s3://old/hive"]}'::jsonb)) as s \gset
select expect((select adopted_seq from owned_prefix where table_id = :'t' and prefix = 's3://old/hive') = :s,
  'recorded at its commit''s sequence');
select expect_error(format($$select cm(tj(%L, '{"own": ["az://elsewhere/x"]}'::jsonb))$$, :'t'), '22023',
  'a store the table holds nothing on');
select expect_error(format($$select cm(tj(%L, '{"own": ["gs://lake/t?x=1"]}'::jsonb))$$, :'t'), '22023',
  'a query is no prefix');
select cm(tj(:'t', '{"own": ["s3://old/hive/"]}'::jsonb));
select expect((select count(*) from owned_prefix where table_id = :'t') = 1, 'adopting again changes nothing');
select cm(tj(:'t', '{"properties": {"x": "y"}}'::jsonb));
select expect((select count(*) from owned_prefix where table_id = :'t') = 1, 'no commit removes one');

-- case: expiring queues only what a table owns; a referenced object stays, until its prefix is adopted
-- rules: RET-4, OWN-1
select mk('t') as t \gset
select adds(:'t', jsonb_build_array(fp(1, 'gs://hive/ventas/dt=1/a.parquet'), fp(2, 'gs://lake/t/b.parquet'),
  fp(3, 'gs://hive/ventas/dt=2/c.parquet')));
-- A compaction: the two old files go, into one under the table's location.
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fp(4, 'gs://lake/t/d.parquet')), 'remove', '[1, 2]'::jsonb)),
  1, 'replace');
select set_retention(1, min_snapshots => 0);
select expire();
select expect(queued() = '{gs://lake/t/b.parquet}', 'the owned file only: the referenced one is forgotten, not queued');
select expect(not exists (select 1 from data_file where path = 'gs://hive/ventas/dt=1/a.parquet'), 'its rows are gone');
select cm(tj(:'t', '{"own": ["gs://hive/ventas"]}'::jsonb));
select cm(tj(:'t', jsonb_build_object('add', jsonb_build_array(fp(5, 'gs://lake/t/e.parquet')), 'remove', '[3]'::jsonb)),
  1, 'replace');
select expire();
select expect(queued() = '{gs://hive/ventas/dt=2/c.parquet,gs://lake/t/b.parquet}', 'once adopted, it is owned');

-- case: a path is queued when its last row goes and a table owns it, whichever table held that row
-- rules: RET-4
select mk('t') as t \gset
select mk('u') as u \gset
select adds(:'t', jsonb_build_array(fp(1, 'gs://lake/t/shared.parquet')));
select adds(:'u', jsonb_build_array(fp(1, 'gs://lake/t/shared.parquet')));
select cm(tj(:'t', '{"remove": [1]}'::jsonb), 1, 'delete');
select set_retention(1, min_snapshots => 0);
select expire();
select expect(queued() = '{}', 'u still holds it');
select cm(tj(:'u', '{"remove": [1]}'::jsonb), 1, 'delete');
select expire();
select expect(queued() = '{gs://lake/t/shared.parquet}', 'the last row was u''s, which only references it; t owns it');

-- case: a table holds a path once on a ref; another table may hold it too
-- rules: CMT-9
select mk('t') as t \gset
select mk('u') as u \gset
select adds(:'t', jsonb_build_array(fp(1, 'gs://hive/a.parquet')));
select expect_error(format($$select adds(%L, jsonb_build_array(fp(2, 'gs://hive/a.parquet')))$$, :'t'), '22023',
  'the same path again');
select expect_error(format($$select adds(%L, jsonb_build_array(fp(3, 'gs://hive/b.parquet'), fp(4, 'gs://hive/b.parquet')))$$, :'t'),
  '22023', 'twice in one commit');
select create_ref('dev', 1) as dev \gset
select expect_error(format($$select adds(%L, jsonb_build_array(fp(5, 'gs://hive/a.parquet')), %s)$$, :'t', :dev), '22023',
  'a branch sees it by inheritance');
select expect(adds(:'u', jsonb_build_array(fp(1, 'gs://hive/a.parquet'))) > 0, 'another table holds it too');
select cm(tj(:'t', '{"remove": [1]}'::jsonb), 1, 'delete');
select expect(adds(:'t', jsonb_build_array(fp(6, 'gs://hive/a.parquet'))) > 0, 'once removed, it may come back');

-- case: a name mapping moves the catalog to format_version 1, which a reader of revision 0.5 opens
-- rules: LAY-2, LAY-3
select mk('t') as t \gset
select expect((select value from meta where key = 'format_version') = '0', 'no mapping: version 0');
select cm(tj(:'t', '{"properties": {"other": "x"}}'::jsonb));
select expect((select value from meta where key = 'format_version') = '0', 'another property changes nothing');
select cm(tj(:'t', jsonb_build_object('properties', jsonb_build_object('schema.name-mapping.default',
  '[{"field-id": 1, "names": ["id"]}]'))));
select expect((select value from meta where key = 'format_version') = '1', 'a mapping: version 1');
select expect(open_catalog() is not null, 'and it opens');

-- case: migrating a catalog of revision 0.3 creates owned_prefix, empty, and changes no row
-- rules: LAY-5
select mk('t') as t \gset
select adds(:'t', jsonb_build_array(fp(1, 'gs://lake/t/a.parquet')));
drop table owned_prefix;
update meta set value = '3' where key = 'writer_version';
select expect_error(format($$select cm(tj(%L, '{"properties": {"x": "y"}}'::jsonb))$$, :'t'), '0A000', 'not migrated');
create temp table rows_before as select (select count(*) from data_file) f, (select count(*) from snapshot) s;
select expect(migrate() is null, 'no commit');
select expect((select value from meta where key = 'writer_version') = '4', 'writer_version 4');
select expect(to_regclass('owned_prefix') is not null and (select count(*) from owned_prefix) = 0, 'owned_prefix, empty');
select expect((select (f, s) from rows_before) = ((select count(*) from data_file), (select count(*) from snapshot)),
  'no row changed');
select expect(owns(:'t', 'gs://lake/t/a.parquet'), 'what its writers wrote stays owned');
