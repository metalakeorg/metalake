-- Catalog layout and identity (§2, §3).

-- case: the catalog holds exactly the tables of §4, with no tenant column
-- rules: LAY-1
select expect(
  (select array_agg(table_name::text order by table_name) from information_schema.tables
    where table_schema = current_schema()) =
  array['cluster_key', 'commit', 'data_file', 'field', 'meta', 'partition_spec', 'ref', 'ref_base', 'sort_order',
        'table_counter', 'table_name', 'table_property', 'tbl', 'tombstone'],
  'the tables of §4');
select expect(not exists (
  select 1 from information_schema.columns where table_schema = current_schema() and column_name ~ 'tenant'),
  'no tenant column');

-- case: meta holds the format version, the catalog UUID and the creation time
-- rules: LAY-2
select expect((select value from meta where key = 'format_version') = '0', 'format_version 0');
select expect((select value::uuid is not null from meta where key = 'catalog_uuid'), 'a catalog UUID');
select expect((select abs(extract(epoch from value::timestamptz - now())) < 60 from meta where key = 'created_at'),
  'created_at is the creation time');

-- case: a reader refuses an unknown or missing format version
-- rules: LAY-3
select expect(open_catalog() = (select value::uuid from meta where key = 'catalog_uuid'), 'version 0 opens');
update meta set value = '1' where key = 'format_version';
select expect_error('select open_catalog()', '0A000', 'version 1');
delete from meta where key = 'format_version';
select expect_error('select open_catalog()', '0A000', 'no version');

-- case: validity ranges are bigint, never NULL at the end, and never empty
-- rules: LAY-4
select expect(not exists (
  select 1 from information_schema.columns
   where table_schema = current_schema() and column_name in ('begin_seq', 'end_seq', 'fork_seq', 'created_seq')
     and (data_type <> 'bigint' or is_nullable = 'YES')), 'bigint, not null');
select expect((select column_default from information_schema.columns
   where table_schema = current_schema() and table_name = 'data_file' and column_name = 'end_seq')
  = '''9223372036854775807''::bigint', 'end_seq defaults to OPEN');
select new_table('gs://lake/t', 0) \gset
select expect_error(format('insert into data_file (table_id, file_id, ref_id, begin_seq, end_seq, record_count, file_size)
  values (%L, 1, 1, 5, 5, 1, 1)', :'new_table'), '23514', 'an empty range');

-- case: commit_seq is drawn from a sequence that starts at 1 and never cycles, and identifies one commit
-- rules: ID-1
select expect((select start_value = 1 and not cycle and data_type = 'bigint'::regtype
  from pg_sequences where schemaname = current_schema() and sequencename = 'commit_seq_gen'), 'commit_seq_gen');
insert into commit (commit_seq, ref_id, xid, author, actor, operation)
  values (nextval('commit_seq_gen'), 1, pg_current_xact_id(), 'a', 'a', 'append');
select expect_error(format('insert into commit (commit_seq, ref_id, xid, author, actor, operation)
  values (%s, 1, pg_current_xact_id(), %L, %L, %L)', currval('commit_seq_gen'), 'a', 'a', 'append'),
  '23505', 'a second commit with the same commit_seq');

-- case: main is ref 1 from the start, and other refs are drawn from 2
-- rules: ID-2
select expect((select name = 'main' and kind = 'branch' and created_seq = 0 from ref where ref_id = 1), 'main');
select expect((select count(*) = 1 from ref), 'only main');
select expect(nextval('ref_id_gen') = 2, 'the first other ref is 2');

-- case: table ids are UUIDs, never reused
-- rules: ID-3
select expect((select count(distinct new_table('gs://lake/t' || g, 0)) = 100 from generate_series(1, 100) g),
  '100 tables, 100 ids');
select expect((select data_type = 'uuid' from information_schema.columns
  where table_schema = current_schema() and table_name = 'tbl' and column_name = 'table_id'), 'uuid');

-- case: file ids are unique in the catalog, across tables
-- rules: ID-4
select expect((select count(distinct nextval('file_id_gen')) = 1000 from generate_series(1, 1000)),
  '1000 draws, 1000 ids');
select expect((select data_type = 'bigint'::regtype and not cycle
  from pg_sequences where schemaname = current_schema() and sequencename = 'file_id_gen'), 'file_id_gen');

-- case: field, spec, partition field and sort order ids increase per table and per kind from their first values
-- rules: ID-5
select new_table('gs://lake/a', 0) as a, new_table('gs://lake/b', 0) as b \gset
select expect(next_id(:'a', 'field') = 1, 'a field 1');
select expect(next_id(:'a', 'field') = 2, 'a field 2');
select expect(next_id(:'a', 'spec') = 0, 'a spec 0');
select expect(next_id(:'a', 'spec') = 1, 'a spec 1');
select expect(next_id(:'a', 'partition_field') = 1000, 'a partition field 1000');
select expect(next_id(:'b', 'field') = 1, 'b field 1');
select expect(next_id(:'a', 'sort_order') = 1, 'a sort order 1');
select expect(next_id(:'a', 'field') = 3, 'a field 3');
select expect_error(format('select next_id(%L, %L)', :'a', 'column'), 'P0001', 'an unknown kind');
select expect_error(format('select next_id(%L, %L)', gen_random_uuid(), 'field'), 'P0001', 'an unknown table');
