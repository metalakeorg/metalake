-- Versioned objects and tables (§5.4, §6). Rows are written directly, as in 20-visibility.

select new_table('gs://lake/t', 1) as t \gset
-- case: a field version is visible from the lowest level of the chain, and a dropped version hides it
-- rules: VIS-9
-- s1 main: 1 id, 2 v. s2 b forks main at 1. s3 b renames 2 to w. s4 main drops 1 and adds 3, after the fork.
-- s5 b changes its own rename: w to x. s6 b drops 2.
select fld(1, 'id', 'long', 1, 1, e => 4), fld(2, 'v', 'string', 1, 1);
select branch(2, 'b', 2, 1, 1);
select fld(2, 'w', 'string', 2, 3, e => 5);
select fld(1, 'id', 'long', 1, 4, dropped => true), fld(3, 'u', 'int', 1, 4);
select fld(2, 'x', 'string', 2, 5, e => 6);
select fld(2, 'x', 'string', 2, 6, dropped => true);
select expect(columns(1, 6) = '2:v:string 3:u:int', 'main: its own changes');
select expect(columns(2, 3) = '1:id:long 2:w:string', 'b at 3: its rename');
select expect(columns(2, 5) = '1:id:long 2:x:string', 'b at 5: its second rename; not main''s drop of 1');
select expect(columns(2, 6) = '1:id:long', 'b at 6: its drop hides 2');
select expect(columns(1, 3) = '1:id:long 2:v:string', 'main at 3: not b''s rename');

-- case: a ref has at most one live row per key, in every versioned table
-- rules: VIS-10
select branch(2, 'b', 2, 1, 1);
select fld(1, 'id', 'long', 1, 1);
select fld(1, 'id', 'long', 2, 3);
select expect_error('select fld(1, ''id2'', ''long'', 1, 2)', '23505', 'a second live field 1 in main');
insert into table_name select table_id, '{db}', 't', false, 1, 1 from tbl;
select expect_error(format('insert into table_name values (%L, ''{db}'', ''u'', false, 1, 2)', :'t'), '23505', 'a name');
insert into table_property select table_id, 'k', 'v', false, 1, 1 from tbl;
select expect_error(format('insert into table_property values (%L, ''k'', ''w'', false, 1, 2)', :'t'), '23505', 'a property');
insert into partition_spec select table_id, 0, '[]', 1, 1 from tbl;
select expect_error(format('insert into partition_spec values (%L, 1, ''[]'', 1, 2)', :'t'), '23505', 'a spec');
insert into sort_order select table_id, 1, '[]', 1, 1 from tbl;
select expect_error(format('insert into sort_order values (%L, 2, ''[]'', 1, 2)', :'t'), '23505', 'a sort order');
insert into cluster_key select table_id, '{1}', 1, 1 from tbl;
select expect_error(format('insert into cluster_key values (%L, ''{1}'', 1, 2)', :'t'), '23505', 'a cluster key');

-- case: a branch creates, renames and drops tables without touching its parent
-- rules: TBL-1
-- s1 main creates db.t. s2 b forks main at 1. s3 b creates db.n and renames db.t to db.r. s4 b drops db.n.
select new_table('gs://lake/n', 3) as n \gset
insert into table_name values (:'t', '{db}', 't', false, 1, 1);
select branch(2, 'b', 2, 1, 1);
insert into table_name values (:'n', '{db}', 'n', false, 2, 3, 4), (:'t', '{db}', 'r', false, 2, 3, 9223372036854775807);
insert into table_name values (:'n', '{db}', 'n', true, 2, 4);
select expect(names(1, 4) = 'db.t', 'main still has db.t only');
select expect(names(2, 3) = 'db.n db.r', 'b at 3');
select expect(names(2, 4) = 'db.r', 'b at 4: db.n is dropped');
select expect(table_by_name(2, '{db}', 'r', 4) = :'t' and table_by_name(1, '{db}', 't', 4) = :'t',
  'a rename keeps the table id');
select expect(table_by_name(2, '{db}', 't', 4) is null and table_by_name(1, '{db}', 'r', 4) is null,
  'each ref resolves only its own name');
select expect(names(1, 0) is null, 'before its creation, no table');

-- case: properties are versioned like any object, and removed with a dropped version
-- rules: TBL-2
select branch(2, 'b', 2, 1, 1);
insert into table_property values (:'t', 'format', 'parquet', false, 1, 1), (:'t', 'owner', 'ana', false, 1, 1);
insert into table_property values (:'t', 'owner', 'bea', false, 2, 3), (:'t', 'format', null, true, 2, 3);
select expect((select string_agg(key || '=' || value, ' ' order by key) from visible_properties(1, :'t', 3))
  = 'format=parquet owner=ana', 'main');
select expect((select string_agg(key || '=' || value, ' ' order by key) from visible_properties(2, :'t', 3))
  = 'owner=bea', 'b');

-- case: the default spec is the visible one, an old spec stays readable by id, and a spec id never changes meaning
-- rules: TBL-3
select fld(1, 'id', 'long', 1, 1), fld(2, 'ts', 'timestamp', 1, 1);
insert into partition_spec values (:'t', 0, '[{"source_ids": [2], "field_id": 1000, "name": "ts_day", "transform": "day"}]', 1, 1, 3);
insert into partition_spec values (:'t', 1, '[{"source_ids": [1], "field_id": 1001, "name": "id_b", "transform": "bucket[16]"}]', 1, 3);
select expect((select spec_id from default_spec(1, :'t', 2)) = 0 and (select spec_id from default_spec(1, :'t', 3)) = 1,
  'the default spec at 2 and at 3');
select expect((select fields -> 0 ->> 'transform' from partition_spec where table_id = :'t' and spec_id = 0) = 'day',
  'spec 0 is still there for the files written with it');
select expect(errors(1, 3) = '{}', 'valid');
select branch(2, 'b', 2, 1, 1);
insert into partition_spec values (:'t', 0, '[]', 2, 3);
select expect(errors(1, 3) = '{TBL-3}', 'spec 0 with two meanings');

-- case: partition fields use known transforms on the source types Iceberg allows, and primitive sources outside
-- lists and maps
-- rules: TBL-3
select fld(1, 'id', 'long', 1, 1), fld(2, 'd', 'date', 1, 1), fld(3, 'ok', 'boolean', 1, 1), fld(4, 'g', 'geometry', 1, 1),
       fld(5, 'tags', 'list', 1, 1), fld(6, 'element', 'string', 1, 1, parent => 5), fld(7, 's', 'struct', 1, 1),
       fld(8, 'x', 'int', 1, 1, parent => 7);
create function spec_errors(transform text, source int, pfid int default 1000) returns text[] language plpgsql as $$
begin
  delete from partition_spec;
  insert into partition_spec select table_id, 0, jsonb_build_array(jsonb_build_object(
    'source_ids', jsonb_build_array(source), 'field_id', pfid, 'name', 'p', 'transform', transform)), 1, 1 from tbl;
  return errors(1, 1);
end $$;
select expect(spec_errors('bucket[16]', 1) = '{}', 'bucket of a long');
select expect(spec_errors('truncate[10]', 1) = '{}', 'truncate of a long');
select expect(spec_errors('day', 2) = '{}', 'day of a date');
select expect(spec_errors('identity', 8) = '{}', 'a field nested in a struct');
select expect(spec_errors('void', 4) = '{}', 'void of anything');
select expect(spec_errors('hour', 2) = '{TBL-3}', 'hour of a date');
select expect(spec_errors('bucket[16]', 3) = '{TBL-3}', 'bucket of a boolean');
select expect(spec_errors('identity', 4) = '{TBL-3}', 'identity of a geometry');
select expect(spec_errors('bucket[0]', 1) = '{TBL-3}', 'zero buckets');
select expect(spec_errors('zorder', 1) = '{TBL-3}', 'an unknown transform');
select expect(spec_errors('identity', 6) = '{TBL-3}', 'a field inside a list');
select expect(spec_errors('identity', 7) = '{TBL-3}', 'a struct');
select expect(spec_errors('identity', 9) = '{TBL-3}', 'a field that is not there');
select expect(spec_errors('identity', 1, 999) = '{TBL-3}', 'a partition field id below 1000');

-- case: the default sort order is the visible one, the unsorted order has no row, and sort fields are valid
-- rules: TBL-4
select fld(1, 'id', 'long', 1, 1), fld(2, 'ok', 'boolean', 1, 1);
select expect(not exists (select 1 from default_sort_order(1, :'t', 1)), 'no row: unsorted');
select expect_error(format('insert into sort_order values (%L, 0, ''[]'', 1, 1)', :'t'), '23514', 'a row for order 0');
insert into sort_order values (:'t', 1,
  '[{"source_ids": [1], "transform": "identity", "direction": "desc", "null_order": "nulls-last"}]', 1, 1, 2);
select expect((select sort_order_id from default_sort_order(1, :'t', 1)) = 1 and errors(1, 1) = '{}', 'order 1');
insert into sort_order values (:'t', 2,
  '[{"source_ids": [2], "transform": "bucket[4]", "direction": "up", "null_order": "nulls-last"}]', 1, 2);
select expect(errors(1, 2) = '{TBL-4}', 'a bad direction and a transform that does not apply, in one field');

-- case: a cluster key names one to four distinct primitive fields outside lists and maps
-- rules: TBL-5
select fld(g, 'c' || g, 'long', 1, 1) from generate_series(1, 5) g;
select fld(6, 'm', 'map', 1, 1), fld(7, 'key', 'string', 1, 1, parent => 6, req => true),
       fld(8, 'value', 'long', 1, 1, parent => 6);
create function key_errors(ids int[]) returns text[] language plpgsql as $$
begin
  delete from cluster_key;
  insert into cluster_key select table_id, ids, 1, 1 from tbl;
  return errors(1, 1);
end $$;
select expect(key_errors('{1}') = '{}', 'one');
select expect(key_errors('{4,1,2,3}') = '{}', 'four');
select expect(key_errors('{1,2,3,4,5}') = '{TBL-5}', 'five');
select expect(key_errors('{}') = '{TBL-5}', 'none');
select expect(key_errors('{1,1}') = '{TBL-5}', 'repeated');
select expect(key_errors('{8}') = '{TBL-5}', 'inside a map');
select expect(key_errors('{6}') = '{TBL-5}', 'a map');
