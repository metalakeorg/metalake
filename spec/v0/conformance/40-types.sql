-- Types and schemas (§7), following Iceberg format version 3.

select new_table('gs://lake/t', 1) as t \gset

-- case: type strings are Iceberg's, in canonical form
-- rules: TYP-1
select expect(canonical_type(i) is not distinct from o, format('%s -> %s', i, coalesce(o, 'invalid')))
  from (values
    ('int', 'int'), ('timestamptz_ns', 'timestamptz_ns'), ('unknown', 'unknown'), ('variant', 'variant'),
    ('decimal(9,2)', 'decimal(9,2)'), ('decimal(9, 2)', 'decimal(9,2)'), ('decimal(38,0)', 'decimal(38,0)'),
    ('decimal(39,0)', null), ('decimal(0,0)', null), ('decimal(9)', null),
    ('fixed[16]', 'fixed[16]'), ('fixed(16)', null), ('fixed[0]', null),
    ('geometry', 'geometry'), ('geometry(OGC:CRS84)', 'geometry'), ('geometry(EPSG:4326)', 'geometry(EPSG:4326)'),
    ('geography', 'geography'), ('geography(OGC:CRS84,spherical)', 'geography'),
    ('geography(EPSG:4326, karney)', 'geography(EPSG:4326,karney)'), ('geography(EPSG:4326,flat)', null),
    ('struct', 'struct'), ('Int', null), ('varchar', null), ('file', null), ('', null)) x(i, o);
select fld(1, 'price', 'decimal(9, 2)', 1, 1), fld(2, 'id', 'long', 1, 1);
select expect(errors(1, 1) = '{TYP-1}', 'a stored type that is not canonical');

-- case: the visible fields form one tree with well-formed lists and maps, distinct siblings and ids in range
-- rules: TYP-2
create function tree_errors(extra text) returns text[] language plpgsql as $$
begin
  delete from field;
  perform fld(1, 'id', 'long', 1, 1, req => true), fld(2, 'tags', 'list', 1, 1), fld(3, 'element', 'string', 1, 1, parent => 2),
          fld(4, 'attrs', 'map', 1, 1), fld(5, 'key', 'string', 1, 1, parent => 4, req => true),
          fld(6, 'value', 'long', 1, 1, parent => 4), fld(7, 'loc', 'struct', 1, 1), fld(8, 'x', 'double', 1, 1, parent => 7);
  if extra is not null then execute extra; end if;
  return errors(1, 1);
end $$;
select expect(tree_errors(null) = '{}', 'a valid tree');
select expect(tree_errors('select fld(9, ''other'', ''int'', 1, 1, parent => 2)') = '{TYP-2}', 'a list with two children');
select expect(tree_errors('update field set name = ''item'' where field_id = 3') = '{TYP-2}', 'a list child not named element');
select expect(tree_errors('delete from field where field_id = 6') = '{TYP-2}', 'a map without value');
select expect(tree_errors('update field set required = false where field_id = 5') = '{TYP-2}', 'an optional map key');
select expect(tree_errors('select fld(9, ''y'', ''int'', 1, 1, parent => 1)') = '{TYP-2}', 'a primitive with a child');
select expect(tree_errors('select fld(9, ''y'', ''int'', 1, 1, parent => 99)') = '{TYP-2}', 'a parent that is not there');
select expect(tree_errors('select fld(9, ''y'', ''int'', 1, 1, parent => 7, pos => 8)') = '{TYP-2}', 'two siblings at one position');
select expect(tree_errors('select fld(9, ''x'', ''int'', 1, 1, parent => 7)') = '{TYP-2}', 'two siblings with one name');
select expect(tree_errors('select fld(9, ''x'', ''int'', 1, 1)') = '{}', 'the same name at another level');
select expect(tree_errors('select fld(2147483448, ''z'', ''int'', 1, 1)') = '{TYP-2}', 'a reserved field id');
select expect(tree_errors('update field set dropped = true where field_id = 7') = '{TYP-2}',
  'a dropped struct leaves its child without a parent');

-- case: a type changes only by Iceberg v3's promotions
-- rules: TYP-3
select expect(type_change_allowed(a, b) = ok, format('%s -> %s: %s', a, b, ok))
  from (values
    ('int', 'long', true), ('float', 'double', true), ('date', 'timestamp', true), ('date', 'timestamp_ns', true),
    ('decimal(9,2)', 'decimal(18,2)', true), ('decimal(9, 2)', 'decimal(10,2)', true),
    ('unknown', 'string', true), ('unknown', 'decimal(5,1)', true), ('long', 'long', true),
    ('long', 'int', false), ('int', 'double', false), ('int', 'string', false), ('double', 'float', false),
    ('date', 'timestamptz', false), ('date', 'timestamptz_ns', false), ('timestamp', 'timestamp_ns', false),
    ('decimal(9,2)', 'decimal(9,2)', true), ('decimal(9,2)', 'decimal(8,2)', false),
    ('decimal(9,2)', 'decimal(18,3)', false), ('fixed[4]', 'binary', false), ('string', 'binary', false),
    ('struct', 'string', false), ('string', 'unknown', false), ('int', 'nonsense', false)) x(a, b, ok);

-- case: date does not become a timestamp while an identity or bucket partition uses it, in any spec
-- rules: TYP-4
select fld(1, 'd', 'date', 1, 1), fld(2, 'e', 'date', 1, 1), fld(3, 'f', 'date', 1, 1);
insert into partition_spec values
  (:'t', 0, '[{"source_ids": [1], "field_id": 1000, "name": "d_b", "transform": "bucket[8]"},
              {"source_ids": [2], "field_id": 1001, "name": "e_day", "transform": "day"}]', 1, 1, 2),
  (:'t', 1, '[{"source_ids": [2], "field_id": 1001, "name": "e_day", "transform": "day"}]', 1, 2, 9223372036854775807);
select expect(not field_type_change_allowed(:'t', 1, 'date', 'timestamp'), 'd: bucket, in an old spec');
select expect(field_type_change_allowed(:'t', 2, 'date', 'timestamp_ns'), 'e: day gives the same values');
select expect(field_type_change_allowed(:'t', 3, 'date', 'timestamp'), 'f: no partition');
update partition_spec set end_seq = 3 where spec_id = 1;
insert into partition_spec values (:'t', 2, '[{"source_ids": [3], "field_id": 1002, "name": "f", "transform": "identity"}]', 1, 3);
select expect(not field_type_change_allowed(:'t', 3, 'date', 'timestamp'), 'f: identity');
select expect(not field_type_change_allowed(:'t', 2, 'date', 'string'), 'never what TYP-3 forbids');
select expect(field_type_change_allowed(:'t', 1, 'date', 'date'), 'no change');

-- case: defaults follow Iceberg's rules
-- rules: TYP-5
-- The table was created at 1; fields that begin at 1 were there from the start.
create function default_errors(extra text) returns text[] language plpgsql as $$
begin
  delete from field;
  perform fld(1, 'id', 'long', 1, 1, req => true),
          fld(2, 'n', 'int', 1, 2, req => true, init => '0', wdef => '1'),
          fld(3, 'tags', 'list', 1, 2, init => '[]', wdef => '["a"]'), fld(4, 'element', 'string', 1, 2, parent => 3),
          fld(5, 's', 'struct', 1, 2, init => '{}'), fld(6, 'x', 'double', 1, 2, parent => 5, init => '1.5'),
          fld(7, 'u', 'unknown', 1, 2), fld(8, 'when', 'date', 1, 2, init => '"2026-11-01"'),
          fld(9, 'ok', 'boolean', 1, 2, wdef => 'true');
  if extra is not null then execute extra; end if;
  return errors(1, 9);
end $$;
-- The same, of a field 10 that a commit adds.
create function added_errors(extra text) returns text[] language plpgsql as $$
begin
  perform default_errors(extra);
  return array(select distinct split_part(e, ':', 1) from added_field_errors(1, (select table_id from tbl), 9, '{10}') e);
end $$;
select expect(default_errors(null) = '{}', 'valid defaults');
select expect(default_errors('update field set initial_default = ''"a"'' where field_id = 4') = '{TYP-5}',
  'a list element with a default');
select expect(added_errors('select fld(10, ''r'', ''string'', 1, 3, req => true)') = '{TYP-5}',
  'a required field added later without defaults');
select expect(added_errors('select fld(10, ''r'', ''string'', 1, 3, req => true, init => ''"a"'')') = '{TYP-5}',
  'a required field added later without a write default');
select expect(default_errors('select fld(10, ''r'', ''string'', 1, 1, req => true)') = '{}',
  'a required field from the creation of the table needs none');
select expect(added_errors('select fld(10, ''y'', ''long'', 1, 3, parent => 5, req => true)') = '{TYP-5}',
  'a required struct field added later without defaults');
select expect(default_errors('update field set required = true where field_id = 7') = '{TYP-5}', 'a required unknown');
select expect(default_errors('select fld(10, ''v'', ''variant'', 1, 3, wdef => ''"x"'')') = '{TYP-5}',
  'a variant with a default');
select expect(default_errors('select fld(10, ''g'', ''geography(EPSG:4326,karney)'', 1, 3, init => ''"POINT (1 2)"'')') = '{TYP-5}',
  'a geography with a default');
select expect(default_errors('update field set initial_default = ''{"6": 2}'' where field_id = 5') = '{TYP-5}',
  'a struct default with field values');
select expect(default_errors('update field set write_default = ''1.5'' where field_id = 2') = '{TYP-5}',
  'an int default that is not an integer');
select expect(default_errors('update field set write_default = ''"true"'' where field_id = 9') = '{TYP-5}',
  'a boolean default that is a string');
select expect(default_errors('update field set initial_default = ''20261101'' where field_id = 8') = '{TYP-5}',
  'a date default that is not a string');
select expect(default_errors(
  'update field set end_seq = 4 where field_id = 2; select fld(2, ''n'', ''int'', 1, 4, req => true, init => ''5'', wdef => ''1'')')
  = '{TYP-5}', 'an initial default that changes');
select expect(default_errors(
  'update field set end_seq = 4 where field_id = 2; select fld(2, ''n'', ''long'', 1, 4, req => true, init => ''0'', wdef => ''7'')')
  = '{}', 'a write default and a type that change');

-- case: the schema renders as an Iceberg JSON struct, per ref
-- rules: TYP-6
select fld(1, 'id', 'long', 1, 1, req => true, pos => 1),
       fld(2, 'tags', 'list', 1, 1, pos => 3), fld(3, 'element', 'string', 1, 1, parent => 2, req => true),
       fld(4, 'attrs', 'map', 1, 1, pos => 4), fld(5, 'key', 'string', 1, 1, parent => 4, req => true),
       fld(6, 'value', 'decimal(9,2)', 1, 1, parent => 4),
       fld(7, 'loc', 'struct', 1, 1, pos => 2), fld(8, 'y', 'double', 1, 1, parent => 7, pos => 2),
       fld(9, 'x', 'double', 1, 1, parent => 7, pos => 1, init => '0', wdef => '0');
update field set doc = 'the key' where field_id = 1;
select branch(2, 'b', 2, 1, 1);
select fld(8, 'lat', 'double', 2, 3, parent => 7, pos => 2);
select expect(schema_json(1, :'t', 3) = '{"type": "struct", "fields": [
    {"id": 1, "name": "id", "required": true, "type": "long", "doc": "the key"},
    {"id": 7, "name": "loc", "required": false, "type": {"type": "struct", "fields": [
      {"id": 9, "name": "x", "required": false, "type": "double", "initial-default": 0, "write-default": 0},
      {"id": 8, "name": "y", "required": false, "type": "double"}]}},
    {"id": 2, "name": "tags", "required": false,
     "type": {"type": "list", "element-id": 3, "element-required": true, "element": "string"}},
    {"id": 4, "name": "attrs", "required": false, "type": {"type": "map", "key-id": 5, "key": "string",
      "value-id": 6, "value-required": false, "value": "decimal(9,2)"}}]}'::jsonb, 'main');
select expect(schema_json(2, :'t', 3) #>> '{fields,1,type,fields,1,name}' = 'lat', 'b sees its rename');
select expect(schema_json(1, :'t', 0) = '{"type": "struct", "fields": []}', 'before any field');
