-- MetaLake v0 · reference behaviour · tables, types and schemas (§6, §7).
--
-- It defines the right answer, not the fast one.

-- TYP-1: the canonical form of a type string, or null if it is not one.
create function canonical_type(t text) returns text language plpgsql immutable as $$
declare m text[];
begin
  if t in ('unknown', 'boolean', 'int', 'long', 'float', 'double', 'date', 'time', 'timestamp', 'timestamptz',
           'timestamp_ns', 'timestamptz_ns', 'string', 'uuid', 'binary', 'variant', 'geometry', 'geography',
           'struct', 'list', 'map') then
    return t;
  end if;
  m := regexp_match(t, '^decimal\(\s*([0-9]{1,2})\s*,\s*([0-9]{1,2})\s*\)$');
  if m is not null then
    return case when m[1]::int between 1 and 38 then format('decimal(%s,%s)', m[1]::int, m[2]::int) end;
  end if;
  m := regexp_match(t, '^fixed\[([0-9]{1,9})\]$');
  if m is not null then
    return case when m[1]::int >= 1 then format('fixed[%s]', m[1]::int) end;
  end if;
  m := regexp_match(t, '^geometry\(([^,()]+)\)$');
  if m is not null then
    return case when m[1] = 'OGC:CRS84' then 'geometry' else t end;
  end if;
  m := regexp_match(t, '^geography\(([^,()]+),\s*(spherical|vincenty|thomas|andoyer|karney)\)$');
  if m is not null then
    return case when m[1] = 'OGC:CRS84' and m[2] = 'spherical' then 'geography'
                else format('geography(%s,%s)', m[1], m[2]) end;
  end if;
  return null;
end $$;

-- The type without its parameters: decimal, fixed, geometry, geography, or the type itself.
create function base_type(t text) returns text language sql immutable as $$
  select regexp_replace(t, '[\[(].*$', '');
$$;

-- TYP-3: whether a field's type may change from a to b. No change is allowed too.
create function type_change_allowed(a text, b text) returns boolean language sql immutable as $$
  with x (a, b) as (select canonical_type(a), canonical_type(b))
  select coalesce(case
    when x.a is null or x.b is null then false
    when x.a = x.b or x.a = 'unknown' then true
    when (x.a, x.b) in (('int', 'long'), ('float', 'double'), ('date', 'timestamp'), ('date', 'timestamp_ns')) then true
    else (select p[2] = q[2] and q[1]::int > p[1]::int
            from regexp_match(x.a, '^decimal\((\d+),(\d+)\)$') p, regexp_match(x.b, '^decimal\((\d+),(\d+)\)$') q)
  end, false) from x;
$$;

-- TYP-4: the same, for field f of table t, given every partition spec the table has had.
create function field_type_change_allowed(t uuid, f int, a text, b text) returns boolean language sql stable as $$
  select type_change_allowed(a, b) and not (
    canonical_type(a) = 'date' and canonical_type(b) in ('timestamp', 'timestamp_ns') and exists (
      select 1 from partition_spec p, jsonb_array_elements(p.fields) pf
       where p.table_id = t and pf -> 'source_ids' @> to_jsonb(f)
         and (pf ->> 'transform' = 'identity' or pf ->> 'transform' like 'bucket[%')));
$$;

-- TBL-3: whether a transform is known and applies to a source type.
create function transform_applies(transform text, source_type text) returns boolean language sql immutable as $$
  with x (b) as (select base_type(canonical_type(source_type)))
  select coalesce(case
    when x.b in ('struct', 'list', 'map') then false
    when transform = 'identity' then x.b not in ('geometry', 'geography', 'variant')
    when transform ~ '^bucket\[[1-9][0-9]{0,8}\]$' then x.b in ('int', 'long', 'decimal', 'date', 'time',
      'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns', 'string', 'uuid', 'fixed', 'binary')
    when transform ~ '^truncate\[[1-9][0-9]{0,8}\]$' then x.b in ('int', 'long', 'decimal', 'string', 'binary')
    when transform in ('year', 'month', 'day') then x.b in ('date', 'timestamp', 'timestamptz', 'timestamp_ns',
      'timestamptz_ns')
    when transform = 'hour' then x.b in ('timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns')
    when transform = 'void' then true
    else false
  end, false) from x;
$$;

-- TYP-5: whether a default has the JSON single-value shape of its type.
create function default_shape_ok(t text, d jsonb) returns boolean language sql immutable as $$
  select d is null or case base_type(t)
    when 'boolean' then jsonb_typeof(d) = 'boolean'
    when 'int' then jsonb_typeof(d) = 'number' and d::text ~ '^-?[0-9]+$'
    when 'long' then jsonb_typeof(d) = 'number' and d::text ~ '^-?[0-9]+$'
    when 'float' then jsonb_typeof(d) = 'number'
    when 'double' then jsonb_typeof(d) = 'number'
    when 'struct' then d = '{}'
    when 'list' then jsonb_typeof(d) = 'array'
    when 'map' then jsonb_typeof(d) = 'object' and jsonb_typeof(d -> 'keys') = 'array'
      and jsonb_typeof(d -> 'values') = 'array'
    else jsonb_typeof(d) = 'string'
  end;
$$;

-- The types of the ancestors of field f, nearest first.
create function ancestor_types(fs field[], f int) returns text[] language plpgsql immutable as $$
declare types text[] := '{}'; p int := (select x.parent_field_id from unnest(fs) x where x.field_id = f);
begin
  while p is not null and cardinality(types) <= cardinality(fs) loop
    types := types || (select x.type from unnest(fs) x where x.field_id = p);
    p := (select x.parent_field_id from unnest(fs) x where x.field_id = p);
  end loop;
  return types;
end $$;

-- A field a partition, a sort order or a cluster key may use: visible, primitive, not inside a list or a map.
create function usable_source(fs field[], f int) returns boolean language sql immutable as $$
  select exists (select 1 from unnest(fs) x where x.field_id = f and x.type not in ('struct', 'list', 'map'))
     and not ancestor_types(fs, f) && '{list,map}';
$$;

-- TYP-6: the schema of t at (r, s) as an Iceberg JSON struct.
create function schema_json(r int, t uuid, s bigint) returns jsonb language plpgsql stable as $$
begin
  return struct_json(array(select f from visible_fields(r, t, s) f), null);
end $$;

create function struct_json(fs field[], parent int) returns jsonb language plpgsql immutable as $$
begin
  return jsonb_build_object('type', 'struct', 'fields', coalesce((
    select jsonb_agg(
      jsonb_build_object('id', f.field_id, 'name', f.name, 'required', f.required, 'type', type_json(fs, f))
      || case when f.doc is null then '{}' else jsonb_build_object('doc', f.doc) end
      || case when f.initial_default is null then '{}' else jsonb_build_object('initial-default', f.initial_default) end
      || case when f.write_default is null then '{}' else jsonb_build_object('write-default', f.write_default) end
      order by f.position)
      from unnest(fs) f where f.parent_field_id is not distinct from parent), '[]'));
end $$;

create function type_json(fs field[], f field) returns jsonb language plpgsql immutable as $$
declare e field; k field; v field;
begin
  case f.type
    when 'struct' then
      return struct_json(fs, f.field_id);
    when 'list' then
      select * into e from unnest(fs) x where x.parent_field_id = f.field_id and x.name = 'element';
      return jsonb_build_object('type', 'list', 'element-id', e.field_id, 'element-required', e.required,
        'element', type_json(fs, e));
    when 'map' then
      select * into k from unnest(fs) x where x.parent_field_id = f.field_id and x.name = 'key';
      select * into v from unnest(fs) x where x.parent_field_id = f.field_id and x.name = 'value';
      return jsonb_build_object('type', 'map', 'key-id', k.field_id, 'key', type_json(fs, k),
        'value-id', v.field_id, 'value-required', v.required, 'value', type_json(fs, v));
    else
      return to_jsonb(f.type);
  end case;
end $$;

-- Every rule of §6 and §7 that table t breaks at (r, s), one line each, prefixed by the rule id. Empty when the
-- table is valid.
-- TYP-5: the fields `added`, new to table t at (r, s), that a table that exists cannot take. It looks at the commit that
-- adds them, since what says that a field came later is its first version, which collection takes.
create function added_field_errors(r int, t uuid, s bigint, added int[]) returns setof text language sql stable as $$
  with fs as (select array(select f from visible_fields(r, t, s) f) as all_fields)
  select format('TYP-5: required field %s was added without defaults', f.field_id)
    from fs, unnest(fs.all_fields) f
   where f.field_id = any (added) and f.required and (f.initial_default is null or f.write_default is null)
     and (f.parent_field_id is null or (ancestor_types(fs.all_fields, f.field_id))[1] = 'struct');
$$;

create function table_errors(r int, t uuid, s bigint) returns setof text language plpgsql stable as $$
declare
  fs field[] := array(select f from visible_fields(r, t, s) f);
begin
  return query select format('TYP-1: field %s has type %s', f.field_id, f.type)
    from unnest(fs) f where canonical_type(f.type) is distinct from f.type;

  return query select format('TYP-2: field %s has no visible struct, list or map parent', f.field_id)
    from unnest(fs) f where f.parent_field_id is not null and not exists (
      select 1 from unnest(fs) p where p.field_id = f.parent_field_id and p.type in ('struct', 'list', 'map'));
  return query select format('TYP-2: %s %s has children %s', p.type, p.field_id, k.names)
    from unnest(fs) p, lateral (
      select coalesce(array_agg(c.name order by c.name), '{}') as names from unnest(fs) c
       where c.parent_field_id = p.field_id) k
   where (p.type = 'list' and k.names <> '{element}')
      or (p.type = 'map' and k.names <> '{key,value}')
      or (p.type not in ('struct', 'list', 'map') and k.names <> '{}');
  return query select format('TYP-2: the key of map %s is optional', c.parent_field_id)
    from unnest(fs) c join unnest(fs) p on p.field_id = c.parent_field_id
   where p.type = 'map' and c.name = 'key' and not c.required;
  return query select format('TYP-2: fields under %s share a name or a position',
                             coalesce(f.parent_field_id::text, 'the table'))
    from unnest(fs) f group by f.parent_field_id
   having count(distinct f.name) < count(*) or count(distinct f.position) < count(*);
  return query select format('TYP-2: field id %s is out of range', f.field_id)
    from unnest(fs) f where f.field_id not between 1 and 2147483447;

  return query select format('TYP-5: field %s, in a list or a map, has a default', c.field_id)
    from unnest(fs) c join unnest(fs) p on p.field_id = c.parent_field_id
   where p.type in ('list', 'map') and (c.initial_default is not null or c.write_default is not null);
  return query select format('TYP-5: the initial default of field %s changed', x.field_id)
    from field x where x.table_id = t group by x.field_id
   having count(distinct coalesce(x.initial_default::text, '')) > 1;
  return query select format('TYP-5: field %s of type %s must be optional with null defaults', f.field_id, f.type)
    from unnest(fs) f
   where base_type(f.type) in ('unknown', 'variant', 'geometry', 'geography')
     and (f.initial_default is not null or f.write_default is not null or (f.type = 'unknown' and f.required));
  return query select format('TYP-5: a default of field %s is not a %s', f.field_id, f.type)
    from unnest(fs) f
   where not (default_shape_ok(f.type, f.initial_default) and default_shape_ok(f.type, f.write_default));

  return query select format('TBL-3: spec %s has more than one list of fields', x.spec_id)
    from partition_spec x where x.table_id = t group by x.spec_id having count(distinct x.fields) > 1;
  return query select format('TBL-3: partition field %s is invalid', pf)
    from default_spec(r, t, s) p, jsonb_array_elements(p.fields) pf
   where jsonb_array_length(pf -> 'source_ids') <> 1
      or (pf ->> 'field_id')::int < 1000
      or not usable_source(fs, (pf -> 'source_ids' ->> 0)::int)
      or not transform_applies(pf ->> 'transform',
        (select f.type from unnest(fs) f where f.field_id = (pf -> 'source_ids' ->> 0)::int));

  return query select format('TBL-4: sort order %s has more than one list of fields', x.sort_order_id)
    from sort_order x where x.table_id = t group by x.sort_order_id having count(distinct x.fields) > 1;
  return query select format('TBL-4: sort field %s is invalid', sf)
    from default_sort_order(r, t, s) o, jsonb_array_elements(o.fields) sf
   where jsonb_array_length(sf -> 'source_ids') <> 1
      or sf ->> 'direction' is distinct from 'asc' and sf ->> 'direction' is distinct from 'desc'
      or sf ->> 'null_order' is distinct from 'nulls-first' and sf ->> 'null_order' is distinct from 'nulls-last'
      or not usable_source(fs, (sf -> 'source_ids' ->> 0)::int)
      or not transform_applies(sf ->> 'transform',
        (select f.type from unnest(fs) f where f.field_id = (sf -> 'source_ids' ->> 0)::int));

  return query select format('TBL-5: cluster key %s is invalid', k.field_ids)
    from visible_cluster_key(r, t, s) k
   where cardinality(k.field_ids) not between 1 and 4
      or (select count(distinct i) from unnest(k.field_ids) i) < cardinality(k.field_ids)
      or exists (select 1 from unnest(k.field_ids) i where not usable_source(fs, i));
end $$;
