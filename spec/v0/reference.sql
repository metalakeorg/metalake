-- MetaLake v0 · reference behaviour.
--
-- It defines the right answer, not the fast one. Loaded after schema.sql, in the catalog's schema.

-- LAY-3: refuse a catalog of an unknown version.
create function open_catalog() returns uuid language plpgsql stable as $$
declare v text := (select value from meta where key = 'format_version');
begin
  if v is distinct from '0' then
    raise exception 'metalake unsupported: format_version %', coalesce(v, 'missing') using errcode = '0A000';
  end if;
  return (select value::uuid from meta where key = 'catalog_uuid');
end $$;

-- ID-3: a new table identity and its counters.
create function new_table(location text, created_seq bigint) returns uuid language sql as $$
  with t as (insert into tbl values (gen_random_uuid(), location, created_seq) returning table_id)
  insert into table_counter (table_id) select table_id from t returning table_id;
$$;

-- ID-5: the next field, spec, partition field or sort order id of a table.
create function next_id(t uuid, kind text) returns int language plpgsql as $$
declare id int;
begin
  if kind not in ('field', 'spec', 'partition_field', 'sort_order') then
    raise exception 'metalake: unknown id kind %', kind;
  end if;
  update table_counter set
      last_field_id = last_field_id + (kind = 'field')::int,
      last_spec_id = last_spec_id + (kind = 'spec')::int,
      last_partition_field_id = last_partition_field_id + (kind = 'partition_field')::int,
      last_sort_order_id = last_sort_order_id + (kind = 'sort_order')::int
    where table_id = t
    returning case kind
      when 'field' then last_field_id when 'spec' then last_spec_id
      when 'partition_field' then last_partition_field_id else last_sort_order_id end
    into id;
  if id is null then
    raise exception 'metalake: unknown table %', t;
  end if;
  return id;
end $$;

-- VIS-3: the chain of a read of r at s.
create function chain(r int, s bigint) returns table (level int, ref_id int, seq bigint) language plpgsql stable as $$
declare seen int[] := '{}';
begin
  level := 0; ref_id := r; seq := s;
  loop
    if ref_id = any (seen) then
      raise exception 'metalake corrupt: the chain of ref % at % reaches itself', r, s using errcode = 'XX000';
    end if;
    seen := seen || ref_id;
    return next;
    select b.parent_ref_id, b.fork_seq into ref_id, seq
      from ref_base b where b.ref_id = chain.ref_id and b.begin_seq <= chain.seq and chain.seq < b.end_seq;
    exit when not found;
    level := level + 1;
  end loop;
end $$;

-- VIS-4 to VIS-6: the files of t visible at (r, s). The tombstones that apply are computed once, with the lowest
-- level each applies at; a row at level i is hidden by a tombstone at a level below i.
create function visible_files(r int, t uuid, s bigint) returns setof data_file language sql stable as $$
  with c as (select * from chain(r, s)),
  hidden as (
    select x.file_id, min(c.level) as level
      from tombstone x join c on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     where x.table_id = t
     group by x.file_id)
  select f.*
    from c
    join data_file f on f.table_id = t and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
    left join hidden h on h.file_id = f.file_id and h.level < c.level
   where h.file_id is null;
$$;

-- §5.4 · versioned objects: the visible version of each key at (r, s), the one valid at the lowest level of the
-- chain, unless it is dropped (VIS-9).

create function visible_table_names(r int, s bigint) returns setof table_name language sql stable as $$
  select (v).* from (
    select distinct on (x.table_id) x as v
      from chain(r, s) c join table_name x on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.table_id, c.level) y
   where not (v).dropped;
$$;

create function visible_properties(r int, t uuid, s bigint) returns setof table_property language sql stable as $$
  select (v).* from (
    select distinct on (x.key) x as v
      from chain(r, s) c join table_property x
        on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.key, c.level) y
   where not (v).dropped;
$$;

create function visible_fields(r int, t uuid, s bigint) returns setof field language sql stable as $$
  select (v).* from (
    select distinct on (x.field_id) x as v
      from chain(r, s) c join field x
        on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.field_id, c.level) y
   where not (v).dropped;
$$;

create function default_spec(r int, t uuid, s bigint) returns setof partition_spec language sql stable as $$
  select x.* from chain(r, s) c join partition_spec x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

create function default_sort_order(r int, t uuid, s bigint) returns setof sort_order language sql stable as $$
  select x.* from chain(r, s) c join sort_order x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

create function visible_cluster_key(r int, t uuid, s bigint) returns setof cluster_key language sql stable as $$
  select x.* from chain(r, s) c join cluster_key x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

-- TBL-1: the table a name denotes at (r, s), or null.
create function table_by_name(r int, ns text[], n text, s bigint) returns uuid language sql stable as $$
  select table_id from visible_table_names(r, s) where namespace = ns and name = n;
$$;

-- §7 · types

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
create function table_errors(r int, t uuid, s bigint) returns setof text language plpgsql stable as $$
declare
  fs field[] := array(select f from visible_fields(r, t, s) f);
  created bigint := (select created_seq from tbl where table_id = t);
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
  return query select format('TYP-5: required field %s was added without defaults', f.field_id)
    from unnest(fs) f
   where f.required and (f.initial_default is null or f.write_default is null)
     and (f.parent_field_id is null or (ancestor_types(fs, f.field_id))[1] = 'struct')
     and (select min(x.begin_seq) from field x where x.table_id = t and x.field_id = f.field_id) > created;
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
