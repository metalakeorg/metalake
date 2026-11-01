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

-- VIS-3: the chain of a read of r at s. `since` is the begin_seq of the ref_base row each level reads through,
-- 0 at a level without one (CMT-2).
create function chain(r int, s bigint) returns table (level int, ref_id int, seq bigint, since bigint)
language plpgsql stable as $$
declare
  seen int[] := '{}';
  parent int;
  fork bigint;
begin
  level := 0; ref_id := r; seq := s;
  loop
    if ref_id = any (seen) then
      raise exception 'metalake corrupt: the chain of ref % at % reaches itself', r, s using errcode = 'XX000';
    end if;
    seen := seen || ref_id;
    select b.parent_ref_id, b.fork_seq, b.begin_seq into parent, fork, since
      from ref_base b where b.ref_id = chain.ref_id and b.begin_seq <= chain.seq and chain.seq < b.end_seq;
    if not found then
      since := 0;
      return next;
      exit;
    end if;
    return next;
    ref_id := parent; seq := fork; level := level + 1;
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

-- §8 · statistics

-- STA-5: a 64-bit or 32-bit integer, big-endian with the sign bit flipped.
create function encode_long(v bigint) returns bytea language sql immutable as $$
  select int8send(v # (-9223372036854775808)::bigint);
$$;

create function encode_int32(v int) returns bytea language sql immutable as $$
  select int4send(v # (-2147483648)::int);
$$;

create function decode_long(b bytea) returns bigint language sql immutable as $$
  select ('x' || encode(b, 'hex'))::bit(64)::bigint # (-9223372036854775808)::bigint;
$$;

create function decode_int32(b bytea) returns int language sql immutable as $$
  select ('x' || encode(b, 'hex'))::bit(32)::int # (-2147483648)::int;
$$;

-- STA-5: a double in IEEE 754 totalOrder. NaN is accepted here only for the z placeholder of STA-9.
create function order_double(v float8) returns bytea language sql immutable as $$
  with x (b) as (select ('x' || encode(float8send(v), 'hex'))::bit(64)::bigint)
  select int8send(case when b >= 0 then b # (-9223372036854775808)::bigint else ~b end) from x;
$$;

-- STA-6: NaN is never a bound.
create function encode_double(v float8) returns bytea language plpgsql immutable as $$
begin
  if v = 'NaN' then
    raise exception 'metalake: NaN is never a bound' using errcode = '22023';
  end if;
  return order_double(v);
end $$;

-- STA-5: the unscaled value of a decimal of scale `scale`, as 16 bytes.
create function encode_decimal(v numeric, scale int) returns bytea language plpgsql immutable as $$
declare
  u numeric := v * 10::numeric ^ scale;
  b bytea := '';
begin
  if u <> trunc(u) then
    raise exception 'metalake: % has more than % decimal places', v, scale using errcode = '22023';
  end if;
  u := u + 2::numeric ^ 127;
  for i in 1..16 loop
    b := set_byte('\x00'::bytea, 0, mod(u, 256)::int) || b;
    u := div(u, 256);
  end loop;
  return b;
end $$;

-- Units from 1970-01-01 00:00:00 UTC, or from midnight for a time, of an ISO 8601 text with up to `digits`
-- fractional digits: 6 for microseconds, 9 for nanoseconds.
create function epoch_units(v text, kind text, digits int) returns numeric language plpgsql immutable as $$
declare
  frac text := coalesce(substring(v from '\.([0-9]+)'), '');
  whole text := regexp_replace(v, '\.[0-9]+', '');
  secs numeric;
begin
  if length(frac) > digits then
    raise exception 'metalake: % has more than % fractional digits', v, digits using errcode = '22023';
  end if;
  secs := case kind
    when 'time' then extract(epoch from whole::time)
    when 'timestamp' then extract(epoch from whole::timestamp)
    else extract(epoch from whole::timestamptz) end;
  return secs * 10::numeric ^ digits + rpad(frac, digits, '0')::numeric;
end $$;

-- STA-5: the encoded bound of value `v` of type `t`. `v` is text: numbers as Postgres reads them (`-0`,
-- `Infinity`), temporal values in ISO 8601, `fixed` and `binary` in hex, the rest as is.
create function encode_bound(t text, v text) returns bytea language plpgsql immutable as $$
declare c text := canonical_type(t);
begin
  return case base_type(c)
    when 'boolean' then case when v::boolean then '\x01'::bytea else '\x00'::bytea end
    when 'int' then encode_long(v::int)
    when 'long' then encode_long(v::bigint)
    when 'date' then encode_int32(v::date - date '1970-01-01')
    when 'time' then encode_long(epoch_units(v, 'time', 6)::bigint)
    when 'timestamp' then encode_long(epoch_units(v, 'timestamp', 6)::bigint)
    when 'timestamptz' then encode_long(epoch_units(v, 'timestamptz', 6)::bigint)
    when 'timestamp_ns' then encode_long(epoch_units(v, 'timestamp', 9)::bigint)
    when 'timestamptz_ns' then encode_long(epoch_units(v, 'timestamptz', 9)::bigint)
    when 'float' then encode_double(v::float4::float8)
    when 'double' then encode_double(v::float8)
    when 'decimal' then encode_decimal(v::numeric, (regexp_match(c, ',([0-9]+)\)'))[1]::int)
    when 'string' then convert_to(v, 'UTF8')
    when 'uuid' then decode(replace(v::uuid::text, '-', ''), 'hex')
    when 'fixed' then decode(v, 'hex')
    when 'binary' then decode(v, 'hex')
  end;
end $$;

-- STA-8: a lower bound, truncated.
create function truncate_lower(t text, b bytea) returns bytea language sql immutable as $$
  select case base_type(canonical_type(t))
    when 'string' then convert_to(left(convert_from(b, 'UTF8'), 16), 'UTF8')
    when 'binary' then substring(b from 1 for 16)
    else b end;
$$;

-- STA-8: an upper bound, truncated and incremented; NULL if nothing can be incremented.
create function truncate_upper(t text, b bytea) returns bytea language plpgsql immutable as $$
declare
  s text;
  cp int;
begin
  if base_type(canonical_type(t)) = 'string' then
    s := convert_from(b, 'UTF8');
    if length(s) <= 16 then return b; end if;
    s := left(s, 16);
    while s <> '' loop
      cp := ascii(right(s, 1));
      s := left(s, -1);
      if cp < 1114111 then
        cp := case when cp + 1 between 55296 and 57343 then 57344 else cp + 1 end;
        return convert_to(s || chr(cp), 'UTF8');
      end if;
    end loop;
    return null;
  elsif base_type(canonical_type(t)) = 'binary' then
    if length(b) <= 16 then return b; end if;
    b := substring(b from 1 for 16);
    while length(b) > 0 loop
      cp := get_byte(b, length(b) - 1);
      b := substring(b from 1 for length(b) - 1);
      if cp < 255 then
        return b || set_byte('\x00'::bytea, 0, cp + 1);
      end if;
    end loop;
    return null;
  end if;
  return b;
end $$;

-- STA-9: a corner of a geospatial bounding box.
create function encode_point(x float8, y float8, z float8 default null, m float8 default null) returns bytea
language sql immutable as $$
  select encode_double(x) || encode_double(y)
      || case when z is not null then encode_double(z) when m is not null then order_double('NaN') else '' end
      || case when m is not null then encode_double(m) else '' end;
$$;

-- STA-7: a stored bound as the encoding of the field's current type: a 4-byte bound of a timestamp field is a
-- date written before the promotion, in days.
create function current_bound(t text, b bytea) returns bytea language sql immutable as $$
  select case
    when length(b) = 4 and base_type(t) in ('timestamp', 'timestamptz') then
      encode_long(decode_int32(b)::bigint * 86400000000)
    when length(b) = 4 and base_type(t) in ('timestamp_ns', 'timestamptz_ns') then
      encode_long(decode_int32(b)::bigint * 86400000000000)
    else b end;
$$;

-- STA-10: the visible files of t at (r, s) that may hold a value of field f in [lo, hi]. `lo` and `hi` are
-- encoded bounds of the field's current type; NULL leaves that end open. Geospatial and variant fields are not
-- pruned here.
create function prune(r int, t uuid, s bigint, f int, lo bytea, hi bytea) returns setof data_file
language sql stable as $$
  with ft as (select type from visible_fields(r, t, s) where field_id = f)
  select v.* from visible_files(r, t, s) v
   where not coalesce((
     select base_type(ft.type) not in ('geometry', 'geography', 'variant')
        and ((lo is not null and current_bound(ft.type, v.upper[i]) < lo)
          or (hi is not null and current_bound(ft.type, v.lower[i]) > hi))
       from ft, array_position(v.stat_field_ids, f) i), false);
$$;

-- Every rule of §8 that a visible file of t breaks at (r, s), one line each, prefixed by the rule id.
create function file_errors(r int, t uuid, s bigint) returns setof text language plpgsql stable as $$
declare fs field[] := array(select f from visible_fields(r, t, s) f);
begin
  return query select format('STA-2: file %s repeats a statistics field', v.file_id)
    from visible_files(r, t, s) v
   where (select count(distinct i) from unnest(v.stat_field_ids) i) < cardinality(v.stat_field_ids);
  return query select format('STA-2: file %s has a NaN count for field %s', v.file_id, x.f)
    from visible_files(r, t, s) v, unnest(v.stat_field_ids, v.nan_count) x(f, n)
   where x.n is not null
     and coalesce((select base_type(y.type) from unnest(fs) y where y.field_id = x.f), '') not in ('float', 'double');
  return query select format('STA-4: file %s has partition %s for spec %s', v.file_id, v.partition, v.spec_id)
    from visible_files(r, t, s) v
   where not exists (select 1 from partition_spec p
                      where p.table_id = t and p.spec_id = v.spec_id
                        and jsonb_array_length(p.fields) = jsonb_array_length(v.partition));
  return query select format('STA-4: file %s has sort order %s', v.file_id, v.sort_order_id)
    from visible_files(r, t, s) v
   where v.sort_order_id <> 0
     and not exists (select 1 from sort_order o where o.table_id = t and o.sort_order_id = v.sort_order_id);
end $$;

-- §9 · commits

-- CMT-2: the snapshot of t at (r, s), or NULL.
create function snapshot_at(r int, t uuid, s bigint) returns bigint language sql stable as $$
  select x.id from chain(r, s) c
    cross join lateral (select max(snapshot_id) as id from snapshot
                         where table_id = t and ref_id = c.ref_id and snapshot_id between c.since and c.seq) x
   where x.id is not null
   order by c.level limit 1;
$$;

-- CMT-10: a writer's intent to upload `path`.
create function register_intent(path text, t uuid, r int, ttl interval) returns void language sql as $$
  insert into intent values (path, t, r, now() + ttl);
$$;

create function conflict(what text) returns void language plpgsql as $$
begin
  raise exception 'metalake conflict: %', what using errcode = '40001';
end $$;

create function invalid(what text) returns void language plpgsql as $$
begin
  raise exception 'metalake invalid: %', what using errcode = '22023';
end $$;

-- The bytea array of a JSON array of hex strings, NULL elements kept.
create function hex_array(j jsonb) returns bytea[] language sql immutable as $$
  select coalesce(array_agg(decode(x, 'hex') order by n), '{}') from jsonb_array_elements_text(j) with ordinality e(x, n);
$$;

create function bigint_array(j jsonb) returns bigint[] language sql immutable as $$
  select coalesce(array_agg(x::bigint order by n), '{}') from jsonb_array_elements_text(j) with ordinality e(x, n);
$$;

-- The reference commit (§9). `c` is one JSON object:
--
--   {"ref": 1, "author": "...", "actor": "...", "operation": "append", "app_id": null, "app_version": null,
--    "definition": null, "message": null,
--    "tables": [{"table_id": "<uuid>", "expected_snapshot": null,
--                "create": {"namespace": ["db"], "name": "t", "location": "gs://..."},
--                "rename": {"namespace": ["db"], "name": "u"}, "drop": true,
--                "add": [{"file_id": null, "path": "...", "spec_id": 0, "partition": [], "sort_order_id": null,
--                         "record_count": 0, "file_size": 0, "footer_size": 0, "first_row_id": null,
--                         "stat_field_ids": [], "lower": ["<hex>"], "upper": ["<hex>"], "null_count": [],
--                         "nan_count": [], "value_count": null}],
--                "remove": [<file_id>],
--                "fields": [{"field_id": 1, "new": true, "parent_field_id": null, "position": 1, "name": "id",
--                            "type": "long", "required": true, "initial_default": null, "write_default": null,
--                            "doc": null}],
--                "drop_fields": [<field_id>],
--                "properties": {"key": "value or null to remove"},
--                "partition_spec": [<partition field>], "sort_order": [<sort field>], "cluster_key": [<field_id>]}]}
--
-- Every key but "ref", "author", "actor", "operation" and "tables" is optional. New field ids are drawn with
-- next_id() beforehand, except in a created table. Returns the commit_seq, or the stored one if an idempotent
-- commit was already applied.
create function commit(c jsonb) returns bigint language plpgsql as $$
declare
  latest constant bigint := 9223372036854775806;
  r int := (c ->> 'ref')::int;
  app text := c ->> 'app_id';
  ver bigint := (c ->> 'app_version')::bigint;
  entries jsonb := coalesce(c -> 'tables', '[]');
  e jsonb;
  x jsonb;
  t uuid;
  stored idempotency;
  rf ref;
  seq bigint;
  cur bigint;
  old field;
  fid bigint;
  n bigint;
  problems text;
begin
  -- CMT-5: idempotency.
  if app is not null then
    if ver is null or ver < 1 then
      perform invalid('app_version must be positive');
    end if;
    insert into idempotency values (r, app, 0, 0) on conflict do nothing;
    select * into stored from idempotency where ref_id = r and app_id = app for update;
    if ver <= stored.version then
      return stored.commit_seq;
    end if;
  end if;

  -- CMT-4: the ref, then the heads of the existing tables, in table_id order.
  if exists (select 1 from jsonb_array_elements(entries) y
              where y ? 'create' or y ? 'rename' or coalesce((y ->> 'drop')::boolean, false)) then
    select * into rf from ref where ref_id = r for update;
  else
    select * into rf from ref where ref_id = r for key share;
  end if;
  if rf.ref_id is null or rf.deleted or rf.kind <> 'branch' then
    perform invalid(format('ref %s does not take commits', r));
  end if;
  for t in select (y ->> 'table_id')::uuid from jsonb_array_elements(entries) y where not y ? 'create' order by 1 loop
    if not exists (select 1 from visible_table_names(r, latest) v where v.table_id = t) then
      perform conflict(format('table %s is not visible on ref %s', t, r));
    end if;
    insert into head values (r, t, snapshot_at(r, t, latest)) on conflict do nothing;
    perform 1 from head h where h.ref_id = r and h.table_id = t for update;
  end loop;

  -- CMT-6 and CMT-7: requirements and conflicts, against the current state under the locks.
  for e in select * from jsonb_array_elements(entries) loop
    t := (e ->> 'table_id')::uuid;
    if e ? 'create' then
      if exists (select 1 from tbl where table_id = t) then
        perform invalid(format('table id %s exists', t));
      end if;
      if table_by_name(r, array(select jsonb_array_elements_text(e -> 'create' -> 'namespace')), e -> 'create' ->> 'name', latest) is not null then
        perform conflict(format('the name %s is taken', e -> 'create'));
      end if;
      continue;
    end if;
    if e ? 'expected_snapshot' and (e ->> 'expected_snapshot')::bigint
         is distinct from (select h.snapshot_id from head h where h.ref_id = r and h.table_id = t) then
      perform conflict(format('table %s is no longer at snapshot %s', t, e ->> 'expected_snapshot'));
    end if;
    if e ? 'rename' and coalesce(table_by_name(r, array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')),
                                               e -> 'rename' ->> 'name', latest), t) <> t then
      perform conflict(format('the name %s is taken', e -> 'rename'));
    end if;
    for fid in select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint loop
      if not exists (select 1 from visible_files(r, t, latest) v where v.file_id = fid) then
        perform conflict(format('file %s is not visible', fid));
      end if;
    end loop;
    for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
      if coalesce((x ->> 'new')::boolean, false) then
        if exists (select 1 from visible_fields(r, t, latest) v
                    where v.parent_field_id is not distinct from (x ->> 'parent_field_id')::int and v.name = x ->> 'name') then
          perform conflict(format('a field named %s exists', x ->> 'name'));
        end if;
      else
        select * into old from visible_fields(r, t, latest) v where v.field_id = (x ->> 'field_id')::int;
        if old.field_id is null then
          perform conflict(format('field %s is not visible', x ->> 'field_id'));
        end if;
        if not field_type_change_allowed(t, old.field_id, old.type, x ->> 'type') then
          perform conflict(format('field %s cannot change from %s to %s', old.field_id, old.type, x ->> 'type'));
        end if;
      end if;
    end loop;
    for fid in select jsonb_array_elements_text(coalesce(e -> 'drop_fields', '[]'))::bigint loop
      if not exists (select 1 from visible_fields(r, t, latest) v where v.field_id = fid) then
        perform conflict(format('field %s is not visible', fid));
      end if;
    end loop;
  end loop;

  -- CMT-3: the commit.
  seq := nextval('commit_seq_gen');
  insert into commit values (seq, r, pg_current_xact_id(), null, c ->> 'author', c ->> 'actor', c ->> 'operation',
                             app, ver, c ->> 'definition', c ->> 'message');

  -- CMT-8: the rows.
  for e in select * from jsonb_array_elements(entries) loop
    t := (e ->> 'table_id')::uuid;
    cur := (select h.snapshot_id from head h where h.ref_id = r and h.table_id = t);

    if e ? 'create' then
      insert into tbl values (t, e -> 'create' ->> 'location', seq);
      insert into table_counter values (t,
        coalesce((select max((y ->> 'field_id')::int) from jsonb_array_elements(e -> 'fields') y), 0),
        0,
        greatest(999, coalesce((select max((y ->> 'field_id')::int) from jsonb_array_elements(e -> 'partition_spec') y), 999)),
        case when e ? 'sort_order' then 1 else 0 end);
      insert into table_name values (t, array(select jsonb_array_elements_text(e -> 'create' -> 'namespace')),
                                     e -> 'create' ->> 'name', false, r, seq);
      insert into partition_spec values (t, 0, coalesce(e -> 'partition_spec', '[]'), r, seq);
      if e ? 'sort_order' then
        insert into sort_order values (t, 1, e -> 'sort_order', r, seq);
      end if;
    end if;

    if e ? 'rename' or coalesce((e ->> 'drop')::boolean, false) then
      update table_name set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into table_name
        select t, coalesce(array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')), v.namespace),
               coalesce(e -> 'rename' ->> 'name', v.name), coalesce((e ->> 'drop')::boolean, false), r, seq
          from visible_table_names(r, seq - 1) v where v.table_id = t;
    end if;

    for x in select * from jsonb_array_elements(coalesce(e -> 'add', '[]')) loop
      insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, sort_order_id,
                             record_count, file_size, footer_size, first_row_id, stat_field_ids, lower, upper,
                             null_count, nan_count, value_count)
      values (t, coalesce((x ->> 'file_id')::bigint, nextval('file_id_gen')), r, seq, x ->> 'path',
              coalesce((x ->> 'spec_id')::int, 0), coalesce(x -> 'partition', '[]'), (x ->> 'sort_order_id')::int,
              (x ->> 'record_count')::bigint, (x ->> 'file_size')::bigint, (x ->> 'footer_size')::int,
              (x ->> 'first_row_id')::bigint,
              array(select y::int from jsonb_array_elements_text(coalesce(x -> 'stat_field_ids', '[]')) y),
              hex_array(coalesce(x -> 'lower', '[]')), hex_array(coalesce(x -> 'upper', '[]')),
              bigint_array(coalesce(x -> 'null_count', '[]')), bigint_array(coalesce(x -> 'nan_count', '[]')),
              case when jsonb_typeof(x -> 'value_count') = 'array' then bigint_array(x -> 'value_count') end);
    end loop;

    for fid in select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint loop
      update data_file set end_seq = seq
       where table_id = t and file_id = fid and ref_id = r and end_seq = 9223372036854775807;
      if not found then
        insert into tombstone values (t, fid, r, seq);
      end if;
    end loop;

    for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
      if coalesce((x ->> 'new')::boolean, false) and not e ? 'create' and (
           (x ->> 'field_id')::int > (select last_field_id from table_counter where table_id = t)
           or exists (select 1 from field f where f.table_id = t and f.field_id = (x ->> 'field_id')::int)) then
        perform invalid(format('field id %s was not drawn for a new field', x ->> 'field_id'));
      end if;
      update field set end_seq = seq
       where table_id = t and field_id = (x ->> 'field_id')::int and ref_id = r and end_seq = 9223372036854775807;
      insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                         write_default, doc, dropped, ref_id, begin_seq)
      values (t, (x ->> 'field_id')::int, (x ->> 'parent_field_id')::int, (x ->> 'position')::int, x ->> 'name',
              x ->> 'type', coalesce((x ->> 'required')::boolean, false), x -> 'initial_default',
              x -> 'write_default', x ->> 'doc', false, r, seq);
    end loop;
    -- JSON null defaults are SQL NULL.
    update field set initial_default = nullif(initial_default, 'null'), write_default = nullif(write_default, 'null')
     where table_id = t and ref_id = r and begin_seq = seq;

    for fid in select jsonb_array_elements_text(coalesce(e -> 'drop_fields', '[]'))::bigint loop
      update field set end_seq = seq
       where table_id = t and field_id = fid and ref_id = r and end_seq = 9223372036854775807;
      insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                         write_default, doc, dropped, ref_id, begin_seq)
        select t, v.field_id, v.parent_field_id, v.position, v.name, v.type, v.required, v.initial_default,
               v.write_default, v.doc, true, r, seq
          from visible_fields(r, t, seq - 1) v where v.field_id = fid;
    end loop;

    for x in select jsonb_build_object('k', k, 'v', v) from jsonb_each(coalesce(e -> 'properties', '{}')) p(k, v) loop
      update table_property set end_seq = seq
       where table_id = t and key = x ->> 'k' and ref_id = r and end_seq = 9223372036854775807;
      insert into table_property values (t, x ->> 'k', x ->> 'v', jsonb_typeof(x -> 'v') = 'null', r, seq);
    end loop;

    if e ? 'partition_spec' and not e ? 'create' then
      update partition_spec set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into partition_spec values (t, next_id(t, 'spec'), e -> 'partition_spec', r, seq);
    end if;
    if e ? 'sort_order' and not e ? 'create' then
      update sort_order set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into sort_order values (t, next_id(t, 'sort_order'), e -> 'sort_order', r, seq);
    end if;
    if e ? 'cluster_key' then
      update cluster_key set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into cluster_key values (t, array(select y::int from jsonb_array_elements_text(e -> 'cluster_key') y), r, seq);
    end if;

    -- CMT-9: the result is valid.
    problems := (select string_agg(p, '; ') from (
      select table_errors(r, t, latest) as p
      union all select file_errors(r, t, latest)
      union all select format('STA-3: file %s has no statistics for cluster key field %s', d.file_id, k)
        from data_file d, visible_cluster_key(r, t, latest) ck, unnest(ck.field_ids) k
       where d.table_id = t and d.ref_id = r and d.begin_seq = seq and not k = any (d.stat_field_ids)) z);
    if problems is not null then
      perform invalid(problems);
    end if;

    -- CMT-1: the snapshot and the head.
    insert into snapshot values (t, seq, r, cur,
      case when e ? 'fields' or e ? 'drop_fields' or e ? 'create' then seq
           else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = cur) end,
      (select spec_id from default_spec(r, t, latest)),
      coalesce((select sort_order_id from default_sort_order(r, t, latest)), 0),
      jsonb_build_object('operation', c ->> 'operation',
        'added-data-files', jsonb_array_length(coalesce(e -> 'add', '[]')),
        'deleted-data-files', jsonb_array_length(coalesce(e -> 'remove', '[]')),
        'added-records', coalesce((select sum((y ->> 'record_count')::bigint) from jsonb_array_elements(e -> 'add') y), 0),
        'deleted-records', coalesce((select sum(d.record_count) from data_file d
                                      where d.table_id = t and d.file_id in (
                                        select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint)
                                        and d.begin_seq < seq and d.end_seq >= seq
                                        and d.ref_id in (select ch.ref_id from chain(r, seq - 1) ch)), 0)));
    insert into head values (r, t, seq) on conflict (ref_id, table_id) do update set snapshot_id = seq;
  end loop;

  -- CMT-10 and CMT-5: intents, idempotency, notification.
  delete from intent where path in (
    select y ->> 'path' from jsonb_array_elements(entries) e2, jsonb_array_elements(coalesce(e2 -> 'add', '[]')) y);
  if app is not null then
    update idempotency set version = ver, commit_seq = seq where ref_id = r and app_id = app;
  end if;
  perform pg_notify('metalake', current_schema() || ':' || seq);
  return seq;
end $$;
