-- MetaLake v0 · reference behaviour · statistics (§8).
--
-- It defines the right answer, not the fast one.

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

-- The files file_errors checks: every visible file of t at (r, s), or with `added`, only the files ref r added
-- at commit `added` (CMT-9).
create function checked_files(r int, t uuid, s bigint, added bigint) returns setof data_file language sql stable as $$
  select * from visible_files(r, t, s) where added is null
  union all
  select * from data_file where added is not null and table_id = t and ref_id = r and begin_seq = added;
$$;

-- Every rule of §8 that a checked file of t breaks at (r, s), one line each, prefixed by the rule id.
create function file_errors(r int, t uuid, s bigint, added bigint default null) returns setof text
language plpgsql stable as $$
declare fs field[] := array(select f from visible_fields(r, t, s) f);
begin
  return query select format('STA-2: file %s repeats a statistics field', v.file_id)
    from checked_files(r, t, s, added) v
   where (select count(distinct i) from unnest(v.stat_field_ids) i) < cardinality(v.stat_field_ids);
  return query select format('STA-2: file %s has a NaN count for field %s', v.file_id, x.f)
    from checked_files(r, t, s, added) v, unnest(v.stat_field_ids, v.nan_count) x(f, n)
   where x.n is not null
     and coalesce((select base_type(y.type) from unnest(fs) y where y.field_id = x.f), '') not in ('float', 'double');
  return query select format('STA-4: file %s has partition %s for spec %s', v.file_id, v.partition, v.spec_id)
    from checked_files(r, t, s, added) v
   where not exists (select 1 from partition_spec p
                      where p.table_id = t and p.spec_id = v.spec_id
                        and jsonb_array_length(p.fields) = jsonb_array_length(v.partition));
  return query select format('STA-4: file %s has sort order %s', v.file_id, v.sort_order_id)
    from checked_files(r, t, s, added) v
   where v.sort_order_id <> 0
     and not exists (select 1 from sort_order o where o.table_id = t and o.sort_order_id = v.sort_order_id);
end $$;
