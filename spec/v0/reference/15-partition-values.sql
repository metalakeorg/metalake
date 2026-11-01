-- MetaLake v0 · reference behaviour · partition values (§15).
--
-- It defines the right answer, not the fast one.

-- 32-bit unsigned arithmetic on bigint: a product modulo 2^32, without overflowing 64 bits, and a rotation.
create function mul32(a bigint, c bigint) returns bigint language sql immutable as $$
  select ((a * (c & 65535)) + (((a * (c >> 16)) & 65535) << 16)) & 4294967295;
$$;

create function rotl32(x bigint, r int) returns bigint language sql immutable as $$
  select ((x << r) | (x >> (32 - r))) & 4294967295;
$$;

-- PAR-1: the 32-bit Murmur3 hash, x86 variant, seed 0, as a signed integer.
create function murmur3_32(b bytea) returns int language plpgsql immutable as $$
declare
  c1 constant bigint := 3432918353;
  c2 constant bigint := 461845907;
  n int := length(b);
  h bigint := 0;
  k bigint;
  i int := 0;
begin
  while i + 4 <= n loop
    k := get_byte(b, i) | (get_byte(b, i + 1) << 8) | (get_byte(b, i + 2) << 16) | (get_byte(b, i + 3)::bigint << 24);
    k := mul32(rotl32(mul32(k, c1), 15), c2);
    h := rotl32(h # k, 13);
    h := (mul32(h, 5) + 3864292196) & 4294967295;
    i := i + 4;
  end loop;
  k := 0;
  if n - i = 3 then k := k # (get_byte(b, i + 2)::bigint << 16); end if;
  if n - i >= 2 then k := k # (get_byte(b, i + 1)::bigint << 8); end if;
  if n - i >= 1 then
    k := k # get_byte(b, i);
    h := h # mul32(rotl32(mul32(k, c1), 15), c2);
  end if;
  h := h # n;
  h := h # (h >> 16);
  h := mul32(h, 2246822507);
  h := h # (h >> 13);
  h := mul32(h, 3266489909);
  h := h # (h >> 16);
  return case when h >= 2147483648 then h - 4294967296 else h end;
end $$;

-- An integer in the fewest bytes of two's complement, big-endian.
create function min_be_bytes(u numeric) returns bytea language plpgsql immutable as $$
declare
  k int := 1;
  x numeric;
  b bytea := '';
begin
  while not (u >= -(2::numeric ^ (8 * k - 1)) and u < 2::numeric ^ (8 * k - 1)) loop
    k := k + 1;
  end loop;
  x := case when u < 0 then u + 256::numeric ^ k else u end;
  for i in 1 .. k loop
    b := set_byte('\x00'::bytea, 0, mod(x, 256)::int) || b;
    x := div(x, 256);
  end loop;
  return b;
end $$;

-- PAR-1: the hash of a value of type t, given as encode_bound() takes it.
create function hash_value(t text, v text) returns int language plpgsql immutable as $$
declare c text := canonical_type(t);
begin
  return murmur3_32(case base_type(c)
    when 'int' then le_bytes(v::bigint, 8)
    when 'long' then le_bytes(v::bigint, 8)
    when 'date' then le_bytes(v::date - date '1970-01-01', 8)
    when 'time' then le_bytes(epoch_units(v, 'time', 6)::bigint, 8)
    when 'timestamp' then le_bytes(epoch_units(v, 'timestamp', 6)::bigint, 8)
    when 'timestamptz' then le_bytes(epoch_units(v, 'timestamptz', 6)::bigint, 8)
    when 'timestamp_ns' then le_bytes(floor(epoch_units(v, 'timestamp', 9) / 1000)::bigint, 8)
    when 'timestamptz_ns' then le_bytes(floor(epoch_units(v, 'timestamptz', 9) / 1000)::bigint, 8)
    when 'decimal' then min_be_bytes(v::numeric * 10::numeric ^ (regexp_match(c, ',([0-9]+)\)'))[1]::int)
    when 'string' then convert_to(v, 'UTF8')
    when 'uuid' then decode(replace(v::uuid::text, '-', ''), 'hex')
    when 'fixed' then decode(v, 'hex')
    when 'binary' then decode(v, 'hex')
  end);
end $$;

-- The double whose IEEE 754 bits are `bits`.
create function bits_to_double(bits bigint) returns float8 language plpgsql immutable as $$
declare
  e int := (bits >> 52) & 2047;
  m bigint := bits & 4503599627370495;
  v float8;
begin
  if e = 2047 then
    v := case when m = 0 then 'Infinity'::float8 else 'NaN'::float8 end;
  elsif e = 0 then
    v := m::float8 * power(2::float8, -1074);
  else
    v := (m + 4503599627370496)::float8 * power(2::float8, e - 1075);
  end if;
  return case when bits < 0 then -v else v end;
end $$;

-- Microseconds from the epoch as an ISO 8601 timestamp, exactly: whole days, then the rest of the day.
create function micros_text(us bigint) returns text language sql immutable as $$
  select to_char((date '1970-01-01' + floor(us / 86400000000.0)::int)::timestamp
                 + (us - floor(us / 86400000000.0)::bigint * 86400000000) * interval '1 microsecond',
                 'YYYY-MM-DD"T"HH24:MI:SS.US');
$$;

-- The value an encoded bound holds, as encode_bound() takes it (STA-5 in reverse).
create function decode_bound(t text, b bytea) returns text language plpgsql immutable as $$
declare
  c text := canonical_type(t);
  s bigint;
  x numeric;
begin
  if b is null then return null; end if;
  case base_type(c)
    when 'boolean' then return (get_byte(b, 0) = 1)::text;
    when 'int', 'long' then return decode_long(b)::text;
    when 'date' then return (date '1970-01-01' + decode_int32(b))::text;
    when 'time' then return to_char(time '00:00' + decode_long(b) * interval '1 microsecond', 'HH24:MI:SS.US');
    when 'timestamp' then return micros_text(decode_long(b));
    when 'timestamptz' then return micros_text(decode_long(b)) || '+00:00';
    when 'timestamp_ns', 'timestamptz_ns' then
      s := decode_long(b);
      return left(micros_text(floor(s / 1000000000.0)::bigint * 1000000), 19)
        || '.' || lpad((s - floor(s / 1000000000.0)::bigint * 1000000000)::text, 9, '0')
        || case when base_type(c) = 'timestamptz_ns' then '+00:00' else '' end;
    when 'float', 'double' then
      s := ('x' || encode(b, 'hex'))::bit(64)::bigint;
      return bits_to_double(case when s < 0 then s # (-9223372036854775808)::bigint else ~s end)::text;
    when 'decimal' then
      x := 0;
      for i in 0 .. 15 loop x := x * 256 + get_byte(b, i); end loop;
      x := x - 2::numeric ^ 127;
      -- Multiplying by 10^-S is exact; dividing by 10^S would round to numeric's division scale.
      return round(x * 10::numeric ^ (-(regexp_match(c, ',([0-9]+)\)'))[1]::int),
                   (regexp_match(c, ',([0-9]+)\)'))[1]::int)::text;
    when 'string' then return convert_from(b, 'UTF8');
    when 'uuid' then return encode(b, 'hex')::uuid::text;
    else return encode(b, 'hex');
  end case;
end $$;

-- PAR-1: the partition value of a value `v` of type t under transform `tr`, as JSON.
create function transform_value(t text, tr text, v text) returns jsonb language plpgsql immutable as $$
declare
  c text := canonical_type(t);
  b text := base_type(canonical_type(t));
  w numeric := (regexp_match(tr, '^(?:bucket|truncate)\[([0-9]+)\]$'))[1]::numeric;
  scale int := (regexp_match(c, ',([0-9]+)\)'))[1]::int;
  u numeric;
  ts timestamp;
begin
  if v is null or tr = 'void' then
    return 'null';
  end if;
  if tr = 'identity' then
    return case when b in ('int', 'long') then to_jsonb(v::bigint) when b = 'boolean' then to_jsonb(v::boolean)
                when b in ('float', 'double') then to_jsonb(v::float8) else to_jsonb(v) end;
  elsif tr like 'bucket[%' then
    return to_jsonb((hash_value(c, v)::bigint & 2147483647) % w);
  elsif tr like 'truncate[%' then
    case b
      when 'int', 'long' then return to_jsonb(v::numeric - mod(mod(v::numeric, w) + w, w));
      when 'decimal' then
        u := v::numeric * 10::numeric ^ scale;
        return to_jsonb(round((u - mod(mod(u, w) + w, w)) * 10::numeric ^ (-scale), scale)::text);
      when 'string' then return to_jsonb(left(v, w::int));
      when 'binary' then return to_jsonb(encode(substring(decode(v, 'hex') from 1 for w::int), 'hex'));
      else perform invalid(format('truncate does not apply to %s', c));
    end case;
  end if;
  -- year, month, day, hour: whole units from 1970-01-01T00:00 UTC, rounding down.
  ts := case b
    when 'date' then v::date::timestamp
    when 'timestamp' then regexp_replace(v, '\.[0-9]+', '')::timestamp
    when 'timestamp_ns' then regexp_replace(v, '\.[0-9]+', '')::timestamp
    when 'timestamptz' then regexp_replace(v, '\.[0-9]+', '')::timestamptz at time zone 'UTC'
    when 'timestamptz_ns' then regexp_replace(v, '\.[0-9]+', '')::timestamptz at time zone 'UTC'
  end;
  return to_jsonb(case tr
    when 'year' then extract(year from ts) - 1970
    when 'month' then (extract(year from ts) - 1970) * 12 + extract(month from ts) - 1
    when 'day' then floor(extract(epoch from ts) / 86400)
    when 'hour' then floor(extract(epoch from ts) / 3600)
  end::bigint);
end $$;

create function sign_bytes(a bytea, b bytea) returns int language sql immutable as $$
  select case when a < b then -1 when a > b then 1 else 0 end;
$$;

-- Compares two partition values of transform `tr` over type t: -1, 0 or 1.
create function compare_partition_values(t text, tr text, a jsonb, b jsonb) returns int language sql immutable as $$
  select case
    when tr in ('identity') or tr like 'truncate[%' then
      sign_bytes(encode_bound(t, a #>> '{}'), encode_bound(t, b #>> '{}'))
    else sign((a #>> '{}')::numeric - (b #>> '{}')::numeric)::int end;
$$;

-- PAR-3: every contradiction between the partition values of a checked file of t and its statistics; with
-- `added`, only the files ref r added at commit `added` (CMT-9).
create function partition_errors(r int, t uuid, s bigint, added bigint default null) returns setof text
language plpgsql stable as $$
declare
  f data_file;
  pf jsonb;
  pos int;
  src int;
  typ text;
  tr text;
  pv jsonb;
  idx int;
  lo bytea;
  hi bytea;
  tl jsonb;
  th jsonb;
begin
  for f in select * from checked_files(r, t, s, added) loop
    pos := 0;
    for pf in select jsonb_array_elements((select p.fields from partition_spec p
                                            where p.table_id = t and p.spec_id = f.spec_id limit 1)) loop
      pv := f.partition -> pos;
      pos := pos + 1;
      tr := pf ->> 'transform';
      src := (pf -> 'source_ids' ->> 0)::int;
      typ := (select x.type from visible_fields(r, t, s) x where x.field_id = src);
      idx := array_position(f.stat_field_ids, src);
      continue when idx is null or typ is null or tr = 'void';
      lo := current_bound(typ, f.lower[idx]);
      hi := current_bound(typ, f.upper[idx]);
      if pv is null or pv = 'null' then
        if lo is not null or hi is not null then
          return next format('PAR-3: file %s has partition value null for %s but holds values', f.file_id, pf ->> 'name');
        end if;
        continue;
      end if;
      if f.null_count[idx] > 0 then
        return next format('PAR-3: file %s holds nulls in partition %s = %s', f.file_id, pf ->> 'name', pv);
      end if;
      continue when lo is null or hi is null;
      tl := transform_value(typ, tr, decode_bound(typ, lo));
      th := transform_value(typ, tr, decode_bound(typ, hi));
      if tr like 'bucket[%' then
        if lo = hi and base_type(typ) not in ('string', 'binary')
           and compare_partition_values(typ, tr, tl, pv) <> 0 then
          return next format('PAR-3: file %s has bucket %s, but its value falls in bucket %s', f.file_id, pv, tl);
        end if;
      elsif base_type(typ) in ('string', 'binary') then
        if compare_partition_values(typ, tr, tl, pv) > 0 or compare_partition_values(typ, tr, pv, th) > 0 then
          return next format('PAR-3: file %s has partition %s = %s outside its bounds', f.file_id, pf ->> 'name', pv);
        end if;
      elsif compare_partition_values(typ, tr, tl, pv) <> 0 or compare_partition_values(typ, tr, th, pv) <> 0 then
        return next format('PAR-3: file %s has partition %s = %s, but its bounds give %s and %s', f.file_id, pf ->> 'name', pv, tl, th);
      end if;
    end loop;
  end loop;
end $$;
