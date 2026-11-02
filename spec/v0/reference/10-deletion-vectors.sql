-- MetaLake v0 · reference behaviour · deletion vectors (§10).
--
-- It defines the right answer, not the fast one.

-- Unsigned integers of `n` bytes at offset `o`, little-endian and big-endian, and the reverse.
create function le_int(b bytea, o int, n int) returns bigint language sql immutable as $$
  select sum(get_byte(b, o + i)::bigint << (8 * i))::bigint from generate_series(0, n - 1) i;
$$;

create function be_int(b bytea, o int, n int) returns bigint language sql immutable as $$
  select sum(get_byte(b, o + i)::bigint << (8 * (n - 1 - i)))::bigint from generate_series(0, n - 1) i;
$$;

create function le_bytes(v bigint, n int) returns bytea language sql immutable as $$
  select string_agg(set_byte('\x00'::bytea, 0, ((v >> (8 * i)) & 255)::int), ''::bytea order by i)
    from generate_series(0, n - 1) i;
$$;

create function be_bytes(v bigint, n int) returns bytea language sql immutable as $$
  select string_agg(set_byte('\x00'::bytea, 0, ((v >> (8 * i)) & 255)::int), ''::bytea order by i desc)
    from generate_series(0, n - 1) i;
$$;

-- CRC-32 (IEEE 802.3, reflected, polynomial 0xEDB88320), as zlib computes it.
create function crc32(b bytea) returns bigint language plpgsql immutable as $$
declare c bigint := 4294967295;
begin
  for i in 0 .. length(b) - 1 loop
    c := c # get_byte(b, i);
    for k in 1 .. 8 loop
      c := case when c & 1 = 1 then (c >> 1) # 3988292384 else c >> 1 end;
    end loop;
  end loop;
  return c # 4294967295;
end $$;

-- DV-2: a 32-bit Roaring bitmap in its standard serialization, starting at byte `start`: its values and its
-- length in bytes. Array, bitset and run containers, with either cookie.
create function roaring32(b bytea, start int, out vals bigint[], out size int) language plpgsql immutable as $$
declare
  cookie bigint := le_int(b, start, 4);
  n int;
  o int;
  runs bytea := '';
  with_offsets boolean;
  keys int[] := '{}';
  cards int[] := '{}';
  got bigint[];
  bits bytea;
  nr int;
begin
  vals := '{}';
  if cookie = 12346 then
    n := le_int(b, start + 4, 4); o := start + 8; with_offsets := true;
  elsif cookie & 65535 = 12347 then
    n := (cookie >> 16) + 1; o := start + 4;
    runs := substring(b from o + 1 for (n + 7) / 8); o := o + (n + 7) / 8;
    with_offsets := n >= 4;
  else
    perform invalid('a deletion vector holds something that is not a Roaring bitmap');
  end if;
  for i in 0 .. n - 1 loop
    keys := keys || le_int(b, o + 4 * i, 2)::int;
    cards := cards || (le_int(b, o + 4 * i + 2, 2) + 1)::int;
  end loop;
  o := o + 4 * n;
  if with_offsets then o := o + 4 * n; end if;
  for i in 1 .. n loop
    if length(runs) > 0 and get_bit(runs, i - 1) = 1 then
      nr := le_int(b, o, 2);
      got := array(select (keys[i]::bigint << 16) + s + g
                     from generate_series(0, nr - 1) j,
                          lateral (select le_int(b, o + 2 + 4 * j, 2) as s, le_int(b, o + 4 + 4 * j, 2) as l) x,
                          generate_series(0, x.l) g);
      o := o + 2 + 4 * nr;
    elsif cards[i] <= 4096 then
      got := array(select (keys[i]::bigint << 16) + le_int(b, o + 2 * j, 2) from generate_series(0, cards[i] - 1) j);
      o := o + 2 * cards[i];
    else
      bits := substring(b from o + 1 for 8192);
      got := array(select (keys[i]::bigint << 16) + j from generate_series(0, 65535) j where get_bit(bits, j) = 1);
      o := o + 8192;
    end if;
    if cardinality(got) <> cards[i] then
      perform invalid('a Roaring container does not hold the values its header counts');
    end if;
    vals := vals || got;
  end loop;
  size := o - start;
end $$;

-- DV-2: the positions of a deletion-vector-v1 blob, after checking its length, magic and checksum.
create function dv_positions(blob bytea) returns setof bigint language plpgsql immutable as $$
declare
  len bigint;
  o int := 16;
  k bigint;
  r record;
begin
  if length(blob) < 20 then
    perform invalid('a deletion vector is too short');
  end if;
  len := be_int(blob, 0, 4);
  if len <> length(blob) - 8 then
    perform invalid('a deletion vector''s length does not match');
  end if;
  if substring(blob from 5 for 4) <> '\xd1d33964'::bytea then
    perform invalid('a deletion vector lacks its magic');
  end if;
  if crc32(substring(blob from 5 for len::int)) <> be_int(blob, 4 + len::int, 4) then
    perform invalid('a deletion vector''s checksum does not match');
  end if;
  for i in 1 .. le_int(blob, 8, 8) loop
    k := le_int(blob, o, 4);
    if k >= 2147483648 then
      perform invalid('a deletion vector holds a position of 2^63 or more');
    end if;
    r := roaring32(blob, o + 4);
    return query select (k << 32) | v from unnest(r.vals) v;
    o := o + 4 + r.size;
  end loop;
  if o <> 4 + len then
    perform invalid('a deletion vector has bytes after its bitmaps');
  end if;
end $$;

-- A 32-bit Roaring bitmap of sorted, distinct values, without run containers.
create function roaring32_encode(vals bigint[]) returns bytea language plpgsql immutable as $$
declare
  keys bigint[] := array(select distinct v >> 16 from unnest(vals) v order by 1);
  n int := cardinality(keys);
  head bytea := le_bytes(12346, 4) || le_bytes(n, 4);
  offsets bytea := '';
  body bytea := '';
  part bytea;
  c bigint[];
begin
  for i in 1 .. n loop
    c := array(select v & 65535 from unnest(vals) v where v >> 16 = keys[i] order by v);
    head := head || le_bytes(keys[i], 2) || le_bytes(cardinality(c) - 1, 2);
    if cardinality(c) <= 4096 then
      part := (select string_agg(le_bytes(v, 2), ''::bytea order by v) from unnest(c) v);
    else
      part := decode(repeat('00', 8192), 'hex');
      for j in 1 .. cardinality(c) loop
        part := set_bit(part, c[j]::int, 1);
      end loop;
    end if;
    offsets := offsets || le_bytes(8 + 8 * n + length(body), 4);
    body := body || part;
  end loop;
  return head || offsets || body;
end $$;

-- DV-2: the deletion-vector-v1 blob of a set of positions.
create function dv_encode(positions bigint[]) returns bytea language plpgsql immutable as $$
declare
  p bigint[] := array(select distinct x from unnest(positions) x order by 1);
  vector bytea;
  body bytea;
  k bigint;
begin
  if exists (select 1 from unnest(p) x where x < 0) then
    perform invalid('a negative position');
  end if;
  vector := le_bytes((select count(distinct x >> 32) from unnest(p) x), 8);
  for k in select distinct x >> 32 from unnest(p) x order by 1 loop
    vector := vector || le_bytes(k, 4)
                     || roaring32_encode(array(select x & 4294967295 from unnest(p) x where x >> 32 = k order by 1));
  end loop;
  body := '\xd1d33964'::bytea || vector;
  return be_bytes(length(body), 4) || body || be_bytes(crc32(body), 4);
end $$;

-- DV-1: the vectors that apply to the visible files of t at (r, s).
create function visible_dvs(r int, t uuid, s bigint) returns setof deletion_vector language sql stable as $$
  select (v).* from (
    select distinct on (x.file_id) x as v
      from chain(r, s) c join deletion_vector x
        on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.file_id, c.level) y
   where (v).file_id in (select file_id from visible_files(r, t, s));
$$;

-- DV-1 for one file: the vector of file `fid` of t at (r, s), if the file is visible and has one.
create function file_dv(r int, t uuid, s bigint, fid bigint) returns deletion_vector language sql stable as $$
  select x.*
    from chain(r, s) c join deletion_vector x
      on x.table_id = t and x.file_id = fid and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   where file_visible(r, t, s, fid)
   order by c.level limit 1;
$$;

-- DV-5: the live rows of each visible file of t at (r, s).
create function live_rows(r int, t uuid, s bigint) returns table (file_id bigint, rows bigint) language sql stable as $$
  select f.file_id, f.record_count - coalesce(d.cardinality, 0)
    from visible_files(r, t, s) f left join visible_dvs(r, t, s) d on d.file_id = f.file_id;
$$;

-- Every rule of §10 that a vector of t breaks at (r, s), one line each; with `added`, only the vectors ref r set
-- at commit `added` (CMT-9).
create function dv_errors(r int, t uuid, s bigint, added bigint default null) returns setof text
language plpgsql stable as $$
declare
  d deletion_vector;
  base deletion_vector;
  f data_file;
  p bigint[];
begin
  for d in select * from visible_dvs(r, t, s) x where added is null
           union all
           select * from deletion_vector x
            where added is not null and x.table_id = t and x.ref_id = r and x.begin_seq = added
              and (file_dv(r, t, s, x.file_id)).begin_seq = added loop
    p := null;
    f := visible_file(r, t, s, d.file_id);
    if d.inline is not null then
      p := array(select dv_positions(d.inline));
      if cardinality(p) <> d.cardinality then
        return next format('DV-2: the vector of file %s holds %s positions, not %s', d.file_id, cardinality(p), d.cardinality);
      end if;
      if exists (select 1 from unnest(p) x where x >= f.record_count) then
        return next format('DV-2: the vector of file %s holds a position beyond its %s rows', d.file_id, f.record_count);
      end if;
    end if;
    if d.cardinality > f.record_count then
      return next format('DV-2: the vector of file %s deletes more rows than it has', d.file_id);
    end if;
    base := file_dv(r, t, d.begin_seq - 1, d.file_id);
    if base.file_id is not null and d.cardinality < base.cardinality then
      return next format('DV-4: the vector of file %s drops positions of the one it replaces', d.file_id);
    elsif base.inline is not null and d.inline is not null
          and exists (select dv_positions(base.inline) except select unnest(p)) then
      return next format('DV-4: the vector of file %s drops positions of the one it replaces', d.file_id);
    end if;
  end loop;
end $$;
