-- MetaLake v0 · reference behaviour · the Iceberg projection (§16).
--
-- It defines the right answer, not the fast one. The Iceberg harness writes the Avro files and serves the REST
-- protocol; everything it serves is computed here.

create function reverse_bytes(b bytea) returns bytea language sql immutable as $$
  select coalesce(string_agg(substring(b from i for 1), ''::bytea order by i desc), ''::bytea)
    from generate_series(1, length(b)) i;
$$;

-- A signed little-endian integer of 4 or 8 bytes.
create function le_signed(b bytea) returns bigint language sql immutable as $$
  select case length(b)
    when 4 then ('x' || encode(reverse_bytes(b), 'hex'))::bit(32)::int::bigint
    else ('x' || encode(reverse_bytes(b), 'hex'))::bit(64)::bigint end;
$$;

-- The float whose IEEE 754 single-precision bits are `bits`, as a double (exact).
create function single_to_double(bits bigint) returns float8 language plpgsql immutable as $$
declare
  e int := (bits >> 23) & 255;
  m bigint := bits & 8388607;
  v float8;
begin
  if e = 255 then
    v := case when m = 0 then 'Infinity'::float8 else 'NaN'::float8 end;
  elsif e = 0 then
    v := m::float8 * power(2::float8, -149);
  else
    v := (m + 8388608)::float8 * power(2::float8, e - 150);
  end if;
  return case when (bits >> 31) & 1 = 1 then -v else v end;
end $$;

-- ICE-3: a MetaLake bound in Iceberg's single-value serialization of the field's current type.
create function iceberg_bound(t text, b bytea) returns bytea language plpgsql immutable as $$
declare
  c text := canonical_type(t);
  x bytea := current_bound(c, b);
  v text;
begin
  if x is null then return null; end if;
  v := decode_bound(c, x);
  case base_type(c)
    when 'boolean' then return case when v::boolean then '\x01'::bytea else '\x00'::bytea end;
    when 'int' then return le_bytes(decode_long(x), 4);
    when 'date' then return le_bytes(decode_int32(x), 4);
    when 'long', 'time', 'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns' then return le_bytes(decode_long(x), 8);
    when 'float' then return reverse_bytes(float4send(v::float4));
    when 'double' then return reverse_bytes(float8send(v::float8));
    when 'decimal' then return min_be_bytes(v::numeric * 10::numeric ^ (regexp_match(c, ',([0-9]+)\)'))[1]::int);
    when 'geometry', 'geography' then
      return (select string_agg(reverse_bytes(float8send(decode_bound('double', substring(x from 8 * i + 1 for 8))::float8)),
                                ''::bytea order by i)
                from generate_series(0, length(x) / 8 - 1) i);
    when 'string', 'uuid', 'fixed', 'binary' then return x;
    else return null;
  end case;
end $$;

-- ICE-6: an Iceberg single-value bound of type t, in MetaLake's encoding (STA-5).
create function iceberg_bound_in(t text, b bytea) returns bytea language plpgsql immutable as $$
declare c text := canonical_type(t);
begin
  if b is null then return null; end if;
  case base_type(c)
    when 'boolean' then return case when get_byte(b, 0) <> 0 then '\x01'::bytea else '\x00'::bytea end;
    when 'int', 'long', 'time', 'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns' then
      return encode_long(le_signed(b));
    when 'date' then return encode_int32(le_signed(b)::int);
    when 'float' then return encode_double(single_to_double(le_signed(b) & 4294967295));
    when 'double' then return encode_double(bits_to_double(le_signed(b)));
    when 'decimal' then
      return encode_decimal(
        ((select sum(get_byte(b, i)::numeric * 256::numeric ^ (length(b) - 1 - i)) from generate_series(0, length(b) - 1) i)
         - case when get_byte(b, 0) >= 128 then 256::numeric ^ length(b) else 0 end)
        * 10::numeric ^ (-(regexp_match(c, ',([0-9]+)\)'))[1]::int),
        (regexp_match(c, ',([0-9]+)\)'))[1]::int);
    else return b;
  end case;
end $$;

-- ICE-5: where the projection materializes an inline file.
create function inline_path(t uuid, file_id bigint) returns text language sql stable as $$
  select location || '/metalake-inline/' || file_id || '.parquet' from tbl where table_id = t;
$$;

-- The path a served file has: its own, or its materialized one (ICE-5).
create function served_path(t uuid, f data_file) returns text language sql stable as $$
  select coalesce(f.path, inline_path(t, f.file_id));
$$;

-- ICE-2 and ICE-3: the files of snapshot s of t, one JSON object each, as the harness writes them to manifests.
create function iceberg_files(t uuid, s bigint) returns setof jsonb language sql stable as $$
  with snap as (select * from snapshot where table_id = t and snapshot_id = s),
  types as (select field_id, type from visible_fields((select ref_id from snap), t, s))
  select jsonb_build_object(
      'path', served_path(t, f), 'inline', case when f.path is null then encode(i.content, 'hex') end,
      'status', case when f.ref_id = snap.ref_id and f.begin_seq = s then 'ADDED' else 'EXISTING' end,
      'data_sequence_number', f.begin_seq, 'spec_id', f.spec_id, 'partition', f.partition,
      'record_count', f.record_count, 'file_size', f.file_size, 'sort_order_id', f.sort_order_id,
      'lower', (select jsonb_object_agg(x.id, encode(iceberg_bound(ty.type, x.lo), 'hex'))
                  from unnest(f.stat_field_ids, f.lower) x(id, lo) join types ty on ty.field_id = x.id where x.lo is not null),
      'upper', (select jsonb_object_agg(x.id, encode(iceberg_bound(ty.type, x.hi), 'hex'))
                  from unnest(f.stat_field_ids, f.upper) x(id, hi) join types ty on ty.field_id = x.id where x.hi is not null),
      'null_count', (select jsonb_object_agg(x.id, x.n) from unnest(f.stat_field_ids, f.null_count) x(id, n) where x.n is not null),
      'nan_count', (select jsonb_object_agg(x.id, x.n) from unnest(f.stat_field_ids, f.nan_count) x(id, n) where x.n is not null))
    from snap, visible_files(snap.ref_id, t, s) f left join inline_file i on i.table_id = t and i.file_id = f.file_id;
$$;

-- ICE-4: the deletion vectors of snapshot s of t, each as the position delete file the projection materializes:
-- its path, where to read the vector (inline, or a Puffin file's range), and its manifest fields.
create function iceberg_dvs(t uuid, s bigint) returns setof jsonb language sql stable as $$
  with snap as (select * from snapshot where table_id = t and snapshot_id = s)
  select jsonb_build_object(
      'path', (select location from tbl where table_id = t) || '/metalake-deletes/' || d.file_id || '-' || d.begin_seq || '.parquet',
      'inline', encode(d.inline, 'hex'), 'puffin', d.path,
      'content_offset', d.content_offset, 'content_size', d.content_size, 'cardinality', d.cardinality,
      'referenced_data_file', served_path(t, f), 'data_sequence_number', d.begin_seq,
      'status', case when d.ref_id = snap.ref_id and d.begin_seq = s then 'ADDED' else 'EXISTING' end,
      'spec_id', f.spec_id, 'partition', f.partition)
    from snap, visible_dvs(snap.ref_id, t, s) d
    join visible_files((select ref_id from snapshot where table_id = t and snapshot_id = s), t, s) f on f.file_id = d.file_id;
$$;

-- ICE-1: the table metadata of t, without the manifest-list paths, which the harness adds per snapshot.
create function iceberg_metadata(t uuid) returns jsonb language sql stable as $$
  with latest (s) as (select 9223372036854775806::bigint),
  snaps as (select * from snapshot where table_id = t),
  main_snap as (select snapshot_at(1, t, (select s from latest)) as id),
  refs as (
    select r.name, r.kind, snapshot_at(r.ref_id, t, (select s from latest)) as id
      from ref r
     where not r.deleted and exists (select 1 from visible_table_names(r.ref_id, (select s from latest)) v where v.table_id = t)),
  schemas as (
    select distinct on (x.schema_seq) x.schema_seq, schema_json(x.ref_id, t, x.snapshot_id) as j
      from snaps x order by x.schema_seq, x.snapshot_id)
  select jsonb_build_object(
    'format-version', 2,
    'table-uuid', t,
    'location', (select location from tbl where table_id = t),
    'last-sequence-number', coalesce((select max(snapshot_id) from snaps), 0),
    'last-updated-ms', coalesce((select (extract(epoch from max(committed_time(snapshot_id))) * 1000)::bigint from snaps), 0),
    'last-column-id', (select last_field_id from table_counter where table_id = t),
    'schemas', (select jsonb_agg(j || jsonb_build_object('schema-id', schema_seq) order by schema_seq) from schemas),
    'current-schema-id', (select schema_seq from snaps where snapshot_id = (select id from main_snap)),
    'partition-specs', (select jsonb_agg(jsonb_build_object('spec-id', spec_id, 'fields',
                          (select coalesce(jsonb_agg(jsonb_build_object('source-id', (pf -> 'source_ids' ->> 0)::int,
                                     'field-id', (pf ->> 'field_id')::int, 'name', pf ->> 'name', 'transform', pf ->> 'transform')), '[]')
                             from jsonb_array_elements(fields) pf)) order by spec_id)
                          from (select distinct on (spec_id) spec_id, fields from partition_spec where table_id = t) x),
    'default-spec-id', (select spec_id from default_spec(1, t, (select s from latest))),
    'last-partition-id', (select last_partition_field_id from table_counter where table_id = t),
    'sort-orders', (select jsonb_agg(o order by (o ->> 'order-id')::int) from (
                      select jsonb_build_object('order-id', 0, 'fields', '[]'::jsonb) as o
                      union all
                      select distinct on (sort_order_id) jsonb_build_object('order-id', sort_order_id, 'fields',
                        (select jsonb_agg(jsonb_build_object('transform', sf ->> 'transform', 'source-id', (sf -> 'source_ids' ->> 0)::int,
                                  'direction', sf ->> 'direction', 'null-order', sf ->> 'null_order')) from jsonb_array_elements(fields) sf))
                        from sort_order where table_id = t) z),
    'default-sort-order-id', coalesce((select sort_order_id from default_sort_order(1, t, (select s from latest))), 0),
    'properties', coalesce((select jsonb_object_agg(key, value) from visible_properties(1, t, (select s from latest))), '{}'),
    'current-snapshot-id', (select id from main_snap),
    'snapshots', coalesce((select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'snapshot-id', x.snapshot_id, 'parent-snapshot-id', x.parent_snapshot_id, 'sequence-number', x.snapshot_id,
        'timestamp-ms', (extract(epoch from committed_time(x.snapshot_id)) * 1000)::bigint,
        'schema-id', x.schema_seq,
        'summary', jsonb_build_object('operation', case x.summary ->> 'operation'
                     when 'merge' then 'overwrite' when 'rebase' then 'overwrite' when 'replace' then 'replace' when 'delete' then 'delete'
                     when 'overwrite' then 'overwrite' else 'append' end)
                   || (select coalesce(jsonb_object_agg(k, v #>> '{}'), '{}') from jsonb_each(x.summary - 'operation') e(k, v))))
        order by x.snapshot_id) from snaps x), '[]'),
    'refs', coalesce((select jsonb_object_agg(name, jsonb_build_object('snapshot-id', id, 'type', kind)) from refs where id is not null), '{}'),
    'snapshot-log', '[]'::jsonb, 'metadata-log', '[]'::jsonb);
$$;

-- The keys of a file's statistics maps, in field id order.
create function stat_keys(a jsonb) returns setof text language sql immutable as $$
  select k from (select jsonb_object_keys(coalesce(a -> 'lower', '{}')) union select jsonb_object_keys(coalesce(a -> 'null_count', '{}'))) x(k)
   order by k::int;
$$;

-- ICE-6: an Iceberg commit of new data files (`added`, objects with "path", "record_count", "file_size",
-- "footer_size", "partition", and "lower", "upper", "null_count", "nan_count" maps by field id with bounds in
-- Iceberg's hex serialization) and removed paths, on ref `ref_name`. Returns MetaLake's snapshot id.
create function iceberg_commit(t uuid, ref_name text, expected bigint, operation text, added jsonb, removed text[])
returns bigint language plpgsql as $$
declare
  r int := (select ref_id from ref where name = ref_name);
  latest constant bigint := 9223372036854775806;
  files jsonb := '[]';
  a jsonb;
  entry jsonb;
begin
  if r is null then
    perform invalid(format('no ref named %s', ref_name));
  end if;
  for a in select * from jsonb_array_elements(added) loop
    files := files || jsonb_build_object(
      'path', a ->> 'path', 'record_count', (a ->> 'record_count')::bigint, 'file_size', (a ->> 'file_size')::bigint,
      'footer_size', (a ->> 'footer_size')::int, 'partition', coalesce(a -> 'partition', '[]'),
      'spec_id', coalesce((a ->> 'spec_id')::int, 0),
      'stat_field_ids', (select coalesce(jsonb_agg(k::int), '[]') from stat_keys(a) k),
      'lower', (select coalesce(jsonb_agg(encode(iceberg_bound_in(ty.type, decode(a -> 'lower' ->> k, 'hex')), 'hex')), '[]')
                  from stat_keys(a) k left join visible_fields(r, t, latest) ty on ty.field_id = k::int),
      'upper', (select coalesce(jsonb_agg(encode(iceberg_bound_in(ty.type, decode(a -> 'upper' ->> k, 'hex')), 'hex')), '[]')
                  from stat_keys(a) k left join visible_fields(r, t, latest) ty on ty.field_id = k::int),
      'null_count', (select coalesce(jsonb_agg(a -> 'null_count' -> k), '[]') from stat_keys(a) k),
      'nan_count', (select coalesce(jsonb_agg(a -> 'nan_count' -> k), '[]') from stat_keys(a) k));
  end loop;
  entry := jsonb_build_object('table_id', t, 'add', files,
    'remove', (select coalesce(jsonb_agg(v.file_id), '[]') from visible_files(r, t, latest) v where served_path(t, v) = any (removed)));
  if expected is not null then
    entry := entry || jsonb_build_object('expected_snapshot', expected);
  end if;
  return commit(jsonb_build_object('ref', r, 'author', 'iceberg', 'actor', 'iceberg-rest', 'operation', operation,
                                   'tables', jsonb_build_array(entry)));
end $$;

-- ICE-7 (revision 0.1): the namespaces under `parent` (an empty array for the top level) on ref r, as JSON arrays.
create function iceberg_namespaces(r int, parent text[]) returns setof jsonb language sql stable as $$
  select to_jsonb(x.namespace) from visible_namespaces(r, 9223372036854775806) x
   where cardinality(x.namespace) = cardinality(parent) + 1
     and (cardinality(parent) = 0 or x.namespace[1:cardinality(parent)] = parent)
   order by x.namespace;
$$;

-- ICE-7: a namespace and its properties, as loadNamespaceMetadata answers, or NULL if it does not exist.
create function iceberg_namespace(r int, ns text[]) returns jsonb language sql stable as $$
  select jsonb_build_object('namespace', to_jsonb(ns), 'properties', coalesce(
           (select jsonb_object_agg(p.key, p.value) from visible_namespace_properties(r, ns, 9223372036854775806) p),
           '{}'))
   where namespace_exists(r, ns, 9223372036854775806);
$$;
