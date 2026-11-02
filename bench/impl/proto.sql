-- Preliminary study of the implementation (ADR 0003 §8): a prototype of the protocol's append, in three call
-- shapes, over the reference DDL of spec/v0. Each takes the files of one commit to table t on ref r and does what
-- the format requires: the locks of CMT-4, the rows of CMT-3, CMT-8 and CMT-1. Bounds are of field 1, the
-- cluster key. Throwaway: the protocol proper is designed after this study.

-- 8.1 · parallel arrays, one element per file.
create function p_append(t uuid, r int, paths text[], record_counts bigint[], sizes bigint[], footers int[],
                         lowers bytea[], uppers bytea[]) returns bigint language plpgsql as $$
declare s bigint;
begin
  perform 1 from ref where ref_id = r for key share;
  perform 1 from head where ref_id = r and table_id = t for update;
  s := nextval('commit_seq_gen');
  insert into commit values (s, r, pg_current_xact_id(), null, 'bench', 'bench', 'append', null, null, null, null);
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                         footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  select t, nextval('file_id_gen'), r, s, x.path, 0, '[]', x.rc, x.size, x.footer, '{1}', array[x.lo], array[x.hi],
         '{0}', '{NULL}'
    from unnest(paths, record_counts, sizes, footers, lowers, uppers) x(path, rc, size, footer, lo, hi);
  insert into snapshot
  select h.table_id, s, r, h.snapshot_id, p.schema_seq, p.spec_id, p.sort_order_id,
         jsonb_build_object('operation', 'append', 'added-data-files', cardinality(paths), 'deleted-data-files', 0,
                            'added-records', (select sum(c) from unnest(record_counts) c), 'deleted-records', 0)
    from head h join snapshot p on p.table_id = h.table_id and p.snapshot_id = h.snapshot_id
   where h.ref_id = r and h.table_id = t;
  update head set snapshot_id = s where ref_id = r and table_id = t;
  return s;
end $$;

-- 8.1 · the same, from one JSON array of objects {path, rc, size, footer, lo, hi}, bounds in hex.
create function p_append_json(t uuid, r int, files jsonb) returns bigint language sql as $$
  select p_append(t, r, array_agg(x.path), array_agg(x.rc), array_agg(x.size), array_agg(x.footer),
                  array_agg(decode(x.lo, 'hex')), array_agg(decode(x.hi, 'hex')))
    from jsonb_to_recordset(files) x(path text, rc bigint, size bigint, footer int, lo text, hi text);
$$;

-- 8.1 · the same, from the rows a client copied into its session's `staged` table.
create function p_append_staged(t uuid, r int) returns bigint language plpgsql as $$
declare s bigint;
begin
  s := p_append(t, r, (select array_agg(path) from staged), (select array_agg(rc) from staged),
                (select array_agg(size) from staged), (select array_agg(footer) from staged),
                (select array_agg(lo) from staged), (select array_agg(hi) from staged));
  truncate staged;
  return s;
end $$;

-- The id of bench table n; table 0 is the large one.
create function bench_id(n int) returns uuid language sql immutable as $$
  select ('00000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid;
$$;

-- Creates bench table n with one long column, its cluster key.
create function bench_table(n int) returns bigint language sql as $$
  select commit(jsonb_build_object('ref', 1, 'author', 'bench', 'actor', 'bench', 'operation', 'append',
    'tables', jsonb_build_array(jsonb_build_object('table_id', bench_id(n),
      'create', jsonb_build_object('namespace', '["bench"]'::jsonb, 'name', 't' || n, 'location', 'gs://bench/t' || n),
      'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}]'::jsonb,
      'cluster_key', '[1]'::jsonb))));
$$;
