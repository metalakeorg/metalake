-- G1: :tables tables on main with one file each, and the calls each load makes, through the protocol.

select set_config('bench.tables', :'tables', false);
create sequence bench_seq;

create function bench_id(n int) returns uuid language sql immutable as $$
  select ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid;
$$;

-- A file of ten rows whose cluster key, field 1, holds [k, k + 9].
create function bench_file(t uuid, k bigint) returns file_add language sql as $$
  select row(t, null, 'gs://bench/' || gen_random_uuid() || '.parquet', 0, '[]'::jsonb, null, 10, 1000000, 10000,
             null, '{1}'::int[], array[encode_long(k)], array[encode_long(k + 9)], '{0}'::bigint[], '{null}'::bigint[],
             null, null, null)::file_add;
$$;

create function bench_create_table(r int, t uuid, ns text, name text) returns bigint language sql as $$
  select (commit(ref_id => r, operation => 'update', changes => jsonb_build_object(t::text, jsonb_build_object(
    'create', jsonb_build_object('namespace', jsonb_build_array(ns), 'name', name, 'location', 'gs://bench'),
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}]'::jsonb,
    'cluster_key', '[1]'::jsonb)), author => 'bench', actor => 'bench')).commit_seq;
$$;

-- The commit of data: one file to table n of ref r.
create function bench_append(r int, n int) returns bigint language sql as $$
  select (commit(ref_id => r, operation => 'append', files => array[bench_file(bench_id(n), n)],
                 author => 'bench', actor => 'bench')).commit_seq;
$$;

create function bench_branch() returns int language sql as $$
  select create_ref(name => 'b' || nextval('bench_seq'), parent => 1);
$$;

-- A table created on main, and dropped, as an agent's change of names does.
create function bench_create() returns int language plpgsql as $$
declare k int := nextval('bench_seq');
begin
  perform bench_create_table(1, bench_id(1000000 + k), 'ddl', 'x' || k);
  return k;
end $$;

create function bench_drop(k int) returns bigint language sql as $$
  select (commit(ref_id => 1, operation => 'update',
                 changes => jsonb_build_object(bench_id(1000000 + k)::text, '{"drop": true}'::jsonb),
                 author => 'bench', actor => 'bench')).commit_seq;
$$;

do $$
begin
  for n in 1 .. current_setting('bench.tables')::int loop
    perform bench_create_table(1, bench_id(n), 'db' || (n % 10), 't' || n);
    perform bench_append(1, n);
  end loop;
end $$;
vacuum analyze;
