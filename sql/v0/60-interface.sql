-- MetaLake v0 · protocol · metadata as SQL (ADR 0003 §2.8).
--
-- Stable views over the tables, for any Postgres client: every live ref at its latest state, the ref a column of
-- each. Within one protocol_version they change only by adding columns. The functions of the reference they rest on
-- resolve through a search_path, so the views reach them through functions that pin it: a client's search_path
-- does not matter.

create function @schema@.latest_namespaces()
returns table (ref_id int, namespace text[], properties jsonb) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select r.ref_id, n.namespace,
         (select coalesce(jsonb_object_agg(x.key, x.value), '{}')
            from visible_namespace_properties(r.ref_id, n.namespace, 9223372036854775806) x where not x.dropped)
    from ref r, visible_namespaces(r.ref_id, 9223372036854775806) n
   where not r.deleted and not n.dropped;
$$;

create function @schema@.latest_tables()
returns table (ref_id int, table_id uuid, namespace text[], name text, snapshot_id bigint) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select r.ref_id, t.table_id, t.namespace, t.name, snapshot_at(r.ref_id, t.table_id, 9223372036854775806)
    from ref r, visible_table_names(r.ref_id, 9223372036854775806) t
   where not r.deleted and not t.dropped;
$$;

create function @schema@.latest_columns()
returns table (ref_id int, table_id uuid, field_id int, parent_field_id int, "position" int, name text, type text,
               required boolean, doc text) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select t.ref_id, t.table_id, f.field_id, f.parent_field_id, f.position, f.name, f.type, f.required, f.doc
    from latest_tables() t, visible_fields(t.ref_id, t.table_id, 9223372036854775806) f
   where not f.dropped;
$$;

create function @schema@.commit_time(seq bigint) returns timestamptz language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select committed_time(seq);
$$;

create view @schema@.refs as
  select r.ref_id, r.name, r.kind, r.protected, p.name as parent, b.fork_seq, r.created_seq,
         @schema@.commit_time(r.created_seq) as created_at
    from @schema@.ref r
    left join @schema@.ref_base b on b.ref_id = r.ref_id and b.end_seq = 9223372036854775807
    left join @schema@.ref p on p.ref_id = b.parent_ref_id
   where not r.deleted;

create view @schema@.namespaces as
  select r.name as ref, n.namespace, n.properties
    from @schema@.latest_namespaces() n join @schema@.ref r on r.ref_id = n.ref_id;

create view @schema@.tables as
  select r.name as ref, t.namespace, t.name, t.table_id, t.snapshot_id
    from @schema@.latest_tables() t join @schema@.ref r on r.ref_id = t.ref_id;

create view @schema@.columns as
  select r.name as ref, t.namespace, t.name as table_name, c.table_id, c.field_id, c.parent_field_id, c."position",
         c.name, c.type, c.required, c.doc
    from @schema@.latest_columns() c
    join @schema@.latest_tables() t on t.ref_id = c.ref_id and t.table_id = c.table_id
    join @schema@.ref r on r.ref_id = c.ref_id;

create view @schema@.snapshots as
  select s.table_id, r.name as ref, s.snapshot_id, s.parent_snapshot_id, s.schema_seq, s.summary ->> 'operation'
         as operation, @schema@.commit_time(s.snapshot_id) as committed_at, s.summary
    from @schema@.snapshot s join @schema@.ref r on r.ref_id = s.ref_id;

create view @schema@.history as
  select c.commit_seq, r.name as ref, @schema@.commit_time(c.commit_seq) as committed_at, c.author, c.actor,
         c.operation, c.app_id, c.app_version, c.message
    from @schema@.commit c join @schema@.ref r on r.ref_id = c.ref_id;

-- The files a read of `table_id` on ref `ref` at `at` opens: plan, by name.
create function @schema@.files(ref text, table_id uuid, at bigint default 9223372036854775806)
returns setof @schema@.planned_file language sql stable as $$
  select f.* from @schema@.ref r, @schema@.plan(r.ref_id, $2, $3) f where r.name = $1 and not r.deleted;
$$;

-- ID-3: writers draw ids before a commit, from outside the catalog's search_path. The reference's function finds its
-- tables through the caller's; the protocol pins it.
alter function @schema@.next_id(uuid, text) set search_path = @schema@, pg_catalog, pg_temp;

-- CMT-2: `snapshot_at` and `table_visible` are called from outside too, and reach the chain of a read through the
-- reference's `chain`, which finds `ref_base` the same way.
alter function @schema@.chain(int, bigint) set search_path = @schema@, pg_catalog, pg_temp;
-- VIS-11: so is `stable_point`, which reads the commit sequence.
alter function @schema@.stable_point() set search_path = @schema@, pg_catalog, pg_temp;

-- A table as a read sees it (TBL-1, VIS-9, §6, §7), for the core, which needs the definition at a ref and a
-- sequence and not only at LATEST: the tables visible at (ref_id, at), and one table's definition there, or null if
-- it is not visible. `specs` and `sort_orders` hold every one the table has had, since each file names its own.
create function @schema@.tables_at(ref_id int, at bigint default 9223372036854775806)
returns table (table_id uuid, namespace text[], name text) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select x.table_id, x.namespace, x.name from visible_table_names($1, $2) x order by x.namespace, x.name;
$$;

create function @schema@.describe(ref_id int, table_id uuid, at bigint default 9223372036854775806)
returns jsonb language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select jsonb_build_object(
    'table_id', n.table_id,
    'namespace', to_jsonb(n.namespace),
    'name', n.name,
    'location', (select t.location from tbl t where t.table_id = $2),
    'snapshot_id', snapshot_at($1, $2, $3),
    'fields', (select coalesce(jsonb_agg(jsonb_build_object(
                 'field_id', f.field_id, 'parent_field_id', f.parent_field_id, 'position', f.position,
                 'name', f.name, 'type', f.type, 'required', f.required, 'initial_default', f.initial_default,
                 'write_default', f.write_default, 'doc', f.doc)
               order by f.parent_field_id nulls first, f.position), '[]')
                 from visible_fields($1, $2, $3) f),
    'spec_id', (select s.spec_id from default_spec($1, $2, $3) s),
    'specs', (select coalesce(jsonb_object_agg(s.spec_id::text, s.fields), '{}')
                from (select distinct spec_id, fields from partition_spec where partition_spec.table_id = $2) s),
    'sort_order_id', (select o.sort_order_id from default_sort_order($1, $2, $3) o),
    'sort_orders', (select coalesce(jsonb_object_agg(o.sort_order_id::text, o.fields), '{}')
                      from (select distinct sort_order_id, fields from sort_order where sort_order.table_id = $2) o),
    'cluster_key', (select to_jsonb(k.field_ids) from visible_cluster_key($1, $2, $3) k),
    'last_field_id', (select c.last_field_id from table_counter c where c.table_id = $2),
    'last_partition_field_id', (select c.last_partition_field_id from table_counter c where c.table_id = $2),
    'properties', (select coalesce(jsonb_object_agg(p.key, p.value), '{}') from visible_properties($1, $2, $3) p))
    from visible_table_names($1, $3) n
   where n.table_id = $2;
$$;

-- VIS-11, CMT-4: the tables of each ref now, each with the sequence to read it at, in one statement. A table's own
-- commits land in sequence order, so no commit of it is in flight below its latest snapshot, and that sequence is stable
-- with nothing to wait for. One statement sees one state, so a commit that wrote several tables is in the sequence of
-- each or of none, and the reads agree as one point would. A branch does not read below its base, so a table it
-- inherits and has not touched is read at the start of the base, where it is the same.
create function @schema@.tables_latest(ref_ids int[])
returns table (ref_id int, table_id uuid, namespace text[], name text, seq bigint) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select r.id, x.table_id, x.namespace, x.name,
         greatest(snapshot_at(r.id, x.table_id, 9223372036854775806),
                  (select b.begin_seq from ref_base b
                    where b.ref_id = r.id and b.end_seq = 9223372036854775807))
    from unnest($1) r(id)
   cross join lateral visible_table_names(r.id, 9223372036854775806) x
   order by r.id, x.namespace, x.name;
$$;

-- TT-3, TBL-1: the tables ref_id saw as of time `at`, each with the sequence to read it at (as_of_time) and its name
-- then: none that was dropped by then or created after. A commit that wrote several tables has one time, so it is in
-- each one's sequence or in none, and the reads agree as one point would. Every table that has been on the ref's
-- chain is a candidate; as_of_time refuses a time less than one second ago or older than the ref keeps.
create function @schema@.tables_as_of(ref_id int, at timestamptz)
returns table (table_id uuid, namespace text[], name text, seq bigint) language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select n.table_id, n.namespace, n.name, s.seq
    from (select distinct x.table_id
            from chain($1, 9223372036854775806) c join table_name x on x.ref_id = c.ref_id) t
   cross join lateral (select as_of_time($1, t.table_id, $2) as seq) s
   cross join lateral (select x.*
                         from chain($1, s.seq) c join table_name x
                           on x.table_id = t.table_id and x.ref_id = c.ref_id
                          and x.begin_seq <= c.seq and c.seq < x.end_seq
                        order by c.level limit 1) n
   where not n.dropped
   order by n.namespace, n.name;
$$;
