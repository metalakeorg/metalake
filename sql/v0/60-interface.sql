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
