-- MetaLake v0 · reference behaviour · migrating a catalog of an earlier revision (§18).
--
-- It defines the right answer, not the fast one.

-- One transaction, each revision in turn. From version 0 to 0.1: a commit `migrate` on main; on main, every
-- namespace, and every parent of one, in which a table exists on main; on each other live branch, those in which a
-- table exists only there. From 0.1 to 0.2: the objects of 04.6-collection.sql and collect_grace, no row changed.
-- Returns the sequence of the commit of 0.1, or NULL if the catalog was already at 0.1 or later.

create function migrate() returns bigint language plpgsql as $$
declare
  latest constant bigint := 9223372036854775806;
  v text;
  m bigint;
  r ref;
begin
  perform 1 from ref where ref_id = 1 for update;
  v := coalesce((select value from meta where key = 'writer_version'), '0');
  if v not in ('0', '1', '2') then
    raise exception 'metalake unsupported: writer_version %', v using errcode = '0A000';
  end if;
  if v = '2' then
    return null;
  end if;
  if v = '0' then
    m := nextval('commit_seq_gen');
    insert into commit values (m, 1, pg_current_xact_id(), null, 'metalake', 'metalake', 'migrate', null, null, null, null);
    for r in select * from ref where not deleted and kind = 'branch' order by ref_id loop
      insert into namespace (namespace, dropped, ref_id, begin_seq)
      select distinct t.namespace[1:n], false, r.ref_id, m
        from visible_table_names(r.ref_id, latest) t, generate_series(1, cardinality(t.namespace)) n
       where not namespace_exists(r.ref_id, t.namespace[1:n], latest);
    end loop;
  end if;
  -- 0.2: as 04.6-collection.sql.
  create table if not exists deletion_queue (
    path text primary key,
    kind text not null check (kind in ('data', 'puffin')),
    expired_at timestamptz not null);
  create index if not exists deletion_queue_due on deletion_queue (expired_at);
  create index if not exists data_file_closed on data_file (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists tombstone_closed on tombstone (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists deletion_vector_closed on deletion_vector (ref_id, end_seq)
    where end_seq <> 9223372036854775807;
  create index if not exists ref_base_closed on ref_base (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists table_name_closed on table_name (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists table_property_closed on table_property (ref_id, end_seq)
    where end_seq <> 9223372036854775807;
  create index if not exists field_closed on field (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists partition_spec_closed on partition_spec (ref_id, end_seq)
    where end_seq <> 9223372036854775807;
  create index if not exists sort_order_closed on sort_order (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists cluster_key_closed on cluster_key (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists namespace_closed on namespace (ref_id, end_seq) where end_seq <> 9223372036854775807;
  create index if not exists namespace_property_closed on namespace_property (ref_id, end_seq)
    where end_seq <> 9223372036854775807;
  create index if not exists data_file_path on data_file (path);
  create index if not exists deletion_vector_path on deletion_vector (path) where path is not null;
  insert into meta values ('collect_grace', '1 day') on conflict (key) do nothing;
  insert into meta values ('writer_version', '2') on conflict (key) do update set value = '2';
  return m;
end $$;
