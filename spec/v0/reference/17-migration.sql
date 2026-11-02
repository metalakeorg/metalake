-- MetaLake v0 · reference behaviour · revision 0.1: migrating a catalog of version 0 (§18).
--
-- It defines the right answer, not the fast one.

-- One transaction: a commit `migrate` on main; on main, every namespace, and every parent of one, in which a table
-- exists on main; on each other live branch, those in which a table exists only there; then writer_version 1.
-- Returns the commit's sequence, or NULL if the catalog is already at writer_version 1.
create function migrate() returns bigint language plpgsql as $$
declare
  latest constant bigint := 9223372036854775806;
  m bigint;
  r ref;
begin
  perform 1 from ref where ref_id = 1 for update;
  if coalesce((select value from meta where key = 'writer_version'), '0') = '1' then
    return null;
  end if;
  if coalesce((select value from meta where key = 'writer_version'), '0') <> '0' then
    raise exception 'metalake unsupported: writer_version %', (select value from meta where key = 'writer_version')
      using errcode = '0A000';
  end if;
  m := nextval('commit_seq_gen');
  insert into commit values (m, 1, pg_current_xact_id(), null, 'metalake', 'metalake', 'migrate', null, null, null, null);
  for r in select * from ref where not deleted and kind = 'branch' order by ref_id loop
    insert into namespace (namespace, dropped, ref_id, begin_seq)
    select distinct t.namespace[1:n], false, r.ref_id, m
      from visible_table_names(r.ref_id, latest) t, generate_series(1, cardinality(t.namespace)) n
     where not namespace_exists(r.ref_id, t.namespace[1:n], latest);
  end loop;
  insert into meta values ('writer_version', '1') on conflict (key) do update set value = '1';
  return m;
end $$;
