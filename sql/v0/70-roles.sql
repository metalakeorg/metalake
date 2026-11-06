-- MetaLake v0 · protocol · roles (ADR 0003 §2.7).
--
-- Rights on tables are what protect the catalog. A reader may read every table and run every function that does
-- not write: one that writes, run as a reader, is refused by the tables. A writer may, besides, run the functions
-- that write, which run as the owner (SECURITY DEFINER, search_path pinned); it has no right to write a table
-- itself, so every write goes through the rules. A catalog without roles runs with one.

-- CMT-10: a writer registers the files it is about to upload, in a transaction of its own.
create function @schema@.register_intents(ref_id int, table_id uuid, paths text[], ttl interval)
returns void language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  insert into intent (path, table_id, ref_id, expires_at)
  select p, $2, $1, now() + $4 from unnest($3) p
  on conflict (path) do update set expires_at = excluded.expires_at;
$$;

-- RET-7b: objects found in storage that nothing knows become expired intents, for orphans to take. An intent that is
-- there stays as it is: its writer may be at work.
create function @schema@.adopt_orphans(ref_id int, table_id uuid, paths text[])
returns bigint language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  with adopted as (
    insert into intent (path, table_id, ref_id, expires_at)
    select p, $2, $1, now() from unnest($3) p
     where not exists (select 1 from data_file d where d.path = p)
       and not exists (select 1 from deletion_vector v where v.path = p)
       and not exists (select 1 from deletion_queue q where q.path = p)
    on conflict (path) do nothing
    returning 1)
  select count(*) from adopted;
$$;

-- The functions a writer calls, and only a writer.
create function @schema@.writing_functions() returns setof regprocedure language sql stable as $$
  select p.oid::regprocedure from pg_proc p
   where p.pronamespace = '@schema@'::regnamespace
     and p.proname in ('commit', 'merge', 'merge_report', 'create_ref', 'delete_ref', 'set_retention',
                       'set_collect_grace',
                       'expire', 'sweep_done', 'forget_intents', 'fill_committed_at', 'next_id',
                       'register_intents', 'adopt_orphans');
$$;

-- Gives the catalog to `owner`, and its rights to `writer` and `reader`, which init created. Run again by migrate,
-- since replacing the functions drops their rights. The names stay in meta.
create function @schema@.grant_roles(owner text, writer text, reader text) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  o record;
  f regprocedure;
begin
  execute format('alter schema @schema@ owner to %I', owner);
  for o in select c.oid::regclass as name, c.relkind from pg_class c
            where c.relnamespace = '@schema@'::regnamespace and c.relkind in ('r', 'v', 'S', 'p') loop
    execute format('alter %s %s owner to %I', case o.relkind when 'v' then 'view' when 'S' then 'sequence'
                                                                 else 'table' end, o.name, owner);
  end loop;
  for o in select p.oid::regprocedure as name from pg_proc p where p.pronamespace = '@schema@'::regnamespace loop
    execute format('alter function %s owner to %I', o.name, owner);
  end loop;
  for o in select t.oid::regtype as name from pg_type t join pg_class c on c.oid = t.typrelid
            where t.typnamespace = '@schema@'::regnamespace and c.relkind = 'c' loop
    execute format('alter type %s owner to %I', o.name, owner);
  end loop;

  revoke all on all tables in schema @schema@ from public;
  revoke all on all sequences in schema @schema@ from public;
  revoke all on all functions in schema @schema@ from public;
  execute format('grant usage on schema @schema@ to %I, %I', writer, reader);
  execute format('grant select on all tables in schema @schema@ to %I', reader);
  execute format('grant execute on all functions in schema @schema@ to %I', reader);
  execute format('grant %I to %I', reader, writer);
  for f in select * from writing_functions() loop
    execute format('alter function %s security definer', f);
    execute format('revoke execute on function %s from %I', f, reader);
    execute format('grant execute on function %s to %I', f, writer);
  end loop;

  insert into meta values ('owner_role', owner), ('writer_role', writer), ('reader_role', reader)
    on conflict (key) do update set value = excluded.value;
end $$;
