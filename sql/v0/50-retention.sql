-- MetaLake v0 · protocol · retention and collection (§13 of the text).
--
-- What collection removes is the reference's (13-retention.sql, installed as it is): it runs in the background, not
-- on a path a reader or a writer waits for. The protocol holds refs with advisory locks (03-helpers.sql), not with
-- their rows, so it replaces the two locks of the reference that take rows, and adds the calls of ADR 0003 §2.4.

-- RET-5: a collection holds every ref shared until it ends: commits go on, and what would make a collectable row
-- needed again (a ref created at an earlier sequence, RET-6; a retention changed; a ref deleted) waits.
create or replace function @schema@.lock_for_collection() returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
begin
  perform lock_ref_shared(r.ref_id) from ref r order by r.ref_id;
end $$;

-- RET-1, RET-5: what ref r keeps, under the ref's exclusive lock.
create or replace function @schema@.set_retention(r int, min_seq bigint default null, max_age interval default null,
    min_snapshots int default null) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
begin
  perform lock_ref(r);
  insert into retention values (r, min_seq, max_age, min_snapshots)
    on conflict (ref_id) do update set min_seq = excluded.min_seq, max_age = excluded.max_age,
                                       min_snapshots = excluded.min_snapshots;
end $$;

-- RET-5 (0.2): expiring and a commit exclude each other on a path, under the reference's key.
create function @schema@.path_lock(path text, exclusive boolean) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
begin
  if exclusive then
    perform pg_advisory_xact_lock(hashtext(current_schema() || ' paths'), hashtext(path));
  else
    perform pg_advisory_xact_lock_shared(hashtext(current_schema() || ' paths'), hashtext(path));
  end if;
end $$;

-- RET-1, RET-4, RET-9: the reference's floor, expiring and sweeping, callable from outside the catalog's search_path.
-- Z3 of ADR 0002 §22 replaces expiring by the protocol's own, by index over the closed rows.
alter function @schema@.retention_floor(int) set search_path = @schema@, pg_catalog, pg_temp;
alter function @schema@.expire() set search_path = @schema@, pg_catalog, pg_temp;
alter function @schema@.sweep_due(int) set search_path = @schema@, pg_catalog, pg_temp;
alter function @schema@.sweep_done(text[]) set search_path = @schema@, pg_catalog, pg_temp;

-- RET-7: the orphans at time `at`: the reference's answer, callable from outside the catalog's search_path.
create or replace function @schema@.orphans(at timestamptz default now()) returns setof text language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select i.path from intent i
   where i.expires_at < at
     and not exists (select 1 from data_file d where d.path = i.path)
     and not exists (select 1 from deletion_vector x where x.path = i.path);
$$;

-- RET-7: forgets the intents of orphans whose objects the caller deleted; an intent that is not an orphan stays.
-- Returns how many it forgot.
create function @schema@.forget_intents(paths text[]) returns bigint language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  with gone as (delete from intent i where i.path = any (paths) and i.path in (select o from orphans() o) returning 1)
  select count(*) from gone;
$$;
