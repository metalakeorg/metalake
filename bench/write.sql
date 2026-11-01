-- MetaLake · preliminary study 2.3: the commit end to end. On top of `schema.sql` + `merge.sql`. Prototype.
--
-- 1. Before uploading, the writer registers the INTENT of each file (its path and when it expires), in its own
--    transaction. So an uploaded Parquet whose commit never arrives is a named orphan, not something to find by listing.
-- 2. It uploads the Parquet files to object storage (outside the database) and computes their payload from the
--    footer: rows, bytes, footer size, min/max.
-- 3. The commit is ONE transaction: it checks idempotency (ref, app) → version, like Delta's `txn`; opens the commit
--    with the head lock; registers the files; deletes their intents; records the version.
-- If the commit response is lost (CommitStateUnknown), retrying with the same (app, version) is safe.

create table if not exists idempotency (
  ref int not null, app text not null, version bigint not null, commit_id bigint not null, primary key (ref, app));
create table if not exists intent (
  path text primary key, tbl int not null, ref int not null, expires_at timestamptz not null);

create or replace function register_intent(t int, r int, paths text[], ttl interval) returns void language sql as $$
  insert into intent select x, t, r, now() + ttl from unnest(paths) x on conflict (path) do nothing;
$$;

-- Returns the commit: positive if it applies it now, negative (the existing one) if that version was already applied.
create or replace function commit_write(t int, r int, a text, ver bigint, files jsonb) returns bigint
language plpgsql as $$
declare v bigint; c bigint; s bigint;
begin
  insert into idempotency values (r, a, 0, 0) on conflict do nothing;
  select version, commit_id into v, c from idempotency where ref = r and app = a for update;
  if v >= ver then return -c; end if;
  s := begin_commit(t, r);
  insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)
    select t, nextval('fid'), r, s, (f ->> 'row_count')::bigint, (f ->> 'min')::bigint, (f ->> 'max')::bigint, f ->> 'path'
      from jsonb_array_elements(files) f;
  delete from intent where path in (select f ->> 'path' from jsonb_array_elements(files) f);
  update idempotency set version = ver, commit_id = s where ref = r and app = a;
  return s;
end $$;

-- The orphans: expired intents whose file never got in. They are deleted from object storage and then from here.
create or replace function orphans(at_ts timestamptz) returns setof text language sql stable as $$
  select i.path from intent i
   where i.expires_at < at_ts and not exists (select 1 from o1_file f where f.tbl = i.tbl and f.path = i.path);
$$;
