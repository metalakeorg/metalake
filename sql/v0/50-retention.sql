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

-- RET-9: how long a queued object waits before a sweep may delete it.
create function @schema@.set_collect_grace(grace interval) returns void language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  insert into meta (key, value) values ('collect_grace', grace::text)
    on conflict (key) do update set value = excluded.value;
$$;

-- RET-9: the grace a queued object waits, in seconds, for a client that keeps objects of its own by it.
create function @schema@.collect_grace_seconds() returns float8 language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select extract(epoch from collect_grace())::float8;
$$;

-- RET-5 (0.2): expiring and a commit exclude each other on a path's bucket, under the reference's key
-- (09-commits.sql, path_bucket). Callers take a set of buckets in one statement, each once, in ascending order.
create function @schema@.path_lock(path text, exclusive boolean) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
begin
  if exclusive then
    perform pg_advisory_xact_lock(hashtext(current_schema() || ' paths'), hashtext(path) & 1023);
  else
    perform pg_advisory_xact_lock_shared(hashtext(current_schema() || ' paths'), hashtext(path) & 1023);
  end if;
end $$;

-- RET-9: the end of a sweep, callable from outside the catalog's search_path.
alter function @schema@.sweep_done(text[]) set search_path = @schema@, pg_catalog, pg_temp;
-- INL-2: whether a table's inline files are due, callable from outside too.
alter function @schema@.inline_flush_due(int, uuid) set search_path = @schema@, pg_catalog, pg_temp;

-- RET-1: the reference's floor, by what it keeps and not by the ref's history, since a reader and every read of changes
-- asks for it (CHG-2, CHG-3). min_snapshots walks that many snapshots down from the latest, each sequence once however many
-- tables it wrote. max_age walks up from the oldest to the first one young enough, as times follow sequences only roughly:
-- it costs the snapshots past their age that expiring has not taken yet.
create or replace function @schema@.retention_floor(r int) returns bigint language plpgsql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  open constant bigint := 9223372036854775807;
  pol retention;
  aged bigint;
  counted bigint;
begin
  select * into pol from retention x where x.ref_id = r;
  if not found then
    return 0;
  end if;
  if pol.max_age is not null then
    select s.snapshot_id into aged from snapshot s join commit c on c.commit_seq = s.snapshot_id
     where s.ref_id = r
       and coalesce(c.committed_at, pg_xact_commit_timestamp(xid32(c.xid)), no_time(c.commit_seq, c.xid))
           >= now() - pol.max_age
     order by s.ref_id, s.snapshot_id limit 1;
    if not found then
      aged := open;
    end if;
  end if;
  if pol.min_snapshots is not null then
    if pol.min_snapshots <= 0 then
      counted := open;
    else
      with recursive w (s, n) as (
        select (select max(x.snapshot_id) from snapshot x where x.ref_id = r), 1
        union all
        select (select max(x.snapshot_id) from snapshot x where x.ref_id = r and x.snapshot_id < w.s), w.n + 1
          from w where w.s is not null and w.n < pol.min_snapshots)
      select coalesce((select w.s from w where w.n = pol.min_snapshots), 0) into counted;
    end if;
  end if;
  return coalesce(least(pol.min_seq, aged, counted), 0);
end $$;

-- RET-3 (note): the protocol's indexes for a ref's snapshots and commits below its floor, and for whether a snapshot
-- of a commit remains.
create index if not exists snapshot_by_ref on @schema@.snapshot (ref_id, snapshot_id);
create index if not exists snapshot_by_id on @schema@.snapshot (snapshot_id);
create index if not exists commit_by_ref on @schema@.commit (ref_id, commit_seq);

-- RET-3 to RET-5: expires up to `lim` collectable rows; the protocol's own, where the reference checks every row of
-- every ranged table. By RET-2, a row of a live ref is collectable only if it closed at or below the ref's floor, so
-- the candidates of a live ref are its closed rows below its floor, through the closed-row index, and those of a
-- deleted ref all its rows; each is then checked against the reach ranges, as many as the refs and their bases.
-- It deletes and queues what the reference's does, in batches of 1,000 rows by default, which bounds how long it
-- holds the refs (RET-5): run it until it returns 0.
drop function @schema@.expire();
create function @schema@.expire(lim int default 1000) returns bigint language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  open constant bigint := 9223372036854775807;
  budget bigint := coalesce(lim, open);
  n bigint := 0;
  k bigint;
  tab text;
  live text;
begin
  perform lock_for_collection();
  create temp table if not exists x_ranges (ref_id int, lo bigint, hi bigint) on commit drop;
  truncate x_ranges;
  insert into x_ranges select * from reach_ranges();
  -- Per ref, the bound of its candidates: a live ref's floor, where its range up to LATEST starts; OPEN if deleted.
  create temp table if not exists x_bound (ref_id int primary key, m bigint, deleted boolean) on commit drop;
  truncate x_bound;
  insert into x_bound
  select r.ref_id, case when r.deleted then open
                        else (select min(g.lo) from x_ranges g where g.ref_id = r.ref_id and g.hi = open - 1) end,
         r.deleted
    from ref r;
  create temp table if not exists x_touched (path text, kind text, table_id uuid, file_id bigint) on commit drop;
  truncate x_touched;

  for tab in select * from ranged_tables() loop
    foreach live in array array['not b.deleted and x.end_seq <> 9223372036854775807 and x.end_seq <= b.m',
                                'b.deleted'] loop
      exit when budget <= 0;
      execute format($q$
        with c as (
          select x.ctid as id from x_bound b join %1$I x on x.ref_id = b.ref_id
           where %2$s
             and not exists (select 1 from x_ranges g
                              where g.ref_id = x.ref_id and x.begin_seq <= g.hi and g.lo < x.end_seq)
           limit $1),
        d as (delete from %1$I x using c where x.ctid = c.id returning x.*),
        t as (insert into x_touched select %3$s from d where %4$s)
        select count(*) from d$q$,
        tab, live,
        case tab when 'data_file' then $s$d.path, 'data', d.table_id, d.file_id$s$
                 when 'deletion_vector' then $s$d.path, 'puffin', d.table_id, d.file_id$s$
                 else 'null, null, null::uuid, null::bigint' end,
        case when tab in ('data_file', 'deletion_vector') then 'true' else 'false' end)
        into k using budget;
      n := n + k;
      budget := budget - k;
    end loop;
  end loop;

  -- RET-4: inline files and footers go with the last row of their file.
  delete from inline_file i using (select distinct table_id, file_id from x_touched where kind = 'data') t
   where i.table_id = t.table_id and i.file_id = t.file_id
     and not exists (select 1 from data_file d where d.table_id = i.table_id and d.file_id = i.file_id);
  get diagnostics k = row_count; n := n + k;
  delete from file_footer f using (select distinct table_id, file_id from x_touched where kind = 'data') t
   where f.table_id = t.table_id and f.file_id = t.file_id
     and not exists (select 1 from data_file d where d.table_id = f.table_id and d.file_id = f.file_id);
  get diagnostics k = row_count; n := n + k;

  -- RET-5, RET-4: a path nothing refers to any more is queued, held against a commit that adds it.
  perform pg_advisory_xact_lock(hashtext(current_schema() || ' paths'), z.b)
     from (select distinct hashtext(path) & 1023 as b from x_touched where path is not null) z
    order by z.b;
  insert into deletion_queue
  select distinct on (t.path) t.path, t.kind, now() from x_touched t
   where t.path is not null
     and not exists (select 1 from data_file d where d.path = t.path)
     and not exists (select 1 from deletion_vector v where v.path = t.path)
   order by t.path
  on conflict (path) do nothing;

  -- RET-3: snapshots below a ref's floor, or of a deleted ref, that no live ref reads at its floor.
  if budget > 0 then
    with c as (
      select s.ctid as id from x_bound b join snapshot s on s.ref_id = b.ref_id
       where (b.deleted or s.snapshot_id < b.m)
         and not exists (select 1 from x_bound l
                          where not l.deleted and snapshot_at(l.ref_id, s.table_id, l.m) = s.snapshot_id)
       limit budget)
    delete from snapshot s using c where s.ctid = c.id;
    get diagnostics k = row_count; n := n + k; budget := budget - k;
  end if;
  delete from head h using ref r where r.ref_id = h.ref_id and r.deleted;
  get diagnostics k = row_count; n := n + k;
  delete from idempotency i using ref r where r.ref_id = i.ref_id and r.deleted;
  get diagnostics k = row_count; n := n + k;
  -- RET-3: the key of an Iceberg commit lasts as a retry could. A native app_id may live for years.
  delete from idempotency i using x_bound b
   where b.ref_id = i.ref_id and not b.deleted and i.app_id like 'iceberg:%' and i.commit_seq < b.m;
  get diagnostics k = row_count; n := n + k;
  -- RET-3 (0.2): commits below a ref's floor, or of a deleted ref, that nothing remaining names.
  if budget > 0 then
    with c as (
      select x.ctid as id from x_bound b join commit x on x.ref_id = b.ref_id
       where (b.deleted or x.commit_seq < b.m)
         and not exists (select 1 from snapshot s where s.snapshot_id = x.commit_seq)
         and not exists (select 1 from ref r where r.created_seq = x.commit_seq)
         and not exists (select 1 from idempotency i where i.commit_seq = x.commit_seq)
       limit budget)
    delete from commit x using c where x.ctid = c.id;
    get diagnostics k = row_count; n := n + k;
  end if;
  return n;
end $$;

-- RET-9: up to `lim` queued objects whose grace has passed, oldest first; sweepers in parallel skip each other's.
create or replace function @schema@.sweep_due(lim int default null) returns setof text language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  select path from deletion_queue where expired_at < now() - collect_grace()
   order by expired_at, path limit lim for update skip locked;
$$;

-- RET-7: the orphans at time `at`: the reference's answer, callable from outside the catalog's search_path.
create or replace function @schema@.orphans(at timestamptz default now()) returns setof text language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select i.path from intent i
   where i.expires_at < at
     and not exists (select 1 from data_file d where d.path = i.path)
     and not exists (select 1 from deletion_vector x where x.path = i.path)
     and not exists (select 1 from deletion_queue q where q.path = i.path);
$$;

-- RET-7: forgets the intents of orphans whose objects the caller deleted; an intent that is not an orphan stays.
-- Returns how many it forgot.
create function @schema@.forget_intents(paths text[]) returns bigint language sql
set search_path = @schema@, pg_catalog, pg_temp as $$
  with gone as (delete from intent i where i.path = any (paths) and i.path in (select o from orphans() o) returning 1)
  select count(*) from gone;
$$;
