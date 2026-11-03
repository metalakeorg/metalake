-- MetaLake v0 · reference behaviour · retention and collection (§13).
--
-- It defines the right answer, not the fast one.

-- RET-5: sets what ref r keeps, under the lock collections take in share mode.
create function set_retention(r int, min_seq bigint default null, max_age interval default null,
    min_snapshots int default null) returns void language plpgsql as $$
begin
  perform 1 from ref where ref_id = r for no key update;
  insert into retention values (r, min_seq, max_age, min_snapshots)
    on conflict (ref_id) do update set min_seq = excluded.min_seq, max_age = excluded.max_age,
                                       min_snapshots = excluded.min_snapshots;
end $$;

-- RET-1 (0.2): ref r's retention floor, the least of the criteria its retention sets; OPEN when they keep no
-- snapshot, 0 without a criterion.
create function retention_floor(r int) returns bigint language sql stable as $$
  with snaps as (select distinct snapshot_id as s from snapshot where ref_id = r),
  pol as (select * from retention where ref_id = r)
  select coalesce((
    select least(
      pol.min_seq,
      case when pol.max_age is not null then
        coalesce((select min(s) from snaps where committed_time(s) >= now() - pol.max_age), 9223372036854775807) end,
      case when pol.min_snapshots is not null then
        case when pol.min_snapshots <= 0 then 9223372036854775807
             else coalesce((select s from snaps order by s desc offset pol.min_snapshots - 1 limit 1), 0) end end)
      from pol), 0);
$$;

-- RET-1: the snapshots ref r keeps.
create function kept_snapshots(r int) returns setof bigint language sql stable as $$
  select distinct snapshot_id from snapshot where ref_id = r and snapshot_id >= retention_floor(r);
$$;

-- RET-2 (0.2): the retained reads, as ranges: every sequence of a live ref from its floor up.
create function retained_ranges() returns table (ref_id int, lo bigint, hi bigint) language sql stable as $$
  select r.ref_id, least(retention_floor(r.ref_id), 9223372036854775806), 9223372036854775806::bigint
    from ref r where not r.deleted;
$$;

-- RET-2: the reach points of the retained reads, as ranges: a read of a ref over [lo, hi] goes, through each
-- ref_base row valid somewhere in it, to the parent at that row's fork (VIS-3).
create function reach_ranges() returns table (ref_id int, lo bigint, hi bigint) language sql stable as $$
  with recursive rr (ref_id, lo, hi) as (
    select * from retained_ranges()
    union
    select b.parent_ref_id, b.fork_seq, b.fork_seq
      from rr join ref_base b on b.ref_id = rr.ref_id and b.begin_seq <= rr.hi and rr.lo < b.end_seq
  )
  select * from rr;
$$;

-- The tables whose rows carry a ref and a validity range (RET-3).
create function ranged_tables() returns setof text language sql immutable as $$
  values ('data_file'), ('tombstone'), ('deletion_vector'), ('ref_base'), ('table_name'), ('table_property'),
         ('field'), ('partition_spec'), ('sort_order'), ('cluster_key'), ('namespace'), ('namespace_property');
$$;

-- RET-3: the collectable rows of a ranged table, as JSON: valid at no sequence of a reach range of their ref.
create function collectable_rows(tab text) returns setof jsonb language plpgsql stable as $$
begin
  return query execute format($q$
    with p as materialized (select * from reach_ranges())
    select to_jsonb(x) from %I x
     where not exists (select 1 from p where p.ref_id = x.ref_id and x.begin_seq <= p.hi and p.lo < x.end_seq)$q$,
    tab);
end $$;

-- RET-3: the snapshots that retained reads use; the others are collectable. Over [lo, hi], a read uses the
-- snapshot in force at lo and every snapshot of the ref after it, up to hi.
create function needed_snapshots() returns table (table_id uuid, snapshot_id bigint) language sql stable as $$
  select distinct t.table_id, x.id
    from retained_ranges() rr, tbl t,
         lateral (select snapshot_at(rr.ref_id, t.table_id, rr.lo) as id
                  union
                  select s.snapshot_id from snapshot s
                   where s.table_id = t.table_id and s.ref_id = rr.ref_id and s.snapshot_id > rr.lo
                     and s.snapshot_id <= rr.hi) x
   where x.id is not null;
$$;

-- RET-5: the lock an expiring batch holds until it ends.
create function lock_for_collection() returns void language sql as $$
  select null from (select 1 from ref for share) x;
$$;

-- RET-4, RET-5: expires every collectable row, in one transaction, and queues the objects nothing refers to any
-- more. Returns how many rows it deleted.
create function expire() returns bigint language plpgsql as $$
declare
  tab text;
  n bigint := 0;
  k bigint;
begin
  perform lock_for_collection();
  create temp table if not exists doomed (tab text, row jsonb) on commit drop;
  truncate doomed;
  for tab in select * from ranged_tables() loop
    insert into doomed select tab, j from collectable_rows(tab) j;
  end loop;
  create temp table if not exists kept_snapshot (table_id uuid, snapshot_id bigint) on commit drop;
  truncate kept_snapshot;
  insert into kept_snapshot select * from needed_snapshots();
  -- The objects the doomed rows refer to: data files that are not inline, and Puffin files.
  create temp table if not exists touched (path text, kind text) on commit drop;
  truncate touched;
  insert into touched
  select distinct d.row ->> 'path', 'data' from doomed d
   where d.tab = 'data_file'
     and not exists (select 1 from inline_file i where i.table_id = (d.row ->> 'table_id')::uuid
                                                   and i.file_id = (d.row ->> 'file_id')::bigint)
  union
  select distinct d.row ->> 'path', 'puffin' from doomed d
   where d.tab = 'deletion_vector' and d.row ->> 'path' is not null;
  -- RET-5: a path is never queued while a commit adds it.
  perform path_lock(t.path, true) from touched t order by t.path;

  for tab in select * from ranged_tables() loop
    execute format($q$
      delete from %I x using doomed d
       where d.tab = $1 and x.ref_id = (d.row ->> 'ref_id')::int and x.begin_seq = (d.row ->> 'begin_seq')::bigint
         and to_jsonb(x) = d.row$q$, tab) using tab;
    get diagnostics k = row_count; n := n + k;
  end loop;
  delete from inline_file i where not exists (select 1 from data_file d where d.table_id = i.table_id and d.file_id = i.file_id);
  get diagnostics k = row_count; n := n + k;
  delete from file_footer x where not exists (select 1 from data_file d where d.table_id = x.table_id and d.file_id = x.file_id);
  get diagnostics k = row_count; n := n + k;
  delete from snapshot s where not exists (select 1 from kept_snapshot k2 where k2.table_id = s.table_id and k2.snapshot_id = s.snapshot_id);
  get diagnostics k = row_count; n := n + k;
  delete from head h where exists (select 1 from ref r where r.ref_id = h.ref_id and r.deleted);
  get diagnostics k = row_count; n := n + k;
  delete from idempotency i where exists (select 1 from ref r where r.ref_id = i.ref_id and r.deleted);
  get diagnostics k = row_count; n := n + k;
  delete from commit c
   where not exists (select 1 from snapshot s where s.snapshot_id = c.commit_seq)
     and not exists (select 1 from ref r where r.created_seq = c.commit_seq)
     and not exists (select 1 from idempotency i where i.commit_seq = c.commit_seq);
  get diagnostics k = row_count; n := n + k;

  -- RET-4: the objects of the doomed rows that no remaining row refers to.
  insert into deletion_queue
  select t.path, t.kind, now() from touched t
   where not exists (select 1 from data_file d where d.path = t.path)
     and not exists (select 1 from deletion_vector x where x.path = t.path)
  on conflict (path) do nothing;
  return n;
end $$;

-- RET-9: the grace a queued object waits.
create function collect_grace() returns interval language sql stable as $$
  select coalesce((select value::interval from meta where key = 'collect_grace'), interval '1 day');
$$;

-- RET-9: up to `lim` queued objects whose grace has passed, oldest first, for a sweeper to delete.
create function sweep_due(lim int default null) returns setof text language sql stable as $$
  select path from deletion_queue where expired_at < now() - collect_grace() order by expired_at, path limit lim;
$$;

-- RET-9: forgets the objects a sweeper deleted; one whose grace has not passed stays. Returns how many it forgot.
create function sweep_done(paths text[]) returns bigint language sql as $$
  with gone as (delete from deletion_queue q where q.path = any (paths) and q.expired_at < now() - collect_grace()
                returning 1)
  select count(*) from gone;
$$;

-- RET-7: the orphans at time `at`.
create function orphans(at timestamptz default now()) returns setof text language sql stable as $$
  select i.path from intent i
   where i.expires_at < at
     and not exists (select 1 from data_file d where d.path = i.path)
     and not exists (select 1 from deletion_vector x where x.path = i.path);
$$;
