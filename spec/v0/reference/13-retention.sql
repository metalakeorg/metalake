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

-- RET-1: the snapshots ref r keeps.
create function kept_snapshots(r int) returns setof bigint language sql stable as $$
  with snaps as (select distinct snapshot_id as s from snapshot where ref_id = r),
  pol as (select * from retention where ref_id = r and (min_seq is not null or max_age is not null or min_snapshots is not null))
  select s from snaps
   where not exists (select 1 from pol)
      or s >= (select min_seq from pol)
      or committed_time(s) >= now() - (select max_age from pol)
      or s in (select s from snaps order by s desc limit coalesce((select min_snapshots from pol), 0));
$$;

-- RET-2: the retained reads and their reach points.
create function retained_reads() returns table (ref_id int, seq bigint) language sql stable as $$
  select r.ref_id, 9223372036854775806::bigint from ref r where not r.deleted
  union
  select r.ref_id, k from ref r, kept_snapshots(r.ref_id) k where not r.deleted;
$$;

create function reach_points() returns table (ref_id int, seq bigint) language sql stable as $$
  select distinct c.ref_id, c.seq from retained_reads() rr, chain(rr.ref_id, rr.seq) c;
$$;

-- The tables whose rows carry a ref and a validity range (RET-3).
create function ranged_tables() returns setof text language sql immutable as $$
  values ('data_file'), ('tombstone'), ('deletion_vector'), ('ref_base'), ('table_name'), ('table_property'),
         ('field'), ('partition_spec'), ('sort_order'), ('cluster_key');
$$;

-- RET-3: the collectable rows of a ranged table, as JSON.
create function collectable_rows(tab text) returns setof jsonb language plpgsql stable as $$
begin
  return query execute format($q$
    with p as materialized (select * from reach_points())
    select to_jsonb(x) from %I x
     where not exists (select 1 from p where p.ref_id = x.ref_id and x.begin_seq <= p.seq and p.seq < x.end_seq)$q$, tab);
end $$;

-- RET-3: the snapshots that retained reads use; the others are collectable.
create function needed_snapshots() returns table (table_id uuid, snapshot_id bigint) language sql stable as $$
  select distinct t.table_id, snapshot_at(rr.ref_id, t.table_id, rr.seq)
    from retained_reads() rr, tbl t
   where snapshot_at(rr.ref_id, t.table_id, rr.seq) is not null;
$$;

-- RET-5: the lock a collection holds until it ends.
create function lock_for_collection() returns void language sql as $$
  select null from (select 1 from ref for share) x;
$$;

-- RET-4: the objects in storage that can be deleted: data files all of whose rows are collectable, and Puffin
-- files no remaining vector refers to.
create function collectable_objects() returns setof text language plpgsql as $$
begin
  perform lock_for_collection();
  return query
    with gone as (select (j ->> 'table_id')::uuid as table_id, (j ->> 'file_id')::bigint as file_id,
                         count(*) as n, max(j ->> 'path') as path
                    from collectable_rows('data_file') j group by 1, 2)
    select g.path from gone g
     where g.path is not null
       and g.n = (select count(*) from data_file d where d.table_id = g.table_id and d.file_id = g.file_id)
    union
    select x.path from deletion_vector x where x.path is not null
    group by x.path
    having count(*) = (select count(*) from collectable_rows('deletion_vector') j where j ->> 'path' = x.path);
end $$;

-- RET-3 and RET-4: deletes every collectable row, after the caller deleted collectable_objects(). Returns how
-- many rows it deleted.
create function collect() returns bigint language plpgsql as $$
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
  return n;
end $$;

-- RET-7: the orphans at time `at`.
create function orphans(at timestamptz default now()) returns setof text language sql stable as $$
  select i.path from intent i
   where i.expires_at < at
     and not exists (select 1 from data_file d where d.path = i.path)
     and not exists (select 1 from deletion_vector x where x.path = i.path);
$$;
