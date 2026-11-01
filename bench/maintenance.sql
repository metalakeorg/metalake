-- MetaLake · preliminary study 2.6: garbage collection and retention with branches. On top of `schema.sql` +
-- `merge.sql`. Prototype.
--
-- A file (table, ref R, begin_seq, end_seq) is seen by:
--   · the snapshots of R in [begin_seq, end_seq);
--   · each direct child branch D of R (and all its descendants, which see R through D) if begin_seq <= D.fork_seq < end_seq.
-- Retention: each ref expires its snapshots older than its horizon. A file can be collected when no retained snapshot
-- of R sees it (end_seq <= horizon of R) and no live child of R sees it at its fork point.
-- A deleted branch whose descendants are still alive stays «hidden»: its rows remain, because its children read through it.
-- Tombstones do not count (keeping more is safe).

alter table ref add column if not exists deleted boolean not null default false;
create table if not exists retention (ref int primary key, horizon bigint not null);

-- The live refs: not deleted, or with some live descendant.
create or replace function is_live(r int) returns boolean language sql stable as $$
  with recursive d (id) as (select r union all select x.id from ref x join d on x.parent = d.id)
  select exists (select 1 from d join ref x on x.id = d.id where not x.deleted);
$$;

create or replace function collectable() returns table (tbl int, id bigint, path text) language sql stable as $$
  select f.tbl, f.id, f.path from o1_file f join ref r on r.id = f.ref
   where (not is_live(r.id))
      or (f.end_seq < 9223372036854775807
          and f.end_seq <= coalesce((select h.horizon from retention h where h.ref = r.id), 0)
          and not exists (select 1 from ref d
                           where d.parent = r.id and is_live(d.id) and f.begin_seq <= d.fork_seq and d.fork_seq < f.end_seq));
$$;
