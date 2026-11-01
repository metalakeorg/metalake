-- MetaLake · preliminary study 1.2: merging a branch into its parent, on top of o1 (validity range per ref with
-- inheritance). Loaded after `schema.sql`. Throwaway prototype.
--
-- A branch b stores its changes since it forked: its files (rows with ref = b), its tombstones (what it removed from
-- what it inherited) and its *deletion vectors*. Merging is carrying that diff to the parent p in one transaction, if
-- it does not conflict with what p changed since the fork, and leaving b as an empty child of p at the merge commit.

-- Deletes by position (deletion vectors): the live set of a file in a ref.
create table if not exists o1_dv (
  tbl int not null, ref int not null, file bigint not null,
  begin_seq bigint not null, end_seq bigint not null default 9223372036854775807, pos bigint[] not null);
create index if not exists o1_dv_f on o1_dv (tbl, file, ref);

-- ── the operations of a branch: each one is a commit with the head lock ────────────────────────────────────────

create or replace function begin_commit(t int, r int) returns bigint language plpgsql as $$
declare s bigint;
begin
  insert into head values (r, t, 0) on conflict do nothing;
  perform 1 from head where ref = r and tbl = t for update;
  s := nextval('seq');
  insert into snapshot values (t, r, s, (select snap from head where ref = r and tbl = t));
  update head set snap = s where ref = r and tbl = t;
  return s;
end $$;

create or replace function branch(name text, parent int) returns int language sql as $$
  insert into ref values ((select coalesce(max(id), 0) + 1 from ref), name, parent, (select last_value from seq))
  returning id;
$$;

-- The live set of (r, t) at s: the o1 query (the tombstones that apply, computed once).
create or replace function visible(r int, t int, s bigint) returns setof bigint language sql stable as $$
  with recursive chain (lvl, ref, upto_s) as (
    select 0, r, s
    union all
    select c.lvl + 1, x.parent, x.fork_seq from chain c join ref x on x.id = c.ref where x.parent is not null),
  covered as (
    select l.file, min(d.lvl) as lvl from o1_tombstone l join chain d on d.ref = l.ref and l.begin_seq <= d.upto_s
     where l.tbl = t group by l.file)
  select f.id from chain c
    join o1_file f on f.tbl = t and f.ref = c.ref and f.begin_seq <= c.upto_s and f.end_seq > c.upto_s
    left join covered x on x.file = f.id and x.lvl < c.lvl
   where x.file is null;
$$;

-- The live DV of a file in (r, t) at s: the one of the nearest level.
create or replace function dv_of(r int, t int, f bigint, s bigint) returns bigint[] language sql stable as $$
  with recursive chain (lvl, ref, upto_s) as (
    select 0, r, s
    union all
    select c.lvl + 1, x.parent, x.fork_seq from chain c join ref x on x.id = c.ref where x.parent is not null)
  select d.pos from chain c join o1_dv d on d.tbl = t and d.file = f and d.ref = c.ref
     and d.begin_seq <= c.upto_s and d.end_seq > c.upto_s
   order by c.lvl limit 1;
$$;

create or replace function op_add(t int, r int, ids bigint[]) returns void language plpgsql as $$
declare s bigint := begin_commit(t, r);
begin
  insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)
    select t, i, r, s, 1000, i * 10, i * 10 + 9, 'gs://lake/f/' || i || '.parquet' from unnest(ids) i;
end $$;

-- Remove (delete a whole file, or the «remove» half of a compaction): one's own rows are closed, inherited ones are covered.
create or replace function op_remove(t int, r int, ids bigint[]) returns void language plpgsql as $$
declare s bigint := begin_commit(t, r); n int;
begin
  if exists (select 1 from unnest(ids) i where i not in (select visible(r, t, s - 1))) then
    raise exception using errcode = '40001', message = 'conflict: a file is no longer live';
  end if;
  update o1_file set end_seq = s where tbl = t and ref = r and id = any (ids) and end_seq = 9223372036854775807;
  insert into o1_tombstone select t, r, i, s from unnest(ids) i
   where not exists (select 1 from o1_file f where f.tbl = t and f.ref = r and f.id = i);
end $$;

create or replace function op_compact(t int, r int, old_ids bigint[], new_id bigint) returns void language plpgsql as $$
begin
  perform op_remove(t, r, old_ids);
  perform op_add(t, r, array[new_id]);
end $$;

-- Delete rows of a file: the new DV is the live one plus the new positions.
create or replace function op_dv(t int, r int, f bigint, pos bigint[]) returns void language plpgsql as $$
declare s bigint := begin_commit(t, r); prev bigint[] := coalesce(dv_of(r, t, f, s - 1), '{}');
begin
  update o1_dv set end_seq = s where tbl = t and ref = r and file = f and end_seq = 9223372036854775807;
  insert into o1_dv values (t, r, f, s, default,
    (select array_agg(distinct p order by p) from unnest(prev || pos) p));
end $$;

-- ── merge ───────────────────────────────────────────────────────────────────────────────────────────────────────

-- What b changed and what its parent changed since the fork, and where they conflict.
create or replace function conflicts(b int) returns table (tbl int, file bigint, what text) language sql stable as $$
  with r as (select parent as p, fork_seq as bif from ref where id = b),
  rem_b as (select l.tbl, l.file from o1_tombstone l where l.ref = b),
  dv_b as (select d.tbl, d.file from o1_dv d where d.ref = b and d.end_seq = 9223372036854775807),
  rem_p as (
    select f.tbl, f.id as file from o1_file f, r
     where f.ref = r.p and f.begin_seq <= r.bif and f.end_seq > r.bif and f.end_seq < 9223372036854775807
    union
    select l.tbl, l.file from o1_tombstone l, r where l.ref = r.p and l.begin_seq > r.bif),
  dv_p as (select d.tbl, d.file from o1_dv d, r where d.ref = r.p and d.begin_seq > r.bif)
  select tbl, file, 'removed in both' from (select * from rem_b intersect select * from rem_p) x
  union all
  select tbl, file, 'rows deleted in a file the other removed' from (select * from dv_b intersect select * from rem_p) x
  union all
  select tbl, file, 'removed a file in which the other deleted rows' from (select * from rem_b intersect select * from dv_p) x;
$$;

-- Merges b into its parent if there are no conflicts; returns how many there are (0 = merged). Atomic across all its tables.
create or replace function merge_branch(b int) returns int language plpgsql as $$
declare p int; s bigint; n int; t int;
begin
  select parent into p from ref where id = b;
  -- The locks on the parent's heads, in table order (no deadlocks between merges).
  perform 1 from head c
   where c.ref = p and c.tbl in (select x.tbl from o1_file x where x.ref = b
                                 union select x.tbl from o1_tombstone x where x.ref = b
                                 union select x.tbl from o1_dv x where x.ref = b)
   order by c.tbl for update;
  select count(*) into n from conflicts(b);
  if n > 0 then return n; end if;
  s := nextval('seq');
  -- The DVs: if the parent also deleted rows of that file, the union of both (deleting is idempotent).
  update o1_dv d set end_seq = s from o1_dv x
   where x.ref = b and x.end_seq = 9223372036854775807
     and d.ref = p and d.tbl = x.tbl and d.file = x.file and d.end_seq = 9223372036854775807;
  insert into o1_dv
    select x.tbl, p, x.file, s, 9223372036854775807,
           (select array_agg(distinct q order by q) from unnest(x.pos || coalesce(dv_of(p, x.tbl, x.file, s - 1), '{}')) q)
      from o1_dv x where x.ref = b and x.end_seq = 9223372036854775807;
  delete from o1_dv where ref = b;
  -- b's files move to the parent: the rows are moved, not copied (cost proportional to the diff).
  update o1_file set ref = p, begin_seq = s where ref = b and end_seq = 9223372036854775807;
  -- b's tombstones: in the parent, its own rows are closed and inherited ones are covered.
  update o1_file f set end_seq = s from o1_tombstone l
   where l.ref = b and f.ref = p and f.tbl = l.tbl and f.id = l.file and f.end_seq = 9223372036854775807;
  insert into o1_tombstone
    select l.tbl, p, l.file, s from o1_tombstone l
     where l.ref = b and not exists (select 1 from o1_file f where f.ref = p and f.tbl = l.tbl and f.id = l.file);
  delete from o1_tombstone where ref = b;
  -- b stays as an empty child of the parent at the merge commit.
  update ref set fork_seq = s where id = b;
  for t in select c.tbl from head c where c.ref = p and c.snap < s
            and c.tbl in (select x.tbl from snapshot x where x.ref = b) loop
    insert into snapshot values (t, p, s, (select snap from head where ref = p and tbl = t));
    update head set snap = s where ref = p and tbl = t;
  end loop;
  return 0;
end $$;
