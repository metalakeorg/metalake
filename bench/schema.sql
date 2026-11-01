-- MetaLake · preliminary study 1.1 and 1.3: throwaway prototype.
--
-- Three ways to store which files a (ref, table, snapshot) sees when branches form a tree with inheritance:
--   o1 · validity range per ref with inheritance: each row lives in its ref (begin_seq, end_seq); a branch stores what
--        it adds and «tombstones» for what it removes from what it inherits. Forking is one row.
--   o2 · deltas: each commit stores what it adds ('a') and what it removes ('r'); the live set is computed by walking.
--   o3 · copy on fork (DuckLake with branches): the branch copies the parent's live rows. Reading is one level.
-- And the commit (1.3) on top of o1: CAS, head lock and global lock.

create extension if not exists btree_gist;

-- The order of the catalog's commits.
create sequence seq;
-- The ids of the files created by the commits of the concurrency benchmark.
create sequence fid start 100000000000;

-- The refs: `fork_seq` is the last seq of the parent that the branch sees.
create table ref (id int primary key, name text unique not null, parent int references ref, fork_seq bigint);

-- The head of (ref, table): the pointer row of the transactional catalog.
create table head (ref int, tbl int, snap bigint not null, primary key (ref, tbl)) with (fillfactor = 50);
create table snapshot (tbl int, ref int, id bigint, parent bigint, primary key (tbl, id));

-- Files marked by the seed, for the correctness checks.
create table marker (name text primary key, file bigint);

-- o1 · validity range per ref with inheritance
create table o1_file (
  tbl int not null, id bigint not null, ref int not null,
  begin_seq bigint not null, end_seq bigint not null default 9223372036854775807,
  row_count bigint, min_k bigint, max_k bigint, path text,
  primary key (tbl, id));
create table o1_tombstone (tbl int, ref int, file bigint, begin_seq bigint, primary key (tbl, file, ref));

-- o2 · deltas per commit
create table o2_change (
  tbl int not null, ref int not null, seq bigint not null, file bigint not null, op "char" not null,
  row_count bigint, min_k bigint, max_k bigint, path text);

-- o3 · copy on fork
create table o3_file (
  tbl int not null, ref int not null, id bigint not null,
  begin_seq bigint not null, end_seq bigint not null default 9223372036854775807,
  row_count bigint, min_k bigint, max_k bigint, path text,
  primary key (tbl, ref, id));

-- The seed: `n` files in `commits` commits on main (one in five, compacted ten commits later), `depth` chained
-- branches (each a child of the previous one) with `k` new files and `m` tombstones each, and then `post` new files
-- and `post` deletions on main, which no branch must see. With `copies = false`, only o1.
create or replace function seed(n bigint, commits int, depth int, k int, m int, post int, copies boolean default true) returns void
language plpgsql as $$
declare
  inf constant bigint := 9223372036854775807;
  d int; s bigint; bif bigint; parent int := 1;
begin
  insert into ref values (1, 'main', null, null);
  insert into o1_file (tbl, id, ref, begin_seq, end_seq, row_count, min_k, max_k, path)
    select 1, g, 1, c,
           case when g % 5 = 0 and c + 10 <= commits then c + 10 else inf end,
           1000, g * 10, g * 10 + 9, 'gs://lake/t/' || g || '.parquet'
      from generate_series(1, n) g, lateral (select ((g - 1) * commits / n + 1)::bigint as c) x;
  perform setval('seq', commits);
  insert into snapshot select 1, 1, c, nullif(c - 1, 0) from generate_series(1, commits) c;

  for d in 1..depth loop
    bif := currval('seq');
    insert into ref values (d + 1, 'branch' || d, parent, bif);
    s := nextval('seq');
    insert into snapshot values (1, d + 1, s, bif);
    insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)
      select 1, 1000000000::bigint * d + j, d + 1, s, 1000, (n / 2) * 10 + j, (n / 2) * 10 + j + 9,
             'gs://lake/t/r' || d || '-' || j || '.parquet'
        from generate_series(1, k) j;
    insert into o1_tombstone
      select 1, d + 1, g, s from (
        select distinct ((j::bigint * 104729 + d * 7919) % n) + 1 as g from generate_series(1, 3 * m) j) x
       where g % 5 <> 0 and g % 7 <> 0
       limit m;
    parent := d + 1;
  end loop;

  -- main goes on after forking: what is added and deleted here, no branch sees.
  s := nextval('seq');
  insert into snapshot values (1, 1, s, s - 1);
  insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)
    select 1, 50000000000 + j, 1, s, 1000, n * 10 + j, n * 10 + j + 9, 'gs://lake/t/p' || j || '.parquet'
      from generate_series(1, post) j;
  update o1_file set end_seq = s
   where tbl = 1 and ref = 1 and id in (
     select g from generate_series(7, n, 7) g where g % 5 <> 0 limit post);

  insert into marker values
    ('new in main after fork', 50000000001),
    ('deleted in main after fork', (select min(id) from o1_file where ref = 1 and end_seq = s)),
    ('tombstone of branch1', (select min(file) from o1_tombstone where ref = 2)),
    ('new in branch1', 1000000001);

  insert into head select r.id, 1, coalesce((select max(id) from snapshot where ref = r.id), 0) from ref r;
  insert into head select 1, t, 0 from generate_series(2, 1001) t;

  if not copies then return; end if;
  -- o2, derived from o1: what was added, what was removed in its own ref, and the tombstones.
  insert into o2_change select tbl, ref, begin_seq, id, 'a', row_count, min_k, max_k, path from o1_file;
  insert into o2_change select tbl, ref, end_seq, id, 'r', null, null, null, null from o1_file where end_seq < inf;
  insert into o2_change select tbl, ref, begin_seq, file, 'r', null, null, null, null from o1_tombstone;

  -- o3, derived from o1: main as is; each branch copies its parent's live set at the fork and applies its own changes.
  insert into o3_file select tbl, ref, id, begin_seq, end_seq, row_count, min_k, max_k, path from o1_file where ref = 1;
  for d in 1..depth loop
    select fork_seq, ref.parent into bif, parent from ref where id = d + 1;
    insert into o3_file
      select tbl, d + 1, id, begin_seq, inf, row_count, min_k, max_k, path from o3_file
       where tbl = 1 and ref = parent and begin_seq <= bif and end_seq > bif;
    insert into o3_file select tbl, ref, id, begin_seq, end_seq, row_count, min_k, max_k, path from o1_file where ref = d + 1;
    update o3_file f set end_seq = l.begin_seq from o1_tombstone l
     where l.ref = d + 1 and f.tbl = 1 and f.ref = d + 1 and f.id = l.file;
  end loop;
end $$;

-- The indexes, after seeding.
create or replace procedure build_indexes() language sql as $$
  create index o1_visible on o1_file using gist (tbl, ref, int8range(min_k, max_k, '[]'));
  create index o1_ref on o1_file (tbl, ref, begin_seq);
  -- What is closed (removed or compacted): what a merge looks at in its parent since the fork.
  create index o1_closed on o1_file (ref, end_seq) where end_seq < 9223372036854775807;
  create index o1_tombstone_ref on o1_tombstone (ref, begin_seq);
  create index o2_visible on o2_change using gist (tbl, ref, int8range(min_k, max_k, '[]')) where op = 'a';
  create index o2_add on o2_change (tbl, ref, seq) where op = 'a';
  create index o2_rem on o2_change (tbl, file, ref) where op = 'r';
  create index o3_visible on o3_file using gist (tbl, ref, int8range(min_k, max_k, '[]'));
  create index o3_ref on o3_file (tbl, ref, begin_seq);
$$;

-- ── 1.3 · the commit, on top of o1 ───────────────────────────────────────────────────────────────────────────────

-- nf new files in (t, r) at snapshot s.
create or replace function add_files(t int, r int, s bigint, nf int) returns void language sql as $$
  insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)
    select t, nextval('fid'), r, s, 1000, s * 10, s * 10 + 9, 'gs://lake/x/' || s || '-' || g || '.parquet'
      from generate_series(1, nf) g;
$$;

-- a · CAS: the writer read the head `expected` earlier; if another commit got in in between, it fails.
create or replace function commit_cas(t int, r int, expected bigint, nf int) returns boolean language plpgsql as $$
declare s bigint;
begin
  update head set snap = nextval('seq') where ref = r and tbl = t and snap = expected returning snap into s;
  if s is null then return false; end if;
  insert into snapshot values (t, r, s, expected);
  perform add_files(t, r, s, nf);
  return true;
end $$;

-- b · head lock: the commit is ordered inside the database, an `append` never conflicts.
create or replace function commit_lock(t int, r int, nf int) returns boolean language plpgsql as $$
declare s bigint; prev bigint;
begin
  select snap into prev from head where ref = r and tbl = t for update;
  s := nextval('seq');
  insert into snapshot values (t, r, s, prev);
  perform add_files(t, r, s, nf);
  update head set snap = s where ref = r and tbl = t;
  return true;
end $$;

-- c · global lock: in addition, the seq has no gaps across the whole catalog (a visible commit implies all earlier ones).
create or replace function commit_global(t int, r int, nf int) returns boolean language plpgsql as $$
begin
  perform pg_advisory_xact_lock(83);
  return commit_lock(t, r, nf);
end $$;

-- d · delete: conflicts if some file is no longer live (another commit deleted or compacted it first).
create or replace function commit_delete(t int, r int, ids bigint[]) returns boolean language plpgsql as $$
declare s bigint; prev bigint; n int;
begin
  select snap into prev from head where ref = r and tbl = t for update;
  s := nextval('seq');
  update o1_file set end_seq = s where tbl = t and ref = r and id = any (ids) and end_seq = 9223372036854775807;
  get diagnostics n = row_count;
  if n < cardinality(ids) then
    raise exception using errcode = '40001', message = 'conflict: a file is no longer live';
  end if;
  insert into snapshot values (t, r, s, prev);
  update head set snap = s where ref = r and tbl = t;
  return true;
end $$;
