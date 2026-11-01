-- o2, pruned by statistics.
with recursive chain (lvl, ref, upto_s) as (
  select 0, :b, cast(:s as bigint)
  union all
  select c.lvl + 1, r.parent, r.fork_seq from chain c join ref r on r.id = c.ref where r.parent is not null)
select f.file, f.path, f.row_count from chain c
  join o2_change f on f.tbl = 1 and f.ref = c.ref and f.op = 'a' and f.seq <= c.upto_s
   and int8range(f.min_k, f.max_k, '[]') && int8range(:lo, :hi, '[]')
 where not exists (
   select 1 from o2_change r join chain d on d.ref = r.ref and d.lvl <= c.lvl and r.seq <= d.upto_s
    where r.tbl = 1 and r.op = 'r' and r.file = f.file);
