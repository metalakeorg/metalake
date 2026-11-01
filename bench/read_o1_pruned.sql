-- o1, pruned by statistics: the files that overlap [:lo, :hi].
with recursive chain (lvl, ref, upto_s) as (
  select 0, :b, cast(:s as bigint)
  union all
  select c.lvl + 1, r.parent, r.fork_seq from chain c join ref r on r.id = c.ref where r.parent is not null),
covered as (
  select l.file, min(d.lvl) as lvl from o1_tombstone l join chain d on d.ref = l.ref and l.begin_seq <= d.upto_s
   where l.tbl = 1 group by l.file)
select f.id, f.path, f.row_count from chain c
  join o1_file f on f.tbl = 1 and f.ref = c.ref and f.begin_seq <= c.upto_s and f.end_seq > c.upto_s
   and int8range(f.min_k, f.max_k, '[]') && int8range(:lo, :hi, '[]')
  left join covered t on t.file = f.id and t.lvl < c.lvl
 where t.file is null;
