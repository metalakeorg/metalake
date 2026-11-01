-- o1: the live set of ref :b at :s walking its ancestors; the tombstones that apply are computed once (the highest level that covers each file).
with recursive chain (lvl, ref, upto_s) as (
  select 0, :b, cast(:s as bigint)
  union all
  select c.lvl + 1, r.parent, r.fork_seq from chain c join ref r on r.id = c.ref where r.parent is not null),
covered as (
  select l.file, min(d.lvl) as lvl from o1_tombstone l join chain d on d.ref = l.ref and l.begin_seq <= d.upto_s
   where l.tbl = 1 group by l.file)
select count(*), sum(f.row_count), bit_xor(f.id) from chain c
  join o1_file f on f.tbl = 1 and f.ref = c.ref and f.begin_seq <= c.upto_s and f.end_seq > c.upto_s
  left join covered t on t.file = f.id and t.lvl < c.lvl
 where t.file is null;
