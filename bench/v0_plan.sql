-- S12 · the files of the large table visible at (:r, LATEST) whose first bound overlaps [:lo, :hi]: the index the v0
-- DDL recommends (data_file_bound) finds the candidates by key range, and the bounds themselves are rechecked.
with recursive c (level, ref_id, seq) as (
  select 0, :r, 9223372036854775806::bigint
  union all
  select c.level + 1, b.parent_ref_id, b.fork_seq
    from c join ref_base b on b.ref_id = c.ref_id and b.begin_seq <= c.seq and c.seq < b.end_seq),
hidden as (
  select x.file_id, min(c.level) as level
    from tombstone x join c on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   where x.table_id = '00000000-0000-0000-0000-000000000000'
   group by x.file_id)
select f.file_id, f.path, f.record_count
  from c
  join data_file f on f.table_id = '00000000-0000-0000-0000-000000000000' and f.ref_id = c.ref_id
   and f.begin_seq <= c.seq and c.seq < f.end_seq
   and bound_range(f.lower[1], f.upper[1]) && bound_range(encode_long(:lo), encode_long(:hi))
   and coalesce(f.lower[1] <= encode_long(:hi), true) and coalesce(f.upper[1] >= encode_long(:lo), true)
  left join hidden h on h.file_id = f.file_id and h.level < c.level
 where h.file_id is null;
