-- o3, pruned by statistics.
select f.id, f.path, f.row_count from o3_file f
 where f.tbl = 1 and f.ref = :b and f.begin_seq <= cast(:s as bigint) and f.end_seq > cast(:s as bigint)
   and int8range(f.min_k, f.max_k, '[]') && int8range(:lo, :hi, '[]');
