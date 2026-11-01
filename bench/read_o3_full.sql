-- o3: a single level, like DuckLake.
select count(*), sum(f.row_count), bit_xor(f.id) from o3_file f
 where f.tbl = 1 and f.ref = :b and f.begin_seq <= cast(:s as bigint) and f.end_seq > cast(:s as bigint);
