\set t random(1, :tables)
select snap as expected from head where ref = 1 and tbl = :t \gset
select commit_cas(:t, 1, :expected, 4);
