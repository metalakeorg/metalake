\set f random(1, :pool)
select commit_delete(1, 1, array[:f * 35 + 1]::bigint[]);
