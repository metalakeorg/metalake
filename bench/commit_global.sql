\set t random(1, :tables)
select commit_global(:t, 1, 4);
