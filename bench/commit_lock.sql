\set t random(1, :tables)
select commit_lock(:t, 1, 4);
