\set n random(1, :tables)
select bench_branch() as b \gset
select bench_append(:b, :n);
select (merge(branch => :b)).commit_seq;
