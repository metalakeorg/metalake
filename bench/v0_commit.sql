-- S12 · one reference commit of 4 files to a random bench table between :first and :last.
\set n random(:first, :last)
select bench_commit(1, :n, 4);
