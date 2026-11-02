-- S12 · one commit of 4 files to a random bench table between :first and :last, with only the statements the format
-- requires (bench_append, in v0.sh), in one round trip.
\set n random(:first, :last)
\set k random(1, 100000000)
select bench_append(:n, :k);
