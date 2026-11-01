#!/usr/bin/env bash
# MetaLake · preliminary study 1.2: what merging costs, o1 only. A table of N files on main, branches with diffs of
# 1,000, 10,000 and 100,000 new files and as many tombstones, and 1,000 changes on main after forking.
# Runs inside the postgres:17 image, as the postgres user.
#
#   N=1000000 bash merge_bench.sh
#
# Environment: N (files on main, default 1000000), SB (shared_buffers, default 6GB), ECS (effective_cache_size,
# default 18GB), PGDATA (default /tmp/pg).
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd)
N=${N:-1000000}
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }
ms() { local a; a=$(date +%s%N); "$@" >/dev/null; echo "$(( ($(date +%s%N) - a) / 1000000 )) ms"; }
if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-6GB} -c work_mem=64MB \
    -c maintenance_work_mem=2GB -c max_wal_size=16GB -c effective_cache_size=${ECS:-18GB} -c jit=off" start >/dev/null
fi
q -f "$D/schema.sql" -f "$D/merge.sql"
r "seed" "$(ms q -c "select seed($N, $((N / 100)), 1, 1000, 1000, 1000, false)")" "build_indexes $(ms q -c "call build_indexes()" -c "vacuum analyze")"
S=$(q -c "select last_value from seq")
r "live" "main=$(q -c "select count(*) from visible(1, 1, $S)")" "branch=$(q -c "select count(*) from visible(2, 1, $S)")"
# Bigger branches, off main: k new files and k tombstones over the live set.
for k in 10000 100000; do
  b=$(q -c "select branch('d$k', 1)")
  q -c "select op_add(1, $b, array(select 60000000000 + $k * 10 + g from generate_series(1, $k) g)::bigint[])" >/dev/null
  q -c "select op_remove(1, $b, array(select g from generate_series($((k / 10000 + 2)), $N, 11) g where g % 5 <> 0 and g % 7 <> 0 and g not in (select file from o1_tombstone) limit $k)::bigint[])" >/dev/null
done
q -c "vacuum analyze"
for b in $(q -c "select id from ref where id > 1 order by id"); do
  d=$(q -c "select (select count(*) from o1_file where ref = $b) || '+' || (select count(*) from o1_tombstone where ref = $b)")
  c=$(ms q -c "select count(*) from conflicts($b)")
  a=$(date +%s%N); v=$(q -c "select merge_branch($b)"); f="$(( ($(date +%s%N) - a) / 1000000 )) ms (returns $v)"
  ok=$(q -c "select count(*) from visible($b, 1, (select last_value from seq))")
  r "merge" "branch=$b diff=$d" "conflicts=$c" "merge=$f" "live in main after merging=$(q -c "select count(*) from visible(1, 1, (select last_value from seq))") (branch=$ok)"
done
# And one that conflicts: detect without applying.
b=$(q -c "select branch('clash', 1)")
q -c "select op_remove(1, $b, array[1]::bigint[])" -c "select op_remove(1, 1, array[1]::bigint[])" >/dev/null
r "clash" "conflicts=$(ms q -c "select count(*) from conflicts($b)")" "merge_branch returns $(q -c "select merge_branch($b)")"
r "end"
