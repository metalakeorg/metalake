#!/usr/bin/env bash
# MetaLake · preliminary study 1.4 + 2.5: how much space MetaLake takes and how it plans at scale, and how to store the
# statistics. o1 only. A table of N files (10⁷ on a GKE test cluster) and, separately, NS files (10⁶) × 20 columns of
# statistics in three shapes. Size is measured here and extrapolated to 10⁸ (it grows linearly); latency, at 10⁶ and 10⁷.
# Runs inside the postgres:17 image, as the postgres user.
#
#   N=10000000 NS=1000000 bash scale_bench.sh
#
# Environment: N (files, default 10000000), NS (files with statistics, default 1000000), DUR (seconds per pgbench
# run, default 5), SB (shared_buffers, default 6GB), ECS (effective_cache_size, default 18GB), PGDATA (default /tmp/pg).
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd)
N=${N:-10000000}; NS=${NS:-1000000}; DUR=${DUR:-5}
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }
ms() { local a; a=$(date +%s%N); "$@" >/dev/null; echo "$(( ($(date +%s%N) - a) / 1000000 )) ms"; }
lat() { pgbench -n -c1 -j1 -T "$DUR" -f "$1" "${@:2}" 2>&1 | awk -F' = ' '/latency average/ {print $2}'; }
size() { q -c "select pg_size_pretty(pg_total_relation_size('$1')) || ' (' || round(pg_total_relation_size('$1')::numeric / greatest((select count(*) from $1), 1)) || ' B/row with indexes; ' || round(pg_relation_size('$1')::numeric / greatest((select count(*) from $1), 1)) || ' B/row the table)'"; }
indexes() { q -c "select string_agg(indexrelid::regclass || '=' || pg_size_pretty(pg_relation_size(indexrelid)), ' ') from pg_index where indrelid = '$1'::regclass"; }
if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-6GB} -c work_mem=64MB \
    -c maintenance_work_mem=2GB -c max_wal_size=16GB -c effective_cache_size=${ECS:-18GB} -c jit=off" start >/dev/null
fi
r "postgres" "$(q -c 'show server_version')" "cpus=$(nproc)" "N=$N NS=$NS"
q -f "$D/schema.sql" -f "$D/stats.sql"

# ── 1.4 · the files table at scale ──────────────────────────────────────────────────────────────────────────────────
r "seed" "$N files: $(ms q -c "select seed($N, $((N / 100)), 1, 1000, 1000, 1000, false)")" \
  "build_indexes $(ms q -c "call build_indexes()" -c "vacuum analyze o1_file" -c "vacuum analyze o1_tombstone")"
r "size o1_file" "$(q -c "select count(*) from o1_file") rows · $(size o1_file)"
r "indexes o1_file" "$(indexes o1_file)"
S=$(q -c "select last_value from seq")
LO=$((N / 4 * 10)); HI=$((LO + N / 100))
for b in 1 2; do
  n=$(q -v b=$b -v s=$S -v lo=$LO -v hi=$HI -f "$D/read_o1_pruned.sql" | wc -l)
  n2=$(q -v b=$b -v s=$S -v lo=$LO -v hi=$((LO + 10000)) -f "$D/read_o1_pruned.sql" | wc -l)
  r "read" "ref=$b" "pruned=$(lat "$D/read_o1_pruned.sql" -D b=$b -D s=$S -D lo=$LO -D hi=$HI) ($n files)" \
    "pruned=$(lat "$D/read_o1_pruned.sql" -D b=$b -D s=$S -D lo=$LO -D hi=$((LO + 10000))) ($n2 files)" \
    "full=$(lat "$D/read_o1_full.sql" -D b=$b -D s=$S)"
done
# Writing at scale: commits of 4 files over 1,000 tables, and the WAL each one leaves.
w0=$(q -c "select pg_current_wal_lsn()"); a=$(q -c "select last_value from seq")
out=$(pgbench -n -c 8 -j 4 -T "$((DUR * 2))" -D tables=1000 -f "$D/commit_lock.sql" 2>&1)
done_=$(q -c "select count(*) from snapshot where id > $a")
r "commit" "8 clients, 1,000 tables" "$(awk -F' = ' '/^tps/ {print $2}' <<<"$out" | cut -d' ' -f1)/s" \
  "lat=$(awk -F' = ' '/latency average/ {print $2}' <<<"$out")" \
  "WAL per commit=$(q -c "select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '$w0') / greatest($done_, 1))")"
# The physical backup (what e.g. CloudNativePG with barman does), timed: to standard output counting bytes, without writing it.
a=$(date +%s%N); bytes=$(pg_basebackup -h /tmp -D - -Ft -X fetch -c fast | wc -c)
r "database" "$(q -c "select pg_size_pretty(pg_database_size('postgres'))")" \
  "pg_basebackup=$(( ($(date +%s%N) - a) / 1000000 )) ms" "backup=$((bytes / 1024 / 1024)) MiB"
q -c "drop table o1_file, o1_tombstone, o2_change, o3_file cascade" -c "checkpoint"

# ── 2.5 · the statistics: NS files × 20 columns ─────────────────────────────────────────────────────────────────────
r "stats" "$NS files × 20 columns: $(ms q -c "call seed_stats($NS)")"
for t in st_row st_json st_arr; do r "size $t" "$(size $t)" "indexes: $(indexes $t)"; done
LO=$((NS / 2 * 10)); HI=$((LO + NS / 100))
for e in 1 2 3; do
  r "prune e$e" \
    "c3 indexed=$(lat "$D/stats_e${e}_c3.sql" -D lo=$LO -D hi=$HI) ($(q -v lo=$LO -v hi=$HI -f "$D/stats_e${e}_c3.sql" | wc -l))" \
    "c7 integer=$(lat "$D/stats_e${e}_c7.sql") ($(q -f "$D/stats_e${e}_c7.sql" | wc -l))" \
    "c15 text=$(lat "$D/stats_e${e}_s15.sql") ($(q -f "$D/stats_e${e}_s15.sql" | wc -l))" \
    "all 20 of 800 files=$(lat "$D/stats_e${e}_all.sql" -D lo=$LO) ($(q -v lo=$LO -f "$D/stats_e${e}_all.sql" | wc -l) rows)"
done
r "end"
