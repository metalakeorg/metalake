#!/usr/bin/env bash
# MetaLake · revision 0.1, G1 (ADR 0002 §21.4): what the exclusive lock on a ref costs the commits of that ref.
# Runs inside the postgres:17 image, as the postgres user, with the protocol of a catalog staged next to it:
# `s-*.sql` (spec/v0/schema), `u-*.sql` (the reference files the protocol reuses, in order) and `p-*.sql` (sql/v0).
#
#   DUR=3 TABLES=50 bash g1.sh                 # locally, for correctness only
#
# Commits of data go to TABLES tables of main from CLIENTS clients, unthrottled, for DUR seconds, in five phases:
# alone; with agents that branch main, commit on the branch and merge it back; with tables created and dropped on
# main; with branches created and deleted; with all three. Each side load runs at RATE per second from 2 clients.
# Each result is one `result<TAB>...` line: commits/s and latency p50, p99 and max of the commits of data, and the
# rate and p99 of each side load.
set -euo pipefail
export LC_ALL=C
D=$(cd "$(dirname "$0")" && pwd)
DUR=${DUR:-30}; TABLES=${TABLES:-1000}; CLIENTS=${CLIENTS:-8}; RATE=${RATE:-10}  # PHASES selects phases
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }

if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-2GB} -c work_mem=64MB \
    -c max_wal_size=16GB -c effective_cache_size=${ECS:-8GB} -c max_connections=200 -c random_page_cost=1.1 \
    -c synchronous_commit=on -c jit=off -c track_commit_timestamp=on" start >/dev/null
fi
r "postgres" "$(q -c 'show server_version')" "cpus=$(nproc)" "tables=$TABLES clients=$CLIENTS rate=$RATE dur=$DUR"

{
  echo "create schema lake; set search_path = lake;"
  for f in "$D"/s-*.sql "$D"/u-*.sql; do cat "$f"; echo; done
  for f in "$D"/p-*.sql; do sed 's/@schema@/lake/g' "$f"; echo; done
} | q
export PGOPTIONS='-c search_path=lake'
q -v tables="$TABLES" -f "$D/g1_setup.sql"

W=$(mktemp -d); cd "$W"
pct() { sort -n | awk '{a[NR] = $1} END {if (NR) printf "%.2f %.2f %.2f\n", a[int(NR * 0.5) + 1] / 1000, a[int(NR * 0.99) + 1] / 1000, a[NR] / 1000}'; }
failed() { grep -oP 'number of failed transactions: \K[0-9]+' "$1" || echo "?"; }
phase() {
  local name=$1 s; shift
  rm -f l-* *.out
  pgbench -n -M prepared -c "$CLIENTS" -j "$CLIENTS" -T "$DUR" -D tables="$TABLES" -f "$D/g1_data.sql" \
    -l --log-prefix=l-data > data.out 2>&1 &
  for s in "$@"; do
    pgbench -n -M prepared -c 2 -j 2 -T "$DUR" -R "$RATE" -D tables="$TABLES" -f "$D/g1_$s.sql" \
      -l --log-prefix="l-$s" > "$s.out" 2>&1 &
  done
  wait
  if ! ls l-data.* >/dev/null 2>&1; then
    r "phase=$name failed"; cat ./*.out; exit 1
  fi
  read -r p50 p99 max < <(cat l-data.* | awk '{print $3}' | pct)
  local line="phase=$name data_tps=$(grep -oP 'tps = \K[0-9.]+' data.out | head -1) p50_ms=$p50 p99_ms=$p99 max_ms=$max"
  for s in "$@"; do
    read -r _ sp99 _ < <(cat l-"$s".* | awk '{print $3}' | pct)
    line+=" ${s}_tps=$(grep -oP 'tps = \K[0-9.]+' "$s.out" | head -1) ${s}_p99_ms=$sp99"
    line+=" ${s}_failed=$(failed "$s.out")"
  done
  line+=" data_failed=$(failed data.out)"
  r "$line"
}
PHASES=${PHASES:-alone merges ddl branches all}
for name in $PHASES; do
  case $name in
    alone) phase alone ;;
    merges) phase merges merge ;;
    ddl) phase ddl ddl ;;
    branches) phase branches ref ;;
    all) phase all merge ddl ref ;;
  esac
done
r "checked" "$(q -c "select 'refs=' || count(*) || ' commits=' || (select count(*) from commit) from ref")" \
  "errors=$(q -c "select count(*) from generate_series(1, $TABLES) n, lateral table_errors(1, bench_id(n), 9223372036854775806) e")"
