#!/usr/bin/env bash
# MetaLake · where the protocol's commit and merge spend their time. Runs like g1.sh (staged by g1-stage.sh), with
# pg_stat_statements tracking the statements inside the protocol's functions. Times on a laptop are only relative.
#
#   DUR=10 TABLES=1000 bash p1.sh
#
# Prints the statements by total time: first under commits of data alone, then under agents that branch, commit
# and merge, one after another.
set -euo pipefail
export LC_ALL=C
D=$(cd "$(dirname "$0")" && pwd)
DUR=${DUR:-10}; TABLES=${TABLES:-1000}; CLIENTS=${CLIENTS:-8}; TOP=${TOP:-20}
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }

initdb -U postgres -A trust >/dev/null
pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-2GB} -c max_connections=200 \
  -c jit=off -c track_commit_timestamp=on -c shared_preload_libraries=pg_stat_statements \
  -c pg_stat_statements.track=all -c pg_stat_statements.max=10000" start >/dev/null
{
  echo "create schema lake; set search_path = lake;"
  for f in "$D"/s-*.sql "$D"/u-*.sql; do cat "$f"; echo; done
  for f in "$D"/p-*.sql; do sed 's/@schema@/lake/g' "$f"; echo; done
} | q
q -c "create extension pg_stat_statements schema public"
export PGOPTIONS='-c search_path=lake,public'
q -v tables="$TABLES" -f "$D/g1_setup.sql" >/dev/null

tag() { sed 's/^/result\t/'; }
top() {
  printf 'result\t== %s\n' "$1"
  q -c "select 'executed ' || round(sum(total_exec_time)::numeric / 1000, 1) || ' s in ' || sum(calls) || ' top-level calls'
          from public.pg_stat_statements where toplevel" | tag
  q -F $'\t' -c "select round(total_exec_time::numeric, 0) as ms, calls,
                        round((total_exec_time / greatest(calls, 1))::numeric, 3) as ms_per_call,
                        regexp_replace(left(query, 150), '\s+', ' ', 'g')
                   from public.pg_stat_statements where dbid = (select oid from pg_database where datname = 'postgres')
                  order by total_exec_time desc limit $TOP" | tag
}
cd "$(mktemp -d)"
q -c "select public.pg_stat_statements_reset()" >/dev/null
pgbench -n -M prepared -c "$CLIENTS" -j "$CLIENTS" -T "$DUR" -D tables="$TABLES" -f "$D/g1_data.sql" | grep -E "^tps|latency average" | tag
top "commits of data"
q -c "select public.pg_stat_statements_reset()" >/dev/null
pgbench -n -M prepared -c 1 -T "$DUR" -D tables="$TABLES" -f "$D/g1_merge.sql" | grep -E "^tps|latency average" | tag
top "branch, commit and merge, one client"
