#!/usr/bin/env bash
# MetaLake preliminary study 2.8: many catalogs in one Postgres. Runs inside the postgres:17 image as `postgres`.
#
#   CATALOGS=1000 DBS=20 bash tenancy_bench.sh
#
# Schema per catalog: CATALOGS schemas in one database, each with the full MetaLake schema (schema.sql + merge.sql):
# how long one catalog takes to create, what an empty catalog weighs, how many catalog objects Postgres carries, and
# whether planning a read in catalog #N is slower than in a database with a single catalog.
# Database per catalog: DBS databases with the same schema, for the per-database cost.
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd)
CATALOGS=${CATALOGS:-1000}; DBS=${DBS:-20}
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }
if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-1GB} -c max_connections=200 -c jit=off" start >/dev/null
fi
q -c "create extension if not exists btree_gist"
DDL=$(cat "$D/schema.sql" "$D/merge.sql" | grep -v "create extension")
s0=$(q -c "select pg_database_size(current_database())")
c0=$(q -c "select count(*) from pg_class")
a=$(date +%s%N)
for i in $(seq 1 "$CATALOGS"); do
  printf 'create schema c%s; set search_path = c%s, public;\n%s\n' "$i" "$i" "$DDL"
done | q >/dev/null 2>&1
ms=$(( ($(date +%s%N) - a) / 1000000 ))
s1=$(q -c "select pg_database_size(current_database())")
c1=$(q -c "select count(*) from pg_class")
r "schema per catalog" "$CATALOGS catalogs in $ms ms ($(( ms / CATALOGS )) ms each)" \
  "empty catalog $(( (s1 - s0) / CATALOGS / 1024 )) KiB" "pg_class +$(( c1 - c0 )) ($(( (c1 - c0) / CATALOGS )) per catalog)"
# The same read in catalog #CATALOGS/2 (among many) and in a fresh database with one catalog.
MID=$((CATALOGS / 2))
seed="select seed(20000, 200, 1, 200, 200, 200, false); call build_indexes(); analyze;"
q -c "set search_path = c$MID, public; $seed" >/dev/null
q -c "create database solo" && q -d solo -c "create extension btree_gist" && (printf '%s\n' "$DDL"; echo "$seed") | q -d solo >/dev/null 2>&1
cat > /tmp/read_mid.sql <<EOF
set search_path = c$MID, public;
select count(*) from visible(1, 1, (select last_value from seq));
EOF
echo "select count(*) from visible(1, 1, (select last_value from seq));" > /tmp/read_solo.sql
lat() { pgbench -n -c1 -j1 -T 5 -f "$1" "${@:2}" 2>&1 | awk -F' = ' '/latency average/ {print $2}'; }
r "read 20000-file table" "catalog #$MID of $CATALOGS: $(lat /tmp/read_mid.sql postgres)" "alone in its database: $(lat /tmp/read_solo.sql solo)"
# Database per catalog.
a=$(date +%s%N); t0=0
for i in $(seq 1 "$DBS"); do
  q -c "create database d$i" && q -d "d$i" -c "create extension btree_gist" && printf '%s\n' "$DDL" | q -d "d$i" >/dev/null 2>&1
done
ms=$(( ($(date +%s%N) - a) / 1000000 ))
per=$(q -c "select avg(pg_database_size(datname))::bigint / 1024 / 1024 from pg_database where datname like 'd%'")
r "database per catalog" "$DBS databases in $ms ms ($(( ms / DBS )) ms each)" "each $per MiB empty" \
  "plus one connection pool per database"
r "end"
