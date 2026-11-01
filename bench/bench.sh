#!/usr/bin/env bash
# MetaLake · preliminary study 1.1 and 1.3: the benchmark. Runs inside the postgres:17 image, as the postgres user.
#
#   N=1000000 DEPTH=5 bash bench.sh          # locally, at small scale: N=20000
#
# Starts its own Postgres in $PGDATA, seeds, checks that the three options see the same thing, measures planning
# (1.1) and concurrent commits (1.3). Each result is one `result<TAB>...` line.
#
# Environment:
#   N        files on main (default 1000000)            COMMITS  commits on main (default N/100)
#   DEPTH    chained branches (default 5)               K        new files per branch (default 1000)
#   M        tombstones per branch (default 1000)       POST     files added and deleted on main after forking (1000)
#   DUR      seconds per pgbench run (default 10)       CLIENTS  pgbench client counts (default "1 8 32 64")
#   SB       shared_buffers (default 8GB)               ECS      effective_cache_size (default 20GB)
#   PGDATA   data directory (default /tmp/pg)
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd)
N=${N:-1000000}; COMMITS=${COMMITS:-$((N / 100))}; DEPTH=${DEPTH:-5}; K=${K:-1000}; M=${M:-1000}; POST=${POST:-1000}
DUR=${DUR:-10}; CLIENTS=${CLIENTS:-1 8 32 64}
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
ms() { local a; a=$(date +%s%N); "$@" >/dev/null; echo "$(( ($(date +%s%N) - a) / 1000000 )) ms"; }
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }

if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-8GB} -c work_mem=64MB \
    -c maintenance_work_mem=2GB -c max_wal_size=16GB -c effective_cache_size=${ECS:-20GB} -c max_connections=200 \
    -c random_page_cost=1.1 -c synchronous_commit=on -c jit=off" start >/dev/null
fi
r "postgres" "$(q -c 'show server_version')" "cpus=$(nproc)" "N=$N commits=$COMMITS depth=$DEPTH k=$K m=$M post=$POST"

q -f "$D/schema.sql"
r "seed" "$(ms q -c "select seed($N, $COMMITS, $DEPTH, $K, $M, $POST)")" "build_indexes+analyze $(ms q -c "call build_indexes()" -c "vacuum analyze")"
r "size" "$(q -c "select string_agg(t || '=' || pg_size_pretty(pg_total_relation_size(t::regclass)), ' ') from unnest(array['o1_file','o1_tombstone','o2_change','o3_file']) t")"
r "rows" "$(q -c "select 'o1=' || (select count(*) from o1_file) || ' o2=' || (select count(*) from o2_change) || ' o3=' || (select count(*) from o3_file)")"
S=$(q -c "select last_value from seq")

# ── correctness: the three options see the same thing in each ref, and what they must see ──────────────────────────
failures=0
for b in $(seq 1 $((DEPTH + 1))); do
  a1=$(q -v b=$b -v s=$S -f "$D/read_o1_full.sql"); a2=$(q -v b=$b -v s=$S -f "$D/read_o2_full.sql")
  a3=$(q -v b=$b -v s=$S -f "$D/read_o3_full.sql")
  if [ "$a1" = "$a2" ] && [ "$a1" = "$a3" ]; then r "same" "ref=$b" "$a1"; else r "DIFFERENT" "ref=$b" "o1=$a1" "o2=$a2" "o3=$a3"; failures=$((failures + 1)); fi
done
while IFS='|' read -r name id lo hi; do
  for b in $(seq 1 $((DEPTH + 1))); do
    case "$name" in
      "new in main after fork"|"tombstone of branch1") [ $b = 1 ] && expect=yes || expect=no ;;
      *) [ $b = 1 ] && expect=no || expect=yes ;;
    esac
    for o in 1 2 3; do
      if q -v b=$b -v s=$S -v lo=$lo -v hi=$hi -f "$D/read_o${o}_pruned.sql" | cut -d'|' -f1 | grep -qx "$id"; then sees=yes; else sees=no; fi
      [ $sees = $expect ] || { r "WRONG" "o$o ref=$b '$name' sees=$sees expected=$expect"; failures=$((failures + 1)); }
    done
  done
done < <(q -c "select m.name, m.file, f.min_k, f.max_k from marker m join o1_file f on f.tbl = 1 and f.id = m.file")
r "correctness" "failures=$failures"

# ── 1.1 · planning: the latency of reading each ref, full and pruned (0.1 % of the keys) ────────────────────────────
LO=$((N / 4 * 10)); HI=$((LO + N / 100))
lat() { pgbench -n -c1 -j1 -T "$DUR" -f "$1" "${@:2}" 2>&1 | awk -F' = ' '/latency average/ {print $2}'; }
for b in 1 2 $(( (DEPTH + 2) / 2 )) $((DEPTH + 1)); do
  for o in 1 2 3; do
    t=$(lat "$D/read_o${o}_full.sql" -D b=$b -D s=$S)
    p=$(lat "$D/read_o${o}_pruned.sql" -D b=$b -D s=$S -D lo=$LO -D hi=$HI)
    n=$(q -v b=$b -v s=$S -v lo=$LO -v hi=$HI -f "$D/read_o${o}_pruned.sql" | wc -l)
    r "read" "o$o" "ref=$b (level $((b - 1)))" "full=$t" "pruned=$p ($n files)"
  done
done

# ── 1.1 · fork: a new branch off main ────────────────────────────────────────────────────────────────────────────────
t1=$(ms q -c "insert into ref values (900, 'new1', 1, $S)")
t3=$(ms q -c "insert into ref values (901, 'new3', 1, $S); insert into o3_file select tbl, 901, id, begin_seq, 9223372036854775807, row_count, min_k, max_k, path from o3_file where tbl = 1 and ref = 1 and begin_seq <= $S and end_seq > $S")
r "fork" "o1/o2=$t1" "o3=$t3"

# ── 1.3 · concurrent commits of 4 files ─────────────────────────────────────────────────────────────────────────────
commits() { # <mode> <tables> <clients>
  local before; before=$(q -c "select last_value from seq")
  local out; out=$(pgbench -n -c "$3" -j "$(( $3 < 4 ? $3 : 4 ))" -T "$DUR" -D tables="$2" --max-tries=1 -f "$D/commit_$1.sql" 2>&1)
  local done_; done_=$(q -c "select count(*) from snapshot where id > $before")
  r "commit" "$1" "tables=$2" "clients=$3" "attempts/s=$(awk -F' = ' '/^tps/ {print $2}' <<<"$out" | cut -d' ' -f1)" \
    "lat=$(awk -F' = ' '/latency average/ {print $2}' <<<"$out")" "committed=$done_ of $(awk -F': ' '/actually processed/ {print $2}' <<<"$out" | cut -d/ -f1)"
}
for c in $CLIENTS; do commits cas 1 "$c"; done
for c in $CLIENTS; do commits lock 1 "$c"; done
for c in $CLIENTS; do commits lock 1000 "$c"; done
for c in $CLIENTS; do commits global 1000 "$c"; done

# ── 1.3 · a real conflict: two deletes of the same file ─────────────────────────────────────────────────────────────
out=$(pgbench -n -c 8 -j 4 -t 50 -D pool=50 --max-tries=1 --failures-detailed -f "$D/commit_delete.sql" 2>&1 || true)
r "delete" "8 clients x 50 over 50 files" "$(grep -E 'actually processed|failed|serialization' <<<"$out" | tr '\n' ' ')"
r "end"
