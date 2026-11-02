#!/usr/bin/env bash
# MetaLake · spec v0 S12: the measurement gate. Runs inside the postgres:17 image, as the postgres user, with the
# reference DDL and behaviour of `spec/v0/` copied next to it as `s-*.sql` (schema) and `r-*.sql` (reference).
#
#   N=20000 bash v0.sh                        # locally, at small scale
#
# Measures the v0 catalog against the targets of ADR 0001 §13.18:
#   - the format: pruned planning at N files on main and on a branch DEPTH levels deep, with the index the v0 DDL
#     recommends (`data_file_bound`), each answer checked against the reference `prune()`;
#   - commits and merges, each through the reference (`commit()`, `merge()`) and through the statements the format
#     requires (`v0_commit_lean.sql`, `v0_merge_lean.sql`), whose results the reference then checks.
# Each result is one `result<TAB>...` line.
#
# Environment:
#   N        files on main of the large table (default 10000000)    DEPTH   chained branches (default 5)
#   K        files and tombstones per branch (default 1000)         DUR     seconds per pgbench run (default 10)
#   TABLES   tables for the commit runs (default 1000)              MERGE   files a merged branch adds (default 10000)
#   SB, ECS  shared_buffers, effective_cache_size (8GB, 20GB)       PGDATA  data directory (default /tmp/pg)
#   GATE     1: only what decides the gate: no reference commits or merges, nothing written to the large table
set -euo pipefail
export LC_ALL=C
D=$(cd "$(dirname "$0")" && pwd)
N=${N:-10000000}; DEPTH=${DEPTH:-5}; K=${K:-1000}; DUR=${DUR:-10}; TABLES=${TABLES:-1000}; MERGE=${MERGE:-10000}
GATE=${GATE:-0}; KINDS="reference lean"; [ "$GATE" = 1 ] && KINDS=lean
export PGDATA=${PGDATA:-/tmp/pg} PGUSER=postgres PGHOST=/tmp PGDATABASE=postgres
ms() { local a; a=$(date +%s%N); "$@" >/dev/null; echo "$(( ($(date +%s%N) - a) / 1000000 )) ms"; }
r() { printf 'result\t%s\n' "$*"; }
q() { psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }

if ! pg_isready -q -h /tmp; then
  initdb -U postgres -A trust >/dev/null
  pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c shared_buffers=${SB:-8GB} -c work_mem=64MB \
    -c maintenance_work_mem=2GB -c max_wal_size=16GB -c effective_cache_size=${ECS:-20GB} -c max_connections=200 \
    -c random_page_cost=1.1 -c synchronous_commit=on -c jit=off -c track_commit_timestamp=on" start >/dev/null
fi
r "postgres" "$(q -c 'show server_version')" "cpus=$(nproc)" "N=$N depth=$DEPTH k=$K tables=$TABLES merge=$MERGE"

for f in "$D"/s-*.sql "$D"/r-*.sql; do q -f "$f"; done
q <<'SQL'
-- The id of bench table n; table 0 is the large one.
create function bench_id(n int) returns uuid language sql immutable as $$
  select ('00000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid;
$$;

-- Creates bench table n with one long column `k`, its cluster key.
create function bench_table(n int) returns bigint language sql as $$
  select commit(jsonb_build_object('ref', 1, 'author', 'bench', 'actor', 'bench', 'operation', 'append',
    'tables', jsonb_build_array(jsonb_build_object('table_id', bench_id(n),
      'create', jsonb_build_object('namespace', '["bench"]'::jsonb, 'name', 't' || n, 'location', 'gs://bench/t' || n),
      'fields', '[{"field_id": 1, "new": true, "parent_field_id": null, "position": 1, "name": "k", "type": "long",
                   "required": true}]'::jsonb,
      'cluster_key', '[1]'::jsonb))));
$$;

-- The "add" list of `files` files with keys from k, ten per file.
create function bench_files(files int, k bigint) returns jsonb language sql volatile as $$
  select jsonb_agg(jsonb_build_object('path', 'gs://bench/' || gen_random_uuid() || '.parquet',
    'spec_id', 0, 'partition', '[]'::jsonb, 'record_count', 1000, 'file_size', 1000000, 'footer_size', 10000,
    'stat_field_ids', '[1]'::jsonb, 'lower', jsonb_build_array(encode(encode_long(k + i * 10), 'hex')),
    'upper', jsonb_build_array(encode(encode_long(k + i * 10 + 9), 'hex')),
    'null_count', '[0]'::jsonb, 'nan_count', '[null]'::jsonb))
    from generate_series(0, files - 1) i;
$$;

-- A commit of `files` new files, and of the removal of `remove`, to bench table n on ref r.
create function bench_commit(r int, n int, files int, remove bigint[] default '{}') returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'bench', 'actor', 'bench', 'operation', 'append',
    'tables', jsonb_build_array(jsonb_build_object('table_id', bench_id(n),
      'add', coalesce(bench_files(files, (random() * 1e8)::bigint), '[]'), 'remove', to_jsonb(remove)))));
$$;

-- What the format requires of a commit that appends 4 files with keys from k to bench table n on main: the locks of
-- CMT-4, the cluster key the added files' statistics are checked against (STA-3), and the rows of CMT-3, CMT-8 and
-- CMT-1, in one round trip. The writer checks its files in memory; the reference finds the result valid afterwards.
create function bench_append(n int, k bigint) returns bigint language plpgsql as $$
declare
  t uuid := bench_id(n);
  s bigint;
begin
  perform 1 from ref where ref_id = 1 for key share;
  perform 1 from head where ref_id = 1 and table_id = t for update;
  perform field_ids from cluster_key where table_id = t and ref_id = 1 and end_seq = 9223372036854775807;
  s := nextval('commit_seq_gen');
  insert into commit values (s, 1, pg_current_xact_id(), null, 'bench', 'bench', 'append', null, null, null, null);
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                         footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  select t, nextval('file_id_gen'), 1, s, 'gs://bench/' || gen_random_uuid() || '.parquet', 0, '[]', 1000, 1000000,
         10000, '{1}', array[encode_long(k + i * 10)], array[encode_long(k + i * 10 + 9)], '{0}', '{NULL}'
    from generate_series(0, 3) i;
  insert into snapshot
  select h.table_id, s, 1, h.snapshot_id, x.schema_seq, x.spec_id, x.sort_order_id,
         '{"operation": "append", "added-data-files": 4, "deleted-data-files": 0, "added-records": 4000,
           "deleted-records": 0}'
    from head h join snapshot x on x.table_id = h.table_id and x.snapshot_id = h.snapshot_id
   where h.ref_id = 1 and h.table_id = t;
  update head set snapshot_id = s where ref_id = 1 and table_id = t;
  return s;
end $$;
SQL

# ── seed: the large table, N files on main, then DEPTH chained branches, then changes on main after the forks ──────
q -c "select bench_table(0)" >/dev/null
seed() {
  q -v n="$N" -v depth="$DEPTH" -v k="$K" <<'SQL'
set session_replication_role = replica;
alter table data_file drop constraint data_file_pkey;
drop index data_file_live, data_file_ref, data_file_bound;
create table bench_seq as select (select last_value from commit_seq_gen) as base;
select setval('commit_seq_gen', (select base from bench_seq) + :n / 1000 + 1);
-- file i holds keys [10 i, 10 i + 9], and 1000 files share a commit.
insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                       footer_size, stat_field_ids, lower, upper, null_count, nan_count)
select bench_id(0), i, 1, (select base from bench_seq) + 1 + i / 1000, 'gs://bench/t0/' || i || '.parquet', 0, '[]',
       1000, 1000000, 10000, '{1}', array[encode_long(i * 10)], array[encode_long(i * 10 + 9)], '{0}', '{NULL}'
  from generate_series(1, :n) i;
select setval('file_id_gen', :n + 1);
-- Branch d forks from branch d - 1 (main for d = 1), adds k files, half of them in the planning window, and hides
-- k files of main, among them those of the window whose id ends in d.
do $$
declare
  n bigint := current_setting('bench.n')::bigint;
  w bigint := n / 4;
  p int := 1;
  b int;
  s bigint;
begin
  for d in 1 .. current_setting('bench.depth')::int loop
    b := create_ref('b' || d, p);
    s := nextval('commit_seq_gen');
    insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                           footer_size, stat_field_ids, lower, upper, null_count, nan_count)
    select bench_id(0), f, b, s, 'gs://bench/t0/' || f || '.parquet', 0, '[]', 1000, 1000000, 10000, '{1}',
           array[encode_long(key)], array[encode_long(key + 9)], '{0}', '{NULL}'
      from (select nextval('file_id_gen') as f,
                   case when i % 2 = 0 then w * 10 + (random() * 8000)::bigint else (random() * n * 10)::bigint end as key
              from generate_series(1, current_setting('bench.k')::int) i) x;
    insert into tombstone (table_id, file_id, ref_id, begin_seq)
    select distinct bench_id(0), f, b, s from (
      select f from generate_series(w, w + 799) f where f % 10 = d
      union select d + 10000 * i from generate_series(0, current_setting('bench.k')::int - 1) i where d + 10000 * i <= n) x;
    p := b;
  end loop;
  -- main, after the forks: removes the files of the window whose id ends in 0 and adds as many.
  s := nextval('commit_seq_gen');
  update data_file set end_seq = s where table_id = bench_id(0) and ref_id = 1 and file_id between w and w + 799
                                     and file_id % 10 = 0;
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                         footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  select bench_id(0), nextval('file_id_gen'), 1, s, 'gs://bench/t0/new-' || i || '.parquet', 0, '[]', 1000, 1000000,
         10000, '{1}', array[encode_long(w * 10 + i * 100)], array[encode_long(w * 10 + i * 100 + 9)], '{0}', '{NULL}'
    from generate_series(0, 79) i;
end $$;
SQL
}
export PGOPTIONS="-c bench.n=$N -c bench.depth=$DEPTH -c bench.k=$K"
r "seed" "$(ms seed)"
r "index pk" "$(ms q -c "alter table data_file add primary key (table_id, file_id, begin_seq)")"
r "index live" "$(ms q -c "create unique index data_file_live on data_file (table_id, file_id) where end_seq = 9223372036854775807")"
r "index ref" "$(ms q -c "create index data_file_ref on data_file (table_id, ref_id, begin_seq)")"
r "index bound" "$(ms q -c "create index data_file_bound on data_file using gist (table_id, ref_id, bound_range(lower[1], upper[1]))")"
q -c "vacuum analyze"
unset PGOPTIONS
r "size" "$(q -c "select 'data_file=' || pg_size_pretty(pg_table_size('data_file')) || ' ' || string_agg(i || '=' || pg_size_pretty(pg_relation_size(i::regclass)), ' ' order by i) from unnest(array['data_file_pkey', 'data_file_live', 'data_file_ref', 'data_file_bound']) i")"
r "rows" "$(q -c "select 'files=' || count(*) || ' tombstones=' || (select count(*) from tombstone) from data_file")"

# ── the format: pruned planning, checked against prune() ──────────────────────────────────────────────────────────
B=$(q -c "select ref_id from ref where name = 'b$DEPTH'")
LO=$((N / 4 * 10))
failures=0
for ref in 1 "$B"; do
  for width in 8000 80000; do
    HI=$((LO + width - 1))
    want=$(q -c "select count(*) || ':' || coalesce(sum(file_id), 0) from prune($ref, bench_id(0), 9223372036854775806, 1, encode_long($LO), encode_long($HI))")
    got=$(q -v r="$ref" -v lo="$LO" -v hi="$HI" -f "$D/v0_plan.sql" | awk -F'|' '{n++; s+=$1} END {printf "%d:%d", n, s}')
    used=$({ echo "explain (costs off)"; cat "$D/v0_plan.sql"; } | q -v r="$ref" -v lo="$LO" -v hi="$HI" \
           | grep -o 'data_file_[a-z_]*' | sort -u | tr '\n' ' ')
    lat=$(pgbench -n -c1 -j1 -T "$DUR" -D r="$ref" -D lo="$LO" -D hi="$HI" -f "$D/v0_plan.sql" 2>&1 \
          | awk -F' = ' '/latency average/ {print $2}')
    [ "$got" = "$want" ] || { r "WRONG" "ref=$ref width=$width got=$got prune=$want"; failures=$((failures + 1)); }
    r "plan" "ref=$ref" "files=${want%%:*}" "latency=$lat" "indexes: $used"
  done
done
r "correctness" "failures=$failures"

# ── commits: the reference commit(), and the statements the format requires of an append (v0_commit_lean.sql) ───
r "create $TABLES tables" "$(ms q -c "select bench_table(n) from generate_series(1, $TABLES) n")"
snaps() { q -c "select count(*) from snapshot"; }
commits() { # <label> <script> <first table> <last table> <clients>
  local before after out
  before=$(snaps)
  out=$(pgbench -n -c "$5" -j "$(( $5 < 4 ? $5 : 4 ))" -T "$DUR" -D first="$3" -D last="$4" -f "$D/$2" 2>&1)
  after=$(snaps)
  r "commit" "$1" "clients=$5" "$(echo "$out" | awk -F' = ' '/latency average/ {printf "latency=%s", $2} /^tps/ {printf " tps=%d", $2}')" \
    "$(echo "$out" | awk -F': ' '/actually processed/ {split($2, a, "/"); printf "processed=%s", a[1]} /failed transactions/ {printf " failed=%s", $2}') snapshots=$((after - before))"
}
for kind in $KINDS; do
  script=v0_commit.sql; [ $kind = lean ] && script=v0_commit_lean.sql
  commits "$kind, 4 files, 1 table" $script 1 1 1
  commits "$kind, 4 files, 1 hot table" $script 1 1 8
  commits "$kind, 4 files, $TABLES tables" $script 1 "$TABLES" 8
  [ "$GATE" = 1 ] || commits "$kind, 4 files, the $N-file table" $script 0 0 1
done
# What the lean commits wrote is a valid catalog: the reference finds nothing wrong with any table, nor with the
# files of the last 500 commits to the large table, and every table's snapshots form one chain that ends at its
# head.
r "lean commits checked" "$(q -c "select 'errors=' || (select count(*) from generate_series(1, $TABLES) n,
    lateral (select table_errors(1, bench_id(n), 9223372036854775806) union all
             select file_errors(1, bench_id(n), 9223372036854775806)) e)
  + (select count(*) from (select s.snapshot_id from snapshot s where s.table_id = bench_id(0) order by s.snapshot_id desc limit 500) s,
       lateral file_errors(1, bench_id(0), 9223372036854775806, s.snapshot_id) e)
  || ' broken_chains=' || (select count(*) from snapshot s where s.parent_snapshot_id is not null and not exists (
       select 1 from snapshot x where x.table_id = s.table_id and x.snapshot_id = s.parent_snapshot_id))
  || ' heads_behind=' || (select count(*) from head h where h.snapshot_id <> (select max(snapshot_id) from snapshot x
       where x.table_id = h.table_id and x.ref_id = h.ref_id))")"

# ── branches and merges: the reference merge(), and set-based statements (v0_merge_lean.sql) checked against it ───
q -f "$D/v0_merge_lean.sql"
r "create branch" "$(ms q -c "select create_ref('fork', 1)")"
A=$((TABLES + 1))
q -c "select bench_table($A)" -c "select bench_commit(1, $A, 1000) from generate_series(1, 100)" >/dev/null
branch() { # <name> <offset of the removed files>: a branch of table A adds MERGE files and removes 100; main adds 1000
  local b
  b=$(q -c "select create_ref('$1', 1)")
  q -c "select bench_commit($b, $A, 1000) from generate_series(1, $MERGE / 1000)" >/dev/null
  q -c "select bench_commit($b, $A, 0, array(select file_id from data_file where table_id = bench_id($A) and ref_id = 1
          and end_seq = 9223372036854775807 order by file_id offset $2 limit 100))" >/dev/null
  q -c "select bench_commit(1, $A, 1000)" >/dev/null
  echo "$b"
}
per() { echo "$(( ${1% ms} * 1000 / (MERGE + 100) )) us"; }
if [ "$GATE" != 1 ]; then
B=$(branch mr 0)
t=$(ms q -c "select merge($B)")
r "merge" "reference" "branch adds $MERGE files and removes 100, table of ~100000 files" "$t" "per changed file=$(per "$t")"
fi
B=$(branch ml 1000)
q -c "create table bench_expected as select file_id from visible_files($B, bench_id($A), 9223372036854775806)
        union (select file_id from visible_files(1, bench_id($A), 9223372036854775806)
               except select file_id from visible_files(1, bench_id($A),
                 (select fork_seq from ref_base where ref_id = $B and end_seq = 9223372036854775807)))"
t=$(ms q -c "select bench_merge($B, bench_id($A))")
r "merge" "lean" "branch adds $MERGE files and removes 100, table of ~100000 files" "$t" "per changed file=$(per "$t")"
r "lean merge checked" "$(q -c "select 'differences=' || (select count(*) from (
      (select file_id from bench_expected except select file_id from visible_files(1, bench_id($A), 9223372036854775806))
      union all
      (select file_id from visible_files(1, bench_id($A), 9223372036854775806) except select file_id from bench_expected)) d)
  || ' errors=' || (select count(*) from table_errors(1, bench_id($A), 9223372036854775806))")"
for kind in $([ "$GATE" = 1 ] || echo reference lean); do
  B=$(q -c "select create_ref('m0$kind', 1)")
  q -c "select bench_commit($B, 0, 100)" >/dev/null
  f="merge($B)"; [ $kind = lean ] && f="bench_merge($B, bench_id(0))"
  r "merge" "$kind" "branch adds 100 files to the $N-file table" "$(ms q -c "set statement_timeout = '120s'" -c "select $f" || echo timeout)"
done
