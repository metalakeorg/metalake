#!/usr/bin/env bash
# Preliminary study of the implementation: a local run, for correctness only (timings on a laptop do not count).
#
#   bash bench/impl/local.sh [files] [tables] [seconds]
#
# Starts postgres:17 in Docker, loads spec/v0, proto.sql and setup.sql, and runs the client built by
# `cargo build --release` in bench/impl.
set -euo pipefail
export LC_ALL=C MSYS_NO_PATHCONV=1
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
N=${1:-20000}; TABLES=${2:-20}; SECS=${3:-2}
image=postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1
mount=$root; command -v cygpath >/dev/null && mount=$(cygpath -w "$root")
docker rm -f metalake-previo >/dev/null 2>&1 || true
docker run -d --name metalake-previo -p 55432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -v "$mount":/m:ro "$image" \
  -c track_commit_timestamp=on -c shared_buffers=1GB >/dev/null
[ -n "${KEEP:-}" ] || trap 'docker rm -f metalake-previo >/dev/null' EXIT
until docker exec metalake-previo pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2
files=$(cd "$root/spec/v0" && { ls schema/*.sql; ls reference/*.sql; } | sed 's#^#/m/spec/v0/#')
{ for f in $files /m/bench/impl/proto.sql; do echo "\\i $f"; done; echo "\\i /m/bench/impl/setup.sql"; } \
  | docker exec -i metalake-previo psql -X -q -U postgres -v ON_ERROR_STOP=1 -v n="$N" -v tables="$TABLES" -o /dev/null
"$here/target/release/metalake-previo" "host=localhost port=55432 user=postgres dbname=postgres" "$N" "$TABLES" "$SECS"
docker exec metalake-previo psql -X -q -At -U postgres -c "select 'checked: errors=' || (select count(*) from generate_series(1, $TABLES) n,
    lateral (select table_errors(1, bench_id(n), 9223372036854775806) union all
             select file_errors(1, bench_id(n), 9223372036854775806)) e)
  || ' broken_chains=' || (select count(*) from snapshot s where s.parent_snapshot_id is not null and not exists (
       select 1 from snapshot x where x.table_id = s.table_id and x.snapshot_id = s.parent_snapshot_id))
  || ' heads_behind=' || (select count(*) from head h where h.snapshot_id <> (select max(snapshot_id) from snapshot x
       where x.table_id = h.table_id and x.ref_id = h.ref_id))"
