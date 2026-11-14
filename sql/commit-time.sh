#!/usr/bin/env bash
# TT-1 on a Postgres that stops keeping commit timestamps (crates/metalake/tests/tt1_commit_time.rs): the pinned
# postgres:17 of sql/test.sh, its setting changed by ALTER SYSTEM and taken by a restart, as a server's operator
# changes it. A catalog is made while it keeps them; then, restarted without them, the core is to refuse a new one and
# to read the first without writing it.
#
#   bash sql/commit-time.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${IMAGE:-postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1}
name=metalake-commit-time-pg; port=${PORT:-55434}
docker rm -f "$name" >/dev/null 2>&1 || true
# As sql/test.sh: on Windows, the machine's own address, where only Docker listens.
host=localhost
if command -v powershell >/dev/null 2>&1; then
  host=$(powershell -NoProfile -Command "(Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { \$_.InterfaceAlias -notmatch 'Loopback|vEthernet|WSL' -and \$_.IPAddress -notlike '169.*' } |
    Select-Object -First 1).IPAddress" | tr -d '\r')
  host=${host:-localhost}
fi
# The setting is not on the command line, which would hold it over ALTER SYSTEM.
docker run -d --name "$name" -p "$port:5432" -e POSTGRES_HOST_AUTH_METHOD=trust "$image" -c fsync=off >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT

ready() { until docker exec "$name" pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2; }
commit_time() {
  docker exec "$name" psql -q -U postgres -c "alter system set track_commit_timestamp = $1" >/dev/null
  docker restart "$name" >/dev/null
  ready
  test "$(docker exec "$name" psql -tA -U postgres -c 'show track_commit_timestamp')" = "$1"
}

ready
export DATABASE_URL="host=$host port=$port user=postgres dbname=postgres"
cd "$here/.."
commit_time on
METALAKE_TT1=kept cargo test -p metalake --test tt1_commit_time
commit_time off
METALAKE_TT1=lost cargo test -p metalake --test tt1_commit_time
