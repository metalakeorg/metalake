#!/usr/bin/env bash
# The protocol's tests: starts the pinned postgres:17 of the specification's runner in Docker, then runs the harness
# against it. Arguments go to `cargo test`.
#
#   bash sql/test.sh                 every test
#   bash sql/test.sh --test r0_skeleton
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${IMAGE:-postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1}
name=metalake-protocol-pg; port=${PORT:-55433}
docker rm -f "$name" >/dev/null 2>&1 || true
# On Windows, WSL relays every port Docker publishes on 127.0.0.1 and ::1, and a connection to localhost can reach
# the relay instead of Docker and hang: connect through the machine's own address, where only Docker listens.
host=localhost
if command -v powershell >/dev/null 2>&1; then
  host=$(powershell -NoProfile -Command "(Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { \$_.InterfaceAlias -notmatch 'Loopback|vEthernet|WSL' -and \$_.IPAddress -notlike '169.*' } |
    Select-Object -First 1).IPAddress" | tr -d '\r')
  host=${host:-localhost}
fi
docker run -d --name "$name" -p "$port:5432" -e POSTGRES_HOST_AUTH_METHOD=trust "$image" \
  -c track_commit_timestamp=on -c fsync=off ${PGARGS:-} >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT
until docker exec "$name" pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2
export DATABASE_URL="host=$host port=$port user=postgres dbname=postgres"
cd "$here/.." && cargo test -p metalake-protocol-tests "$@"
