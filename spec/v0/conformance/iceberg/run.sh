#!/usr/bin/env bash
# The MetaLake v0 Iceberg harness (§16): the projection checked with PyIceberg.
#
#   spec/v0/conformance/iceberg/run.sh
#
# Builds the harness image once (the pinned Postgres of the SQL suite plus a pinned PyIceberg), then runs harness.py
# inside it against a fresh catalog. It prints one line per check and the coverage of the ICE rules.
set -euo pipefail
IMAGE=metalake-iceberg-harness:v0
here=$(cd "$(dirname "$0")" && pwd)

if [ -z "${METALAKE_IN_CONTAINER:-}" ]; then
  root=$(cd "$here/../../../.." && { pwd -W 2>/dev/null || pwd; })
  docker image inspect "$IMAGE" >/dev/null 2>&1 || (cd "$here" && docker build -q -t "$IMAGE" . >/dev/null)
  exec env MSYS_NO_PATHCONV=1 docker run --rm -e METALAKE_IN_CONTAINER=1 -v "$root:/metalake:ro" -u postgres \
    "$IMAGE" bash /metalake/spec/v0/conformance/iceberg/run.sh
fi

export PGDATA=/tmp/pg PGHOST=/tmp PGUSER=postgres PGDATABASE=postgres
initdb -A trust >/dev/null
pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c track_commit_timestamp=on -c fsync=off" start >/dev/null
mkdir -p /tmp/wh
exec /venv/bin/python /metalake/spec/v0/conformance/iceberg/harness.py
