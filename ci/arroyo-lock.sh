#!/usr/bin/env bash
# Locks metalake-arroyo, after its Arroyo rev or the core's dependencies change. Cargo takes the highest version the
# core's Arrow range allows, 59, beside Arroyo's 58.4; every Arrow crate of the core goes down to Arroyo's, and the lock
# is refused if one stays at 59.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/../crates/metalake-arroyo"
if [ "${1:-}" = --check ]; then
  if grep -A1 -E '^name = "(arrow-[a-z]+|parquet)"$' Cargo.lock | grep -q 'version = "59'; then
    echo "Arrow 59 is in metalake-arroyo's lock: run ci/arroyo-lock.sh" && exit 1
  fi
  exit 0
fi
cargo generate-lockfile
for c in parquet arrow-array arrow-schema arrow-select arrow-buffer arrow-data; do
  cargo update -q "$c@59" --precise 58.4.0 2>/dev/null || true
done
"$here/arroyo-lock.sh" --check
