#!/usr/bin/env bash
# Stages G1 in a flat directory, as the bench runner mounts it: the bench, spec/v0/schema as s-*, the reference files
# the protocol reuses as u-NN-* in their order, and sql/v0 as p-*.
#
#   bash bench/g1-stage.sh <dir>
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); out=$1
rm -rf "$out"; mkdir -p "$out"
cp "$root"/bench/g1.sh "$root"/bench/p1.sh "$root"/bench/g1_*.sql "$out/"
for f in "$root"/spec/v0/schema/*.sql; do cp "$f" "$out/s-$(basename "$f")"; done
i=10; grep -v '^#' "$root/sql/v0/reference-reused.txt" | while read -r f; do
  [ -n "$f" ] && cp "$root/spec/v0/reference/$f" "$out/u-$i-$f"; i=$((i + 1))
done
for f in "$root"/sql/v0/*.sql; do cp "$f" "$out/p-$(basename "$f")"; done
