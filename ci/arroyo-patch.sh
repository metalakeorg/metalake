#!/usr/bin/env bash
# Checks that metalake-arroyo patches crates.io as Arroyo does at the rev it depends on: a dependency's patches don't
# reach the crate that depends on it, so the crate copies them, and a rev bump that leaves them behind fails here.
set -euo pipefail
manifest="$(dirname "$0")/../crates/metalake-arroyo/Cargo.toml"
rev=$(sed -n 's/^arroyo-operator = { git = "https:\/\/github.com\/describeloai\/arroyo", rev = "\([0-9a-f]*\)" }$/\1/p' "$manifest")
[ -n "$rev" ] || { echo "no Arroyo rev in $manifest"; exit 1; }
patches() { tr -d '\r' | sed -n '/^\[patch.crates-io\]/,$p' | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'; }
ours=$(patches < "$manifest")
theirs=$(curl -fsSL "https://raw.githubusercontent.com/describeloai/arroyo/$rev/Cargo.toml" | patches)
if [ "$ours" != "$theirs" ]; then
  diff <(echo "$theirs") <(echo "$ours") || true
  echo "metalake-arroyo's [patch.crates-io] differs from Arroyo's at $rev" && exit 1
fi
echo "[patch.crates-io] is Arroyo's at $rev"
