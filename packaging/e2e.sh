#!/usr/bin/env bash
# What a person does with a wheel, on a machine that has nothing else: a new environment, an empty home, no Docker.
#
#   bash packaging/e2e.sh dist/metalake-*.whl
#   bash packaging/e2e.sh metalake==0.1.0 --index-url https://test.pypi.org/simple/
set -euo pipefail

if [ -f "$1" ]; then
  wanted=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
  size=$(python -c "import os, sys; print(os.path.getsize(sys.argv[1]) // 1048576)" "$wanted")
  echo "wheel: $size MB"
  [ "$size" -lt 100 ] || { echo "PyPI takes at most 100 MB per file"; exit 1; }
  source=(--no-index "$wanted")
else
  source=("$@")
fi
work=$(mktemp -d)
python -m venv "$work/venv"
bin="$work/venv/bin"
[ -d "$bin" ] || bin="$work/venv/Scripts"
"$bin/python" -m pip install --quiet "${source[@]}"

export HOME="$work/home" USERPROFILE="$work/home" PATH="$bin:$PATH"
unset METALAKE_POSTGRES METALAKE_SCHEMA METALAKE_POSTGRES_BIN PGPASSWORD
mkdir -p "$HOME/demo"
cd "$HOME/demo"
trap 'metalake stop >/dev/null 2>&1 || true' EXIT

metalake --version
metalake init ./lake
metalake status
metalake sql "create table t (k bigint); insert into t select value from generate_series(1, 200000)"
[ "$(metalake sql 'select count(*) as n from t' --format csv | tail -1)" = 200000 ]
ls lake/data/default/*/data/*.parquet >/dev/null
metalake branch create dev
metalake sql "insert into dev.default.t values (0)"
metalake merge dev
[ "$(metalake sql 'select count(*) as n from t' --format csv | tail -1)" = 200001 ]
metalake maintain >/dev/null
metalake stop
[ ! -e "$HOME/.metalake" ] || { echo "it fetched what the wheel should carry"; exit 1; }
trap - EXIT
rm -rf "$work"
echo "e2e ok"
