#!/usr/bin/env bash
# The Python API's tests against a wheel, installed where nothing else is. `bare` has no pyarrow or arro3, `current` has
# today's libraries, and `oldest` the oldest that pyproject.toml promises.
#
#   bash packaging/python_tests.sh dist/metalake-*.whl bare
set -euo pipefail

wheel=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
tests=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/crates/metalake-py/tests
work=$(mktemp -d)
python -m venv "$work/venv"
bin="$work/venv/bin"
[ -d "$bin" ] || bin="$work/venv/Scripts"
"$bin/python" -m pip install --quiet --no-index "$wheel"
case "$2" in
  bare) "$bin/python" -m pip install --quiet pytest polars; export METALAKE_TEST_BARE=1 ;;
  current) "$bin/python" -m pip install --quiet pytest polars pyarrow pandas mypy ;;
  oldest) "$bin/python" -m pip install --quiet pytest "polars==1.22.0" "pyarrow==15.0.0" "numpy<2" "pandas<3" ;;
  *) echo "bare, current or oldest"; exit 2 ;;
esac
"$bin/python" --version
"$bin/python" -m pip list 2>/dev/null | grep -iE '^(polars|pyarrow|pandas) ' || true
unset METALAKE_POSTGRES METALAKE_SCHEMA METALAKE_POSTGRES_BIN PGPASSWORD
cd "$work"
if [ "$2" = current ]; then
  "$bin/python" -m mypy.stubtest metalake --ignore-missing-stub --mypy-config-file "$tests/../pyproject.toml"
  "$bin/python" -m mypy --config-file "$tests/../pyproject.toml" "$tests/typing_check.py"
fi
# A test that hangs says where every thread is and ends the run, rather than wait for the limit of the job in silence.
"$bin/python" -m pytest "$tests" -q --tb=short -p no:cacheprovider \
  -o faulthandler_timeout=180 -o faulthandler_exit_on_timeout=true
