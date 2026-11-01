#!/usr/bin/env bash
# The MetaLake v0 conformance runner.
#
#   spec/v0/run.sh             every case, in a pinned postgres:17
#   spec/v0/run.sh visibility  only the case files whose name contains the filter
#
# Each case runs in a fresh schema holding schema.sql and reference.sql. A case passes when it runs without error.
# The runner then checks rule coverage: every rule id of metalake-v0.md must be named by a passing case, and every
# rule a case names must exist in the text.
set -euo pipefail
shopt -s nullglob
IMAGE=postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1
here=$(cd "$(dirname "$0")" && pwd)

if [ -z "${METALAKE_IN_CONTAINER:-}" ]; then
  root=$(cd "$here/../.." && { pwd -W 2>/dev/null || pwd; })
  exec env MSYS_NO_PATHCONV=1 docker run --rm -e METALAKE_IN_CONTAINER=1 -v "$root:/metalake:ro" -u postgres \
    "$IMAGE" bash /metalake/spec/v0/run.sh "$@"
fi

export PGDATA=/tmp/pg PGHOST=/tmp PGUSER=postgres PGDATABASE=postgres
initdb -A trust >/dev/null
pg_ctl -w -l /tmp/pg.log -o "-k /tmp -c listen_addresses='' -c track_commit_timestamp=on -c fsync=off" start >/dev/null
psql -X -q -c "create extension dblink" >/dev/null

# Split every case file at its `-- case:` lines. What precedes the first case is a fixture shared by all of them.
cases=/tmp/cases; mkdir -p "$cases"
for f in "$here"/conformance/[0-9]*.sql; do
  case "$(basename "$f")" in *"${1:-}"*) ;; *) continue ;; esac
  awk -v dir="$cases" -v base="$(basename "$f" .sql)" '
    /^-- case: / { n++; out = sprintf("%s/%s.%02d.sql", dir, base, n); printf "%s", pre > out }
    n == 0 { pre = pre $0 "\n"; next }
    { print > out }' "$f"
done

pass=0; fail=0; covered=""; i=0
for c in "$cases"/*.sql; do
  i=$((i + 1))
  title=$(sed -n 's/^-- case: //p' "$c")
  rules=$(sed -n 's/^-- rules: //p' "$c" | tr ',' ' ')
  if out=$(psql -X -q -o /dev/null -v ON_ERROR_STOP=1 2>&1 <<SQL
create schema c$i;
set search_path = c$i;
set client_min_messages = warning;
\i $here/schema.sql
\i $here/reference.sql
\i $here/conformance/_support.sql
\i $c
SQL
  ); then
    pass=$((pass + 1)); covered="$covered $rules"
    printf 'ok    %s  %s\n' "$(basename "$c" .sql)" "$title"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s  %s\n%s\n' "$(basename "$c" .sql)" "$title" "$(sed 's/^/      /' <<<"$out")"
  fi
done

text=$(grep -o '\*\*[A-Z]\+-[0-9]\+\.\*\*' "$here/../metalake-v0.md" | tr -d '*.' | sort -u || true)
named=$(tr ' ' '\n' <<<"$covered" | grep . | sort -u || true)
uncovered=$(comm -23 <(printf '%s\n' "$text" | grep .) <(printf '%s\n' "$named" | grep .) || true)
unknown=$(comm -13 <(printf '%s\n' "$text" | grep .) <(printf '%s\n' "$named" | grep .) || true)
printf '\n%d passed, %d failed; %d rules in the text, %d covered\n' "$pass" "$fail" \
  "$(grep -c . <<<"$text" || true)" "$(comm -12 <(printf '%s\n' "$text" | grep .) <(printf '%s\n' "$named" | grep .) | grep -c . || true)"
[ -z "$uncovered" ] || printf 'rules without a passing case: %s\n' "$(echo $uncovered)"
[ -z "$unknown" ] || printf 'rules named by cases but absent from the text: %s\n' "$(echo $unknown)"
[ "$fail" -eq 0 ] && [ -z "$uncovered" ] && [ -z "$unknown" ]
