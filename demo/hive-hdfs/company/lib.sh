# What Tienda's scripts share: Hive's command line in its container, and the history's calendar.
# Sourced, not run.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Docker takes the host's paths: on Windows, Git Bash's /c/... as C:/...
if command -v cygpath >/dev/null 2>&1; then here=$(cygpath -m "$here"); fi
compose=(docker compose -f "$here/../compose.yml")
# Git Bash would turn the containers' paths into Windows ones.
export MSYS_NO_PATHCONV=1

# Runs Hive SQL: `hive -f /company/hive/x.sql dt=2024-01-01 hora=00`, or `hive -e "select 1"`.
hive() {
  local how=$1 what=$2
  shift 2
  local vars=()
  for pair in "$@"; do vars+=(--hivevar "$pair"); done
  "${compose[@]}" exec -T hive beeline -u jdbc:hive2://localhost:10000/ --silent=true --showHeader=false \
    --outputformat=tsv2 "${vars[@]}" "$how" "$what" 2> >(grep -vE "^SLF4J|beeline$|No such file" >&2)
}

# HDFS's command line, in the NameNode's container.
hdfs() {
  "${compose[@]}" exec -T namenode hdfs "$@" 2> >(grep -v "^SLF4J" >&2)
}

# The date of day n, 1 being 2024-01-01.
date_of() {
  python -c "import datetime,sys; print(datetime.date(2024,1,1)+datetime.timedelta(days=int(sys.argv[1])-1))" "$1"
}

# The day the brands come (alter.sql), and the last day the history loads in bulk.
BRAND_DAY=45
BULK_DAYS=60

# One day of production, its raw data already landed: four drops, then the close.
run_day() {
  local dt marca=""
  dt=$(date_of "$1")
  [ "$1" -ge "$BRAND_DAY" ] && marca=", marca"
  for h in 00 06 12 18; do hive -f /company/hive/drop.sql "dt=$dt" "hora=$h" >/dev/null; done
  hive -f /company/hive/close.sql "dt=$dt" "marca=$marca" >/dev/null
}

# How many files and bytes each table of the warehouse holds on HDFS.
files() {
  hdfs dfs -count /user/hive/warehouse/curated.db/* /user/hive/warehouse/marts.db/* /user/hive/warehouse/ops.db/* |
    awk '{n=split($4,p,"/"); printf "%-40s files=%-6s bytes=%s\n", p[n-1] "/" p[n], $2, $3}'
}
