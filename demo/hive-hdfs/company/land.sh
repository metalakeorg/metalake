#!/usr/bin/env bash
# Lands Tienda's raw data on HDFS, as its systems drop it, and has Hive see it: one day, or the whole history.
#
#   bash land.sh          everything generate.py wrote, at once, then each raw table's partitions found
#   bash land.sh 91       one day: its files put, and its partitions added
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
TABLES=(eventos clientes productos stock)

if [ $# -eq 0 ]; then
  "${compose[@]}" exec -T hive bash -c 'for t in eventos clientes productos stock; do
      hdfs dfs -mkdir -p /landing/$t && hdfs dfs -put -f /company/out/raw/$t/* /landing/$t/; done' \
    2> >(grep -v "^SLF4J" >&2)
  for t in "${TABLES[@]}"; do hive -e "msck repair table raw.$t" >/dev/null; done
else
  dt=$(date_of "$1")
  "${compose[@]}" exec -T hive bash -c "for t in eventos clientes productos stock; do
      hdfs dfs -mkdir -p /landing/\$t && hdfs dfs -put -f /company/out/raw/\$t/dt=$dt /landing/\$t/; done" \
    2> >(grep -v "^SLF4J" >&2)
  for t in clientes productos stock; do
    hive -e "alter table raw.$t add if not exists partition (dt='$dt')" >/dev/null
  done
  for h in 00 06 12 18; do
    hive -e "alter table raw.eventos add if not exists partition (dt='$dt', hora='$h')" >/dev/null
  done
fi
