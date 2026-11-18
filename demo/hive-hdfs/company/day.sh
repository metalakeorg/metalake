#!/usr/bin/env bash
# One more day of Tienda's production, after its history: the day's raw data generated and landed, then Hive's four
# drops and the close. With no day given, the one after the last in curated.ventas.
#
#   bash day.sh          the next day
#   bash day.sh 95       day 95, 2024-04-04
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
if [ $# -gt 0 ]; then
  day=$1
else
  last=$(hive -e "select max(dt) from curated.ventas" | tr -d '[:space:]')
  day=$(python -c "import datetime,sys; print((datetime.date.fromisoformat(sys.argv[1])-datetime.date(2024,1,1)).days+2)" "$last")
fi
start=$(date +%s)
python "$here/generate.py" --out "$here/out" --day "$day" >/dev/null
bash "$here/land.sh" "$day"
run_day "$day"
echo "day $day ($(date_of "$day")) in $(($(date +%s) - start))s"
