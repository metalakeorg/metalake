#!/usr/bin/env bash
# Tienda's 90 days, as its warehouse lived them: the refined databases emptied, the first 60 days loaded in bulk
# (the brands arriving on day 45 between the two halves), then days 61 to 90 one by one, four drops and a close each.
# The raw data must be landed first (land.sh). What it took, and the files it left, go to out/baseline.txt.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
baseline=$here/out/baseline.txt
start=$(date +%s)

for db in curated marts ops; do hive -e "drop database if exists $db cascade" >/dev/null; done
hdfs dfs -rm -r -f -skipTrash /user/hive/warehouse/curated.db /user/hive/warehouse/marts.db /user/hive/warehouse/ops.db \
  >/dev/null
hive -f /company/hive/schema.sql >/dev/null

upto=$(date_of "$BULK_DAYS")
brand_from=$(date_of "$BRAND_DAY")
hive -f /company/hive/backfill.sql "upto=$upto" "brand_from=$brand_from" >/dev/null
hive -f /company/hive/alter.sql >/dev/null
hive -f /company/hive/backfill_after_brand.sql "upto=$upto" "brand_from=$brand_from" >/dev/null
bulk=$(date +%s)
echo "bulk: days 1-$BULK_DAYS in $((bulk - start))s"

for day in $(seq $((BULK_DAYS + 1)) 90); do
  t=$(date +%s)
  run_day "$day"
  echo "day $day ($(date_of "$day")) in $(($(date +%s) - t))s"
done

{
  echo "history: 90 days in $(($(date +%s) - start))s (bulk $((bulk - start))s)"
  files
} | tee "$baseline"
