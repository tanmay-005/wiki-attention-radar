#!/usr/bin/env bash
# Re-process the configured period after `languages` changed in settings.yaml.
# The raw dumps already in HDFS hold every wiki, so nothing is downloaded again: the MapReduce
# output and warehouse partitions of the period are removed and rebuilt with the new language
# list, then S1..S6 run. S4 runs with --refresh so every page gets the current region rules.
#
# Runs INSIDE edge, in the background (about 5 hours). From docker/ in PowerShell:
#   docker compose exec -d edge bash -c "bash scripts/rebuild_languages.sh > /data/raw-cache/rebuild.log 2>&1"
# Follow it:
#   docker compose exec edge tail -n 20 /data/raw-cache/rebuild.log
set -uo pipefail
cd /workspace
start=$(date +%s)
step () { echo; echo "== $1   $(date '+%Y-%m-%d %H:%M:%S')"; }
fail () { echo "!! $1 FAILED, stopping."; exit 1; }
spark () { spark-submit --master local[3] --driver-memory 3g "$@" 2>/dev/null; }
WH=/radar/warehouse/radar.db

step "languages: $(python3 -c 'from spark.common.config import load; print(", ".join(load()["languages"]))' 2>/dev/null)"

step "1/5 namespace names of every configured wiki"
python3 scripts/fetch_namespaces.py || fail "fetch_namespaces"

step "2/5 remove the old MapReduce output and warehouse partitions of the period"
for DAY in $(python3 scripts/list_days.py --full --kind hourly 2>/dev/null); do
  hdfs dfs -rm -r -f -skipTrash "/radar/staged/hourly/dt=$DAY" "/radar/staged/daily/dt=$DAY" \
    "$WH/hourly_views/dt=$DAY" "$WH/daily_views/dt=$DAY" > /dev/null 2>&1
done
for DAY in $(python3 scripts/list_days.py --full --kind country 2>/dev/null); do
  hdfs dfs -rm -r -f -skipTrash "$WH/country_daily/dt=$DAY" > /dev/null 2>&1
done
echo "  done"

step "3/5 MapReduce and warehouse"
bash scripts/prepare_full.sh || fail "prepare_full.sh"

step "4/5 Spark detection"
spark spark/jobs/s1_build_series.py   || fail "S1"
spark spark/jobs/s2_spikes_global.py  || fail "S2"
spark spark/jobs/s3_spikes_country.py || fail "S3"
spark-submit --master local[2] spark/jobs/s4_enrich.py --refresh 2>/dev/null || fail "S4"
spark spark/jobs/s5_events.py         || fail "S5"
spark-submit --master local[2] spark/jobs/s5b_context.py 2>/dev/null || fail "S5b"

step "5/5 serving (Cassandra + MongoDB)"
bash scripts/run_s6.sh 2>/dev/null || fail "S6"

echo
echo "== all done in $(( ($(date +%s) - start) / 60 )) min   $(date '+%Y-%m-%d %H:%M:%S')"
echo "Then register the new partitions in Hive (from docker/):"
echo "  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/02_warehouse.hql"
