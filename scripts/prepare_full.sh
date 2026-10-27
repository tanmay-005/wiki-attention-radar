#!/usr/bin/env bash
# Phase 4 prep: MapReduce + warehouse load for the whole configured period.
# Days that are already done are skipped, so it is safe to re-run after an interruption.
#
# Runs INSIDE edge, best in the background (it takes a few hours):
#   docker compose exec -d edge bash -c "bash scripts/prepare_full.sh > /data/raw-cache/prepare_full.log 2>&1"
# Follow it with:
#   docker compose exec edge tail -f /data/raw-cache/prepare_full.log
set -uo pipefail
cd /workspace
start=$(date +%s)
echo "== MapReduce (MR1 + MR2) for every hourly day   $(date)"
for DAY in $(python3 scripts/list_days.py --full --kind hourly 2>/dev/null); do
  if hdfs dfs -test -e "/radar/staged/daily/dt=$DAY/_SUCCESS"; then
    echo "  $DAY already has MR2 output, skipped"
    continue
  fi
  t0=$(date +%s)
  if bash mapreduce/run_mr1.sh "$DAY" > /dev/null 2>&1 && bash mapreduce/run_mr2.sh "$DAY" > /dev/null 2>&1; then
    echo "  $DAY MR1 + MR2 done in $(( $(date +%s) - t0 )) s"
  else
    echo "  $DAY FAILED (logs: benchmarks/results/logs/mr1_${DAY}_*.log, mr2_${DAY}_*.log); stopping"
    exit 1
  fi
done

echo "== Spark warehouse load (hourly, daily, country)   $(date)"
SPARK_LOG=/data/raw-cache/prepare_full_spark.log
spark-submit --master local[3] --driver-memory 3g spark/jobs/load_warehouse.py --full --skip-existing \
  > "$SPARK_LOG" 2>&1
rc=$?
grep -E "^==|rows|skipped|done in" "$SPARK_LOG"
[[ $rc -eq 0 ]] || { echo "warehouse load FAILED (full output: $SPARK_LOG)"; exit 1; }

echo "== all done in $(( ($(date +%s) - start) / 60 )) min   $(date)"
echo "Next: register the partitions in Hive (from docker/):"
echo "  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/02_warehouse.hql"
