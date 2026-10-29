#!/usr/bin/env bash
# Phase 7: run benchmarks B1, B2, B4, B5 and B6 on one day of the final (October 2026) data, and make
# the copies B3 needs. B3 itself runs in hiveserver2 afterwards (the command is printed at the end).
#
# Runs INSIDE edge, in the background (about 25 minutes). From docker/ in PowerShell:
#   docker compose exec -d edge bash -c "bash benchmarks/run_benchmarks.sh 2026-10-20 > /data/raw-cache/bench.log 2>&1"
# Follow it:
#   docker compose exec edge tail -n 20 /data/raw-cache/bench.log
set -uo pipefail
DAY="${1:-2026-10-20}"
cd /workspace
RES=benchmarks/results
step () { echo; echo "== $1   $(date '+%H:%M:%S')"; }
fail () { echo "!! $1 FAILED, stopping"; exit 1; }
start=$(date +%s)

hdfs dfs -test -e "/radar/staged/hourly/dt=$DAY/_SUCCESS" || fail "no MR1 output for $DAY"
echo "$DAY" > $RES/day.txt

step "B1 combiner (MR2 with and without)"
bash benchmarks/b1_combiner.sh "$DAY" || fail "B1"
step "B2 map-output compression (MR1 none / snappy / gzip)"
bash benchmarks/b2_compression.sh "$DAY" || fail "B2"
step "B4 MapReduce vs Spark (daily aggregation)"
bash benchmarks/b4_mr_vs_spark.sh "$DAY" || fail "B4"
step "B5 RDD vs DataFrame"
spark-submit --master local[3] --driver-memory 3g benchmarks/b5_rdd_vs_df.py "$DAY" 2>/dev/null || fail "B5"
step "B3 copies of hourly_views as text and Parquet"
spark-submit --master local[3] --driver-memory 3g benchmarks/b3_make_copies.py "$DAY" 2>/dev/null || fail "B3 copies"
step "B6 query latency (Cassandra, MongoDB)"
python3 benchmarks/b6_latency.py || fail "B6"

echo
echo "== all done in $(( ($(date +%s) - start) / 60 )) min"
echo "Now B3 in hiveserver2 (from docker/ in PowerShell):"
echo "  docker compose exec hiveserver2 bash /workspace/benchmarks/b3_formats.sh $DAY 3"
