#!/usr/bin/env bash
# Benchmark B3: the same day of hourly_views stored as text, ORC and Parquet.
# Measures HDFS size and the time of three queries on each format.
# First make the copies (inside edge):
#   spark-submit --master local[3] --driver-memory 3g benchmarks/b3_make_copies.py [DAY]
# Then run this INSIDE hiveserver2:
#   docker compose exec hiveserver2 bash /workspace/benchmarks/b3_formats.sh [DAY] [RUNS]
# RUNS (default 1): run each query this many times and keep the fastest.
set -euo pipefail
DAY="${1:-2026-10-20}"; RUNS="${2:-1}"
cd /workspace
export HADOOP_CONF_DIR=/opt/hive/conf
HDFS="${HADOOP_HOME:-/opt/hadoop}/bin/hdfs"
URL=jdbc:hive2://localhost:10000
CSV=benchmarks/results/b3.csv
mkdir -p benchmarks/results/logs
LOG=benchmarks/results/logs/b3_${DAY}.log
: > "$LOG"

echo "registering the text and Parquet copies ..."
beeline -u "$URL" -n root -f benchmarks/b3_formats.hql >> "$LOG" 2>&1 \
  || { echo "table step failed, see $LOG"; exit 1; }
"$HDFS" dfs -test -d "/radar/benchmarks/b3/hourly_views_text/dt=$DAY" \
  || { echo "copies missing: run benchmarks/b3_make_copies.py $DAY in edge first"; exit 1; }

# Session settings so every run really reads the files:
#  no answers from table statistics, no query-result cache, no fetch-task shortcut.
PRE="USE radar; SET hive.compute.query.using.stats=false; SET hive.query.results.cache.enabled=false; SET hive.fetch.task.conversion=none;"
Q1="SELECT COUNT(*), SUM(views) FROM TBL WHERE dt='$DAY'"
Q2="SELECT page_title, SUM(views) AS v FROM TBL WHERE dt='$DAY' AND lang='ja' GROUP BY page_title ORDER BY v DESC LIMIT 10"
Q3="SELECT hour_ts, SUM(views_mobile), SUM(views) FROM TBL WHERE dt='$DAY' GROUP BY hour_ts ORDER BY hour_ts"

to_ms () {        # "12.345" -> 12345 (bash has no floating point)
  local i="${1%.*}" f="${1#*.}000"
  echo $(( 10#$i * 1000 + 10#${f:0:3} ))
}

time_query () {   # $1 = SQL; prints the fastest time (seconds) beeline reports for the final statement
  local best="" t
  for _ in $(seq "$RUNS"); do
    t=$(beeline -u "$URL" -n root -e "$PRE $1" 2>&1 | tee -a "$LOG" \
        | grep -oE '\([0-9]+\.[0-9]+ seconds\)' | tail -1 | grep -oE '[0-9]+\.[0-9]+' || true)
    [[ -z "$t" ]] && { echo "query failed, see $LOG" >&2; exit 1; }
    if [[ -z "$best" ]] || (( $(to_ms "$t") < $(to_ms "$best") )); then best="$t"; fi
  done
  echo "$best"
}

echo "format,table,size_bytes,q1_count_sum_s,q2_one_language_top10_s,q3_hourly_curve_s" > "$CSV"
for pair in "text:hourly_views_text:/radar/benchmarks/b3/hourly_views_text" \
            "orc:hourly_views:/radar/warehouse/radar.db/hourly_views" \
            "parquet:hourly_views_parquet:/radar/benchmarks/b3/hourly_views_parquet"; do
  IFS=: read -r fmt tbl path <<< "$pair"
  size=$("$HDFS" dfs -du -s "$path/dt=$DAY" | { read -r s _; echo "$s"; })
  t1=$(time_query "${Q1//TBL/$tbl}")
  t2=$(time_query "${Q2//TBL/$tbl}")
  t3=$(time_query "${Q3//TBL/$tbl}")
  echo "$fmt,$tbl,$size,$t1,$t2,$t3" | tee -a "$CSV"
done
echo "saved $CSV (full beeline output in $LOG)"
