#!/usr/bin/env bash
# Benchmark B4 (step 4.13): daily aggregation of one day, MapReduce (MR2) vs Spark DataFrame.
# Same input (MR1 output for DAY), same result; compares wall-clock time.
# Runs INSIDE edge:  bash benchmarks/b4_mr_vs_spark.sh [DAY]
set -euo pipefail
DAY="${1:-2024-06-03}"
cd /workspace
CSV=benchmarks/results/b4.csv
echo "engine,wall_seconds,rows,views" > "$CSV"

echo "MR2 (Hadoop Streaming on YARN) ..."
bash mapreduce/run_mr2.sh "$DAY" > /dev/null 2>&1
LOG="benchmarks/results/logs/mr2_${DAY}_comb-yes.log"
mr_secs=$(grep -oE "WALL_SECONDS=[0-9]+" "$LOG" | cut -d= -f2)
mr_rows=$(grep -oE "Reduce output records=[0-9]+" "$LOG" | tail -1 | cut -d= -f2)
echo "mapreduce_mr2,$mr_secs,$mr_rows," >> "$CSV"

echo "Spark DataFrame (local[3]) ..."
spark-submit --master local[3] --driver-memory 3g benchmarks/b4_spark_daily.py "$DAY" 2>/dev/null \
  | tee /tmp/b4_spark.txt | grep -vE "^RESULT"
grep "^RESULT" /tmp/b4_spark.txt | cut -d, -f2- >> "$CSV"
echo; column -s, -t "$CSV" 2>/dev/null || cat "$CSV"
echo "saved $CSV"
