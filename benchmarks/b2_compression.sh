#!/usr/bin/env bash
# B2: MR1 with map-output compression none / snappy / gzip (combiner off in all three, same as normal MR1 runs).
# Usage (inside edge): bash benchmarks/b2_compression.sh [DAY] [--quick]
set -euo pipefail
DAY="${1:-2024-06-03}"; QUICK="${2:-}"
CSV=/workspace/benchmarks/results/b2.csv; rm -f "$CSV"
L=/workspace/benchmarks/results/logs
for c in none snappy gzip; do
  bash /workspace/mapreduce/run_mr1.sh "$DAY" --map-compress "$c" $QUICK --out "/radar/bench/b2/$c" > /dev/null 2>&1 || { echo "run failed, see $L"; exit 1; }
  python3 /workspace/benchmarks/job_metrics.py "$L/mr1_${DAY}_comb-no_mapc-${c}.log" "$CSV" "map_compress=$c"
done
hdfs dfs -rm -r -f -skipTrash /radar/bench/b2 >/dev/null 2>&1 || true
echo "saved $CSV"
