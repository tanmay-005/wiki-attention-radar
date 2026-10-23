#!/usr/bin/env bash
# B1: MR2 (daily aggregation) with and without the combiner on the same input.
# MR2 is where the combiner matters: one mapper reads all 24 hours x 2 platforms of a
# language, so the combiner collapses them before the shuffle. Needs MR1 output for DAY.
# Usage (inside edge): bash benchmarks/b1_combiner.sh [DAY]
set -euo pipefail
DAY="${1:-2024-06-03}"
CSV=/workspace/benchmarks/results/b1.csv; rm -f "$CSV"
L=/workspace/benchmarks/results/logs
for comb in yes no; do
  opt=(); [[ $comb == no ]] && opt=(--no-combiner)
  bash /workspace/mapreduce/run_mr2.sh "$DAY" "${opt[@]}" > /dev/null 2>&1 || { echo "run failed, see $L"; exit 1; }
  python3 /workspace/benchmarks/job_metrics.py "$L/mr2_${DAY}_comb-${comb}.log" "$CSV" "combiner=$comb"
done
echo "saved $CSV  (the last run was without the combiner; its output is identical, so nothing to redo)"
