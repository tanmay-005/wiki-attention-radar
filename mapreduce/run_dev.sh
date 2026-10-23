#!/usr/bin/env bash
# Runs MR1 then MR2 for every dev day.  Usage (inside edge): bash mapreduce/run_dev.sh [--dev|--full]
set -euo pipefail
MODE="${1:---dev}"
for DAY in $(cd /workspace && python3 scripts/list_days.py "$MODE" --kind hourly); do
  echo "================ $DAY ================"
  bash /workspace/mapreduce/run_mr1.sh "$DAY"
  bash /workspace/mapreduce/run_mr2.sh "$DAY"
done
