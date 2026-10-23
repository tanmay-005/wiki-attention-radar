#!/usr/bin/env bash
# MR4 search (map-only): every hourly row of one title.
# Usage (inside edge):  bash mapreduce/run_mr4.sh 2024-06-03 "Virat_Kohli" [lang]
set -euo pipefail
DAY="$1"; TITLE="$2"; LANG_FILTER="${3:-}"
source /workspace/mapreduce/mr_env.sh
SAFE=$(echo "$TITLE" | tr -c 'A-Za-z0-9_\n-' '_')
OUT="/radar/staged/search/$SAFE/dt=$DAY"
hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true
mkdir -p /workspace/benchmarks/results/logs

run_job "/workspace/benchmarks/results/logs/mr4_${DAY}_${SAFE}.log" \
  -D mapreduce.job.name="MR4 search '$TITLE' $DAY" \
  -D mapreduce.job.reduces=0 \
  -files "$MR_DIR/mr4_search.py,$MR_DIR/common_title.py" \
  -cmdenv SEARCH_TITLE="$TITLE" \
  -cmdenv SEARCH_LANG="$LANG_FILTER" \
  -mapper "python3 mr4_search.py" \
  -input "/radar/staged/hourly/dt=$DAY" -output "$OUT"

echo; echo "Matches (sorted by hour):"
hdfs dfs -cat "$OUT/part-*" | sort -t$'\t' -k4,4 -k2,2
