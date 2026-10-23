#!/usr/bin/env bash
# MR3 top_n: top 100 articles per (language, day) using a secondary sort.
# Usage (inside edge):  bash mapreduce/run_mr3.sh 2024-06-03 [N]
set -euo pipefail
DAY="$1"; N="${2:-100}"
source /workspace/mapreduce/mr_env.sh
OUT="/radar/staged/topn/dt=$DAY"
hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true
mkdir -p /workspace/benchmarks/results/logs

run_job "/workspace/benchmarks/results/logs/mr3_${DAY}.log" \
  -D mapreduce.job.name="MR3 top_n $DAY" \
  -D stream.num.map.output.key.fields=3 \
  -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2 -k3,3nr" \
  -D mapreduce.partition.keypartitioner.options="-k1,2" \
  -D mapreduce.job.reduces=1 \
  -files "$MR_DIR/mr3_mapper.py,$MR_DIR/mr3_reducer.py" \
  -cmdenv TOPN="$N" \
  -mapper "python3 mr3_mapper.py" \
  -reducer "python3 mr3_reducer.py" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input "/radar/staged/daily/dt=$DAY" -output "$OUT"

echo; echo "Top 5 per language:"
hdfs dfs -cat "$OUT/part-*" | awk -F'\t' '$3<=5'
