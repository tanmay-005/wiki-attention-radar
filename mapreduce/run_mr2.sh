#!/usr/bin/env bash
# MR2 daily_agg: /radar/staged/hourly/dt=<DAY> -> /radar/staged/daily/dt=<DAY>
# Usage (inside edge):  bash mapreduce/run_mr2.sh 2024-06-03 [--no-combiner]
set -euo pipefail
DAY="$1"; COMB_OPTS=(-combiner "python3 sum_reducer.py"); COMB=yes
[[ "${2:-}" == "--no-combiner" ]] && { COMB_OPTS=(); COMB=no; }
source /workspace/mapreduce/mr_env.sh
OUT="/radar/staged/daily/dt=$DAY"
hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true
mkdir -p /workspace/benchmarks/results/logs

run_job "/workspace/benchmarks/results/logs/mr2_${DAY}_comb-${COMB}.log" \
  -libjars "$RADAR_JAR" \
  -D mapreduce.job.name="MR2 daily_agg $DAY" \
  -D stream.num.map.output.key.fields=3 \
  -D stream.num.reduce.output.key.fields=3 \
  -D mapreduce.job.reduces="$NUM_LANGS" \
  -D radar.langs="$RADAR_LANGS" \
  -D mapreduce.job.reduce.slowstart.completedmaps=0.8 \
  -D mapreduce.map.output.compress=true \
  -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
  -files "$MR_DIR/mr2_mapper.py,$MR_DIR/sum_reducer.py" \
  -mapper "python3 mr2_mapper.py" \
  "${COMB_OPTS[@]}" \
  -reducer "python3 sum_reducer.py" \
  -partitioner radar.mr.LangPartitioner \
  -input "/radar/staged/hourly/dt=$DAY" -output "$OUT"

hdfs dfs -ls -h "$OUT"
