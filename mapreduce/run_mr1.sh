#!/usr/bin/env bash
# MR1 filter_normalise: raw hourly dumps of one day -> /radar/staged/hourly/dt=<DAY>
#
# Usage (inside edge):  bash mapreduce/run_mr1.sh 2024-06-03 [options]
#   --combiner               also run sum_reducer.py as a combiner (off by default: each hourly
#                            file has every key only once, so it merges almost nothing here)
#   --map-compress X         none | snappy | gzip   (default snappy; benchmark B2)
#   --out PATH               output folder (default /radar/staged/hourly/dt=<DAY>)
#   --quick                  only the first 6 hours of the day (faster benchmarks)
set -euo pipefail
DAY="$1"; shift
COMBINER=no; MAPC=snappy; OUT="/radar/staged/hourly/dt=$DAY"; INPUT="/radar/raw/hourly/dt=$DAY"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --combiner) COMBINER=yes ;;
    --map-compress) MAPC="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    --quick) INPUT="/radar/raw/hourly/dt=$DAY/pageviews-*-0[0-5]0000.gz" ;;
    *) echo "unknown option $1"; exit 1 ;;
  esac
  shift
done
source /workspace/mapreduce/mr_env.sh

case "$MAPC" in
  none)   MAPC_OPTS=(-D mapreduce.map.output.compress=false) ;;
  snappy) MAPC_OPTS=(-D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec) ;;
  gzip)   MAPC_OPTS=(-D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec) ;;
  *) echo "--map-compress must be none|snappy|gzip"; exit 1 ;;
esac
COMB_OPTS=(); [[ "$COMBINER" == yes ]] && COMB_OPTS=(-combiner "python3 sum_reducer.py")

hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1 || true
mkdir -p /workspace/benchmarks/results/logs
LOG="/workspace/benchmarks/results/logs/mr1_${DAY}_comb-${COMBINER}_mapc-${MAPC}.log"

run_job "$LOG" \
  -libjars "$RADAR_JAR" \
  -D mapreduce.job.name="MR1 filter_normalise $DAY (combiner=$COMBINER, mapc=$MAPC)" \
  -D stream.num.map.output.key.fields=4 \
  -D stream.num.reduce.output.key.fields=4 \
  -D mapreduce.job.reduces="$NUM_LANGS" \
  -D radar.langs="$RADAR_LANGS" \
  -D mapreduce.job.reduce.slowstart.completedmaps=0.8 \
  "${MAPC_OPTS[@]}" \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
  -files "$MR_DIR/mr1_mapper.py,$MR_DIR/sum_reducer.py,$MR_DIR/common_title.py" \
  -cmdenv RADAR_LANGS="$RADAR_LANGS" \
  -mapper "python3 mr1_mapper.py" \
  "${COMB_OPTS[@]}" \
  -reducer "python3 sum_reducer.py" \
  -partitioner radar.mr.LangPartitioner \
  -input "$INPUT" -output "$OUT"

echo; echo "Output files (one language per file):"
hdfs dfs -ls -h "$OUT"
