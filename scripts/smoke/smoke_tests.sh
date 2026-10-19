#!/usr/bin/env bash
# Phase 0 smoke tests: MapReduce on YARN (Pi) + Python Hadoop Streaming.
# Run: docker compose exec edge bash /workspace/scripts/smoke/smoke_tests.sh
set -euo pipefail
cd /workspace/scripts/smoke

echo "== 1/2 Pi example on YARN =="
yarn jar "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 10

echo "== 2/2 Python streaming word count =="
printf "big\ndata\nbig\nradar\nbig\n" > /tmp/smoke.txt
hdfs dfs -mkdir -p /tmp/smoke
hdfs dfs -put -f /tmp/smoke.txt /tmp/smoke/input.txt
hdfs dfs -rm -r -f /tmp/smoke/out
mapred streaming \
  -D mapreduce.job.name="smoke streaming" \
  -files wc_mapper.py,wc_reducer.py \
  -mapper "python3 wc_mapper.py" \
  -reducer "python3 wc_reducer.py" \
  -input /tmp/smoke/input.txt \
  -output /tmp/smoke/out
echo "Expected: big 3, data 1, radar 1"
hdfs dfs -cat /tmp/smoke/out/part-*
