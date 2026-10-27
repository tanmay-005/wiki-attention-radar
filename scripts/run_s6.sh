#!/usr/bin/env bash
# Step 5.x: load the serving stores. Runs INSIDE edge, with Cassandra and MongoDB up:
#   bash scripts/run_s6.sh                 # everything
#   bash scripts/run_s6.sh --skip-series   # only events (Cassandra small tables + MongoDB), about a minute
#   bash scripts/run_s6.sh --dry-run       # build everything, write nothing
#
# The Spark Cassandra connector is fetched from Maven Central on the first run and kept
# in /data/raw-cache/ivy2 (on D:), so later runs start offline.
set -euo pipefail
cd /workspace
exec spark-submit --master local[3] --driver-memory 3g \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.jars.ivy=/data/raw-cache/ivy2 \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.output.ignoreNulls=true \
  --conf spark.cassandra.output.concurrent.writes=4 \
  spark/jobs/s6_load_serving.py "$@"
