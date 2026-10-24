#!/usr/bin/env bash
# Recreates every table and function after the hiveserver2 container was recreated
# (its metastore lives inside the container). Data in HDFS is not touched.
# Runs INSIDE hiveserver2:  docker compose exec hiveserver2 bash /workspace/hive/rebuild_metadata.sh
set -euo pipefail
for f in hive/01_raw_tables.hql hive/udf/register.hql hive/02_warehouse.hql; do
  echo "== $f"
  bash /workspace/hive/run.sh "$f" > /dev/null 2>&1 || { echo "failed: $f (run it on its own to see the error)"; exit 1; }
done
echo "metadata rebuilt"
