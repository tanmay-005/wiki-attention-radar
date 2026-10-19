#!/usr/bin/env bash
# Creates the HDFS folder layout (docs/03_DATA_MODEL.md §2).
# Run: docker compose exec edge bash /workspace/scripts/init_hdfs.sh
set -euo pipefail
hdfs dfs -mkdir -p /radar/raw/hourly /radar/raw/country_daily \
                   /radar/staged /radar/output /radar/warehouse \
                   /radar/benchmarks /radar/lib /tmp
hdfs dfs -chmod -R 777 /radar /tmp
hdfs dfs -ls /radar
