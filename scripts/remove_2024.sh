#!/usr/bin/env bash
# Remove the June 2024 data used while the pipeline was being built. Everything the project shows
# now is October 2026, and nothing reads the 2024 partitions any more.
# Deletes, for every day of 2024: raw dumps, MapReduce output, warehouse partitions and benchmark
# copies in HDFS, and the downloaded files in the local cache (on D:). Cannot be undone; the data
# can be downloaded again from Wikimedia if it is ever needed.
#
# Runs INSIDE edge:   bash scripts/remove_2024.sh
# Afterwards remove the 2024 partitions from Hive's metastore (from docker/ in PowerShell):
#   docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/sync_partitions.hql
set -uo pipefail
echo "HDFS before:"; hdfs dfs -du -s -h /radar

for p in "/radar/raw/hourly/dt=2024-*" "/radar/raw/country_daily/dt=2024-*" \
         "/radar/staged/*/dt=2024-*" "/radar/warehouse/radar.db/*/dt=2024-*" \
         "/radar/benchmarks/b3/*/dt=2024-*"; do
  n=$(hdfs dfs -ls -d "$p" 2>/dev/null | grep -c "dt=2024-" || true)
  echo "  $p: $n folders"
  [[ $n -gt 0 ]] && hdfs dfs -rm -r -f -skipTrash "$p" > /dev/null
done
echo "HDFS after:"; hdfs dfs -du -s -h /radar

CACHE=/data/raw-cache
echo "local cache before: $(du -sh $CACHE | cut -f1)"
rm -rf $CACHE/hourly/dt=2024-* $CACHE/country_daily/dt=2024-*
echo "local cache after:  $(du -sh $CACHE | cut -f1)"
rm -rf /workspace/benchmarks/results/2024-06-03
echo
echo "Done. Now remove the 2024 partitions from Hive (from docker/ in PowerShell):"
echo "  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/sync_partitions.hql"
