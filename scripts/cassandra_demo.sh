#!/usr/bin/env bash
# Step 5.x: run the Cassandra demos and keep their output for the report.
# Runs INSIDE the cassandra container (from docker/ in PowerShell):
#   docker compose exec cassandra bash /workspace/scripts/cassandra_demo.sh
set -uo pipefail
mkdir -p /workspace/data/exports /workspace/report/outputs
OUT=/workspace/report/outputs
cqlsh --request-timeout=120 -f /workspace/scripts/crud_demo_cassandra.cql 2>&1 | tee "$OUT/p5_cassandra_crud.txt"
{
  echo
  echo "== table sizes (nodetool flush, then tablestats)"
  nodetool flush radar
  nodetool tablestats radar | grep -E "^\s+Table: |Space used \(live\)|Number of partitions \(estimate\)|SSTable count"
} 2>&1 | tee -a "$OUT/p5_cassandra_crud.txt"
echo
echo "== COPY TO / COPY FROM"
cqlsh --request-timeout=120 -f /workspace/scripts/cassandra_copy_demo.cql 2>&1 | tee "$OUT/p5_cassandra_copy.txt"
echo
echo "rows in the CSV (without the header): $(( $(wc -l < /workspace/data/exports/event_rank.csv) - 1 ))"
echo "saved $OUT/p5_cassandra_crud.txt and $OUT/p5_cassandra_copy.txt"
