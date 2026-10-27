#!/usr/bin/env bash
# Step 5.x: MongoDB demos plus mongoexport / mongoimport, output kept for the report.
# Runs INSIDE the mongo container (from docker/ in PowerShell):
#   docker compose exec mongo bash /workspace/scripts/mongo_demo.sh
set -uo pipefail
mkdir -p /workspace/data/exports /workspace/report/outputs
OUT=/workspace/report/outputs
mongosh --quiet /workspace/scripts/crud_demo_mongo.js 2>&1 | tee "$OUT/p5_mongo_crud.txt"
{
  echo
  echo "== mongoexport: events read by people in India -> JSON"
  mongoexport --db radar --collection events --query '{"scopes": "R:IN"}' --out /workspace/data/exports/events_R_IN.json
  echo "== mongoimport: the JSON -> radar.events_import_demo"
  mongoimport --db radar --collection events_import_demo --drop --file /workspace/data/exports/events_R_IN.json
  mongosh --quiet --eval '
    const r = db.getSiblingDB("radar");
    print("source documents (scopes R:IN): " + r.events.countDocuments({ scopes: "R:IN" }));
    print("imported documents:             " + r.events_import_demo.countDocuments());
    const d = r.events_import_demo.findOne({}, { first_detected: 1, peak_views: 1 });
    print("types kept after the round trip: first_detected " + typeof d.first_detected + " (" + d.first_detected.constructor.name + "), peak_views " + d.peak_views.constructor.name);'
} 2>&1 | tee "$OUT/p5_mongo_export_import.txt"
echo "saved $OUT/p5_mongo_crud.txt and $OUT/p5_mongo_export_import.txt"
