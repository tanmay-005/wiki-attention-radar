#!/usr/bin/env bash
# Whole pipeline for the period in config/settings.yaml, unattended:
#   download -> HDFS -> MapReduce + warehouse (prepare_full.sh) -> Spark S1..S5 -> serving S6.
# Every step skips work that is already done, so after an interruption just start it again.
#
# Needs the hadoop and nosql profiles running. Start it from docker/ in PowerShell:
#   docker compose exec -d edge bash -c "bash scripts/run_all.sh > /data/raw-cache/run_all.log 2>&1"
# Follow it:
#   docker compose exec edge tail -n 20 /data/raw-cache/run_all.log
set -uo pipefail
cd /workspace
start=$(date +%s)
step () { echo; echo "== $1   $(date '+%Y-%m-%d %H:%M:%S')"; }
fail () { echo "!! $1 FAILED, stopping. Fix it and run the script again (finished steps are skipped)."; exit 1; }
spark () { spark-submit --master local[3] --driver-memory 3g "$@" 2>/dev/null; }

step "period: $(python3 -c 'from spark.common.config import load; p=load()["period"]; print(p["hourly_start"], "->", p["hourly_end"], "| detect from", p["detect_start"], "| country", p["country_start"], "->", p["country_end"])' 2>/dev/null)"

step "1/6 download hourly dumps"
python3 scripts/download_hourly.py --full 2>/dev/null | tail -n 5 || fail "hourly download"
step "2/6 download per-country files"
python3 scripts/download_country.py --full 2>/dev/null | tail -n 5 || fail "country download"

step "3/6 copy to HDFS (local copies kept, so a re-run does not download again)"
bash scripts/load_hdfs.sh --full 2>/dev/null | tail -n 8 || fail "HDFS load"

step "4/6 MapReduce and warehouse"
bash scripts/prepare_full.sh || fail "prepare_full.sh"

step "5/6 Spark detection"
spark spark/jobs/s1_build_series.py   || fail "S1"
spark spark/jobs/s2_spikes_global.py  || fail "S2"
spark spark/jobs/s3_spikes_country.py || fail "S3"
spark-submit --master local[2] spark/jobs/s4_enrich.py 2>/dev/null || fail "S4"
spark spark/jobs/s5_events.py         || fail "S5"
spark-submit --master local[2] spark/jobs/s5b_context.py 2>/dev/null || fail "S5b"

step "6/6 serving (Cassandra + MongoDB)"
bash scripts/run_s6.sh 2>/dev/null || fail "S6"

echo
echo "== all done in $(( ($(date +%s) - start) / 60 )) min   $(date '+%Y-%m-%d %H:%M:%S')"
