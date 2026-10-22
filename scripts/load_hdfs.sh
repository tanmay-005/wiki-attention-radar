#!/usr/bin/env bash
# Copies downloaded files from the raw cache into HDFS, keeping the dt= layout.
# Only files missing in HDFS are uploaded, so re-running is safe.
#
# Usage (inside the edge container):
#   bash /workspace/scripts/load_hdfs.sh --dev
#   bash /workspace/scripts/load_hdfs.sh --full
#   bash /workspace/scripts/load_hdfs.sh --full --delete-local   # free disk after upload
set -euo pipefail

MODE="${1:---dev}"
DELETE_LOCAL="no"
[[ "${2:-}" == "--delete-local" ]] && DELETE_LOCAL="yes"

cd "$(dirname "$0")/.."
CACHE=$(python3 -c "from spark.common.config import load; print(load()['paths']['local_raw_cache'])")
HDFS_HOURLY=$(python3 -c "from spark.common.config import load; print(load()['paths']['raw_hourly'])")
HDFS_COUNTRY=$(python3 -c "from spark.common.config import load; print(load()['paths']['raw_country'])")

uploaded=0; skipped=0; missing_local=0

load_kind () {   # $1 = local sub-folder, $2 = HDFS root, $3 = list_days kind
  local sub="$1" hroot="$2" kind="$3"
  for DAY in $(python3 scripts/list_days.py "$MODE" --kind "$kind"); do
    local ldir="$CACHE/$sub/dt=$DAY"
    local hdir="$hroot/dt=$DAY"
    if [[ ! -d "$ldir" ]]; then
      echo "  !! $sub $DAY: not downloaded yet ($ldir)"
      missing_local=$((missing_local+1)); continue
    fi
    hdfs dfs -mkdir -p "$hdir"
    existing=$(hdfs dfs -ls -C "$hdir" 2>/dev/null | xargs -r -n1 basename || true)
    to_put=()
    for f in "$ldir"/*; do
      [[ -f "$f" ]] || continue
      name=$(basename "$f")
      [[ "$name" == *.part ]] && continue
      if grep -qxF "$name" <<< "$existing"; then
        skipped=$((skipped+1))
      else
        to_put+=("$f")
      fi
    done
    if (( ${#to_put[@]} > 0 )); then
      hdfs dfs -put "${to_put[@]}" "$hdir/"
      uploaded=$((uploaded+${#to_put[@]}))
    fi
    echo "  $sub dt=$DAY: $(hdfs dfs -count "$hdir" | awk '{print $2}') file(s) in HDFS"
    if [[ "$DELETE_LOCAL" == "yes" ]]; then
      rm -rf "$ldir"
    fi
  done
}

echo "== Loading hourly dumps ($MODE) =="
load_kind hourly "$HDFS_HOURLY" hourly
echo "== Loading per-country files ($MODE) =="
load_kind country_daily "$HDFS_COUNTRY" country

echo
echo "== Summary =="
echo "  uploaded now:      $uploaded"
echo "  already in HDFS:   $skipped"
echo "  days not on disk:  $missing_local"
hdfs dfs -du -s -h /radar/raw
