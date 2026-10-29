#!/usr/bin/env bash
# Runs an HQL file with beeline. Runs INSIDE the hiveserver2 container:
#   docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/01_raw_tables.hql [--hivevar k=v ...]
#
# Default variables (override with --hivevar):
#   from / to  date range for loads (default: everything)
#   day        the day the checks and analysis look at (default 2026-10-20)
#   day2       the day after, for day-over-day comparisons (default 2026-10-21)
set -euo pipefail
cd /workspace
FILE="$1"; shift
exec beeline -u jdbc:hive2://localhost:10000 -n root \
  --hivevar from=0000-01-01 --hivevar to=9999-12-31 \
  --hivevar day=2026-10-20 --hivevar day2=2026-10-21 \
  "$@" -f "$FILE"
