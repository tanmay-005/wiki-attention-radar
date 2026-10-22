#!/usr/bin/env python3
"""Print the days (YYYY-MM-DD, one per line) for a mode, used by shell scripts.

    python3 scripts/list_days.py --dev  --kind hourly
    python3 scripts/list_days.py --full --kind country
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.common.config import country_days, hourly_days, load  # noqa: E402

ap = argparse.ArgumentParser()
g = ap.add_mutually_exclusive_group(required=True)
g.add_argument("--dev", action="store_true")
g.add_argument("--full", action="store_true")
ap.add_argument("--kind", choices=["hourly", "country"], default="hourly")
a = ap.parse_args()

cfg = load()
fn = hourly_days if a.kind == "hourly" else country_days
for d in fn(cfg, dev=a.dev):
    print(d.isoformat())
