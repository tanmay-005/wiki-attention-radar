#!/usr/bin/env python3
"""Download Wikimedia hourly pageview dumps into the raw cache (Docker volume on D:).

Usage (inside the edge container, from /workspace):
    python3 scripts/download_hourly.py --dev              # 2 dev days = 48 files
    python3 scripts/download_hourly.py --full             # whole period in settings.yaml
    python3 scripts/download_hourly.py --day 2026-10-20   # one or more specific days

Files land in  <local_raw_cache>/hourly/dt=YYYY-MM-DD/pageviews-YYYYMMDD-HH0000.gz
Re-running skips files that are already complete.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.common_download import append_log, download, human, summarize  # noqa: E402
from spark.common.config import hourly_days, load  # noqa: E402


def plan(days: list[dt.date], cfg: dict) -> list[tuple[str, Path]]:
    root = Path(cfg["paths"]["local_raw_cache"]) / "hourly"
    tmpl = cfg["wikimedia"]["hourly_url"]
    jobs = []
    for d in days:
        for h in range(24):
            url = tmpl.format(y=d.year, m=d.month, d=d.day, h=h)
            name = f"pageviews-{d:%Y%m%d}-{h:02d}0000.gz"
            jobs.append((url, root / f"dt={d:%Y-%m-%d}" / name))
    return jobs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dev", action="store_true", help="dev days from settings.yaml")
    g.add_argument("--full", action="store_true", help="full hourly period from settings.yaml")
    g.add_argument("--day", action="append", help="specific day YYYY-MM-DD (repeatable)")
    ap.add_argument("--workers", type=int, help="parallel downloads (default from settings, max 4)")
    args = ap.parse_args()

    cfg = load()
    if args.day:
        days = [dt.date.fromisoformat(x) for x in args.day]
    else:
        days = hourly_days(cfg, dev=args.dev)
    workers = min(args.workers or cfg["wikimedia"]["max_parallel_downloads"], 4)
    ua = cfg["wikimedia"]["user_agent"]

    jobs = plan(days, cfg)
    print(f"Hourly dumps: {len(days)} day(s), {len(jobs)} files, {workers} parallel download(s)")
    print(f"Destination:  {Path(cfg['paths']['local_raw_cache']) / 'hourly'}\n")

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(download, url, dest, ua, "gzip"): dest for url, dest in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            results.append(r)
            if r.status == "downloaded":
                speed = human(r.bytes / r.seconds) + "/s" if r.seconds else ""
                print(f"[{i:>3}/{len(jobs)}] {r.dest.name}  {human(r.bytes)} in {r.seconds:.0f}s ({speed})", flush=True)
            elif r.status == "skipped":
                print(f"[{i:>3}/{len(jobs)}] {r.dest.name}  already present", flush=True)
            else:
                print(f"[{i:>3}/{len(jobs)}] {r.dest.name}  {r.status.upper()}: {r.error}", flush=True)

    append_log(Path(cfg["paths"]["local_raw_cache"]) / "download_log.csv", results)
    return summarize(results)


if __name__ == "__main__":
    sys.exit(main())
