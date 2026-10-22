#!/usr/bin/env python3
"""Download Wikimedia per-country daily pageview files (differential privacy dataset).

Usage (inside the edge container, from /workspace):
    python3 scripts/download_country.py --dev
    python3 scripts/download_country.py --full
    python3 scripts/download_country.py --day 2024-06-03

Files land in  <local_raw_cache>/country_daily/dt=YYYY-MM-DD/YYYY-MM-DD.tsv
The published files are zero-padded, tab-separated .tsv files (2024-06-03.tsv),
not the .csv names given in the dataset README. If the main URL pattern returns
404, a few alternative layouts are tried and the one that worked is printed.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.common_download import Result, append_log, download, human, summarize  # noqa: E402
from spark.common.config import country_days, load  # noqa: E402


def candidate_urls(cfg: dict, d: dt.date) -> list[str]:
    main = cfg["wikimedia"]["country_url"].format(y=d.year, m=d.month, d=d.day)
    base = main.rsplit("/", 1)[0]
    alts = [
        f"{base}/{d.year}/{d.year}-{d.month}-{d.day}.csv",          # year sub-folder
        f"{base}/{d:%Y-%m-%d}.csv",                                  # zero-padded
        f"{base}/{d.year}-{d.month}-{d.day}.csv.gz",                 # compressed
    ]
    return [main] + [a for a in alts if a != main]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dev", action="store_true")
    g.add_argument("--full", action="store_true")
    g.add_argument("--day", action="append", help="specific day YYYY-MM-DD (repeatable)")
    args = ap.parse_args()

    cfg = load()
    days = [dt.date.fromisoformat(x) for x in args.day] if args.day else country_days(cfg, dev=args.dev)
    ua = cfg["wikimedia"]["user_agent"]
    root = Path(cfg["paths"]["local_raw_cache"]) / "country_daily"
    print(f"Per-country daily files: {len(days)} day(s) -> {root}\n")

    results: list[Result] = []
    for i, d in enumerate(days, 1):
        folder = root / f"dt={d:%Y-%m-%d}"
        existing = [p for p in folder.glob("*") if not p.name.endswith(".part")] if folder.exists() else []
        if existing:
            results.append(Result("", existing[0], "skipped", existing[0].stat().st_size))
            print(f"[{i:>2}/{len(days)}] {d}  already present ({existing[0].name})")
            continue

        r = None
        for url in candidate_urls(cfg, d):
            name = url.rsplit("/", 1)[1]
            kind = "gzip" if name.endswith(".gz") else "text"
            r = download(url, folder / name, ua, kind)
            if r.status != "missing":
                break
        results.append(r)
        if r.status == "downloaded":
            note = "" if r.url == candidate_urls(cfg, d)[0] else f"  (via alternative URL: {r.url})"
            print(f"[{i:>2}/{len(days)}] {d}  {r.dest.name}  {human(r.bytes)} in {r.seconds:.0f}s{note}", flush=True)
        else:
            print(f"[{i:>2}/{len(days)}] {d}  {r.status.upper()}: {r.error}", flush=True)

    append_log(root.parent / "download_log.csv", results)
    code = summarize(results)
    if code:
        print("\nIf every day is 'missing', open the dataset home page in a browser and tell Claude "
              "the exact link of one daily file, so settings.yaml can be corrected:")
        print("  " + cfg["wikimedia"]["country_url"].rsplit("/", 1)[0] + "/")
    return code


if __name__ == "__main__":
    sys.exit(main())
