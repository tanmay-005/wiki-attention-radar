#!/usr/bin/env python3
"""Step 1.4: inspect one hourly file and one country file and report their real format.

    python3 scripts/inspect_raw.py                       # first file of each kind in the cache
    python3 scripts/inspect_raw.py --hourly PATH --country PATH

Prints sample lines, field counts, top domain codes, and for the country file the
detected delimiter, header and sample country values. Copy the output into
docs/03_DATA_MODEL.md §1 (step 1.4).
"""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.common.config import load  # noqa: E402

N = 200_000  # lines sampled from the hourly file


def first_file(root: Path, pattern: str) -> Path | None:
    files = sorted(p for p in root.rglob(pattern) if not p.name.endswith(".part"))
    return files[0] if files else None


def inspect_hourly(path: Path, langs: list[str]) -> None:
    print(f"\n=== HOURLY FILE: {path.name} (full scan) ===")
    fields = collections.Counter()
    sizes_zero = 0
    lines = 0
    per_dom = collections.Counter()        # lines per domain code for our languages
    views_dom = collections.Counter()      # views per domain code for our languages
    ns = {l: collections.Counter() for l in langs}
    top = {l: [] for l in langs}
    wanted = set(langs) | {l + ".m" for l in langs}
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i < 3:
                print("  first lines:", line.rstrip())
            lines += 1
            parts = line.rstrip("\n").split(" ")
            fields[len(parts)] += 1
            if len(parts) != 4:
                continue
            dom, title, views, size = parts
            if size == "0":
                sizes_zero += 1
            if dom in wanted and views.isdigit():
                v = int(views)
                per_dom[dom] += 1
                views_dom[dom] += v
                lang = dom.split(".")[0]
                if ":" in title:
                    ns[lang][title.split(":", 1)[0]] += v
                if dom == lang:
                    top[lang].append((v, title))
    print(f"  total lines:          {lines:,}")
    print(f"  fields per line:      {dict(fields)}")
    print(f"  response_size == 0:   {sizes_zero/lines:.1%} of lines")
    print("  our languages (lines / views this hour):")
    for l in langs:
        for d in (l, l + ".m"):
            print(f"    {d:<6} {per_dom[d]:>10,} lines {views_dom[d]:>12,} views")
    for l in langs:
        print(f"  [{l}] top title prefixes before ':' (by views): "
              + ", ".join(f"{k} ({v:,})" for k, v in ns[l].most_common(8)))
        best = sorted(top[l], reverse=True)[:5]
        print(f"  [{l}] top desktop titles: " + ", ".join(f"{t} ({v:,})" for v, t in best))


def inspect_country(path: Path) -> None:
    print(f"\n=== COUNTRY FILE: {path.name} ===")
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
        lines = [next(f, "").rstrip("\n") for _ in range(2000)]
    lines = [l for l in lines if l]
    for l in lines[:5]:
        print("  sample:", l)
    try:
        dialect = csv.Sniffer().sniff("\n".join(lines[:50]), delimiters=",\t;|")
        delim = dialect.delimiter
    except csv.Error:
        delim = "\t" if lines and lines[0].count("\t") > lines[0].count(",") else ","
    has_header = False
    try:
        has_header = csv.Sniffer().has_header("\n".join(lines[:50]))
    except csv.Error:
        pass
    rows = list(csv.reader(io.StringIO("\n".join(lines[:2000])), delimiter=delim))
    widths = collections.Counter(len(r) for r in rows)
    print(f"  delimiter:            {delim!r}")
    print(f"  header row detected:  {has_header}  -> first row: {rows[0] if rows else None}")
    print(f"  columns per row:      {dict(widths)}   (observed layout: 7 = country_name, country_code, project, page_id, page_title, item_id, views)")
    body = rows[1:] if has_header else rows
    if body and len(body[0]) >= 1:
        c = collections.Counter(r[0] for r in body if r)
        print("  country column values: " + ", ".join(f"{k} ({v})" for k, v in c.most_common(10)))
        print("  -> country is an ISO code" if all(len(k) == 2 and k.isupper() for k in list(c)[:10])
              else "  -> country looks like a NAME (not a 2-letter code)")
    if body and len(body[0]) >= 7:
        cc = collections.Counter(r[1] for r in body)
        p = collections.Counter(r[2] for r in body)
        vals = sorted(int(r[6]) for r in body if r[6].lstrip("-").isdigit())
        print("  country code column:  " + ", ".join(f"{k} ({v})" for k, v in cc.most_common(8)))
        print("  project values:       " + ", ".join(f"{k} ({v})" for k, v in p.most_common(8)))
        if vals:
            print(f"  views column:         min {vals[0]}, median {vals[len(vals)//2]}, max {vals[-1]}  (min shows the release threshold)")
    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
        total = sum(1 for _ in f)
    print(f"  total rows in file:   {total:,}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hourly", type=Path)
    ap.add_argument("--country", type=Path)
    a = ap.parse_args()
    cfg = load()
    cache = Path(cfg["paths"]["local_raw_cache"])
    h = a.hourly or first_file(cache / "hourly", "pageviews-*.gz")
    c = a.country or first_file(cache / "country_daily", "*.tsv*")
    if h:
        inspect_hourly(h, cfg["languages"])
    else:
        print("No hourly file found. Run download_hourly.py first.")
    if c:
        inspect_country(c)
    else:
        print("No country file found. Run download_country.py first.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
