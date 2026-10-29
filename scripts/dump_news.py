"""Write the news of the detection period, as Wikipedia recorded it, to plain files for picking the
known events of the validation (step 7.x). Sources: the daily Portal:Current events pages and the
deaths list for the month.

  data/validation/news_2026-10.tsv     date, section, topic, text, links
  data/validation/deaths_2026-10.tsv   date, person, text

The known events are chosen from these lists by hand, for being notable and having a clear date,
without looking at what the radar found.

Run inside edge (needs internet):   python3 scripts/dump_news.py
"""
from __future__ import annotations

import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from enrichment.context import (deaths_title, fetch_wikitext, parse_current_events,  # noqa: E402
                                parse_deaths, portal_title)
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import load  # noqa: E402


def main() -> None:
    cfg = load()
    per = cfg["period"]
    start = dt.date.fromisoformat(str(per["detect_start"]))
    end = dt.date.fromisoformat(str(per["hourly_end"]))
    days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    months = sorted({(d.year, d.month) for d in days})
    client = WikidataClient(cfg["wikimedia"]["user_agent"], min_interval=cfg["wikimedia"]["wikidata_min_interval_s"])
    pages = fetch_wikitext(client, [portal_title(d) for d in days] + [deaths_title(y, m) for y, m in months])

    out = ROOT / "data" / "validation"
    out.mkdir(parents=True, exist_ok=True)
    tag = f"{start:%Y-%m}"
    n = 0
    with open(out / f"news_{tag}.tsv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["date", "section", "topic", "text", "links"])
        for d in days:
            for it in parse_current_events(pages.get(portal_title(d), ""), d):
                w.writerow([d, it["section"] or "", it["topic"] or "", it["text"],
                            " | ".join(dict.fromkeys(it["links"] + it["topic_links"]))])
                n += 1
    m = 0
    with open(out / f"deaths_{tag}.tsv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["date", "person", "text"])
        for y, mo in months:
            for it in parse_deaths(pages.get(deaths_title(y, mo), ""), y, mo):
                if start <= it["date"] <= end:
                    w.writerow([it["date"], it["person"], it["text"]])
                    m += 1
    got = sum(portal_title(d) in pages for d in days)
    print(f"news: {n} lines from {got} of {len(days)} days -> data/validation/news_{tag}.tsv")
    print(f"deaths: {m} -> data/validation/deaths_{tag}.tsv")


if __name__ == "__main__":
    main()
