"""Evidence for labelling the top 50 worldwide stories (precision@50, step 7.x).

For each story in data/validation/top50_to_label.csv, the lead article's edit history is read from
Wikipedia: how many edits it got in the 5 days around the spike (2 days before first detection to
3 days after) against the 30 days before, and the edit summaries in that window. A real-world event
(a death, a release, a match, an appearance) shows up as a burst of edits whose summaries say what
happened; a bot or counting artefact gets views without edits.

  data/validation/top50_evidence.tsv   rank, event_id, title, lang, lead title, edits around the spike,
                                       edits in the 30 days before, edit summaries

Run inside edge (MongoDB up, internet):   python3 scripts/top50_evidence.py
"""
from __future__ import annotations

import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import conn, load  # noqa: E402

VAL = ROOT / "data" / "validation"


def revisions(client, lang: str, title: str, start: dt.datetime, end: dt.datetime, limit: int) -> list[dict]:
    """Revisions of a page between start and end (newest first)."""
    data = client._get({"action": "query", "prop": "revisions", "titles": title, "redirects": 1,
                        "rvprop": "timestamp|comment|user", "rvlimit": limit,
                        "rvstart": end.strftime("%Y-%m-%dT%H:%M:%SZ"), "rvend": start.strftime("%Y-%m-%dT%H:%M:%SZ")},
                       url=f"https://{lang}.wikipedia.org/w/api.php")
    pages = (data.get("query") or {}).get("pages") or []
    return (pages[0].get("revisions") or []) if pages else []


def main() -> None:
    cfg = load()
    client = WikidataClient(cfg["wikimedia"]["user_agent"], min_interval=0.5)
    from pymongo import MongoClient
    db = MongoClient(conn(cfg)["mongo_uri"], serverSelectionTimeoutMS=5000)["radar"]
    with open(VAL / "top50_to_label.csv", encoding="utf-8") as f:
        top = list(csv.DictReader(f))
    leads = {d["_id"]: d.get("lead") or {} for d in
             db["events"].find({"_id": {"$in": [r["event_id"] for r in top]}}, {"lead": 1})}
    out = []
    for r in top:
        lead = leads.get(r["event_id"], {})
        lang, title = lead.get("lang", "en"), lead.get("title", r["title"])
        first = dt.datetime.strptime(r["first_detected"], "%Y-%m-%d %H:%M")
        around = revisions(client, lang, title, first - dt.timedelta(days=2), first + dt.timedelta(days=3), 100)
        before = revisions(client, lang, title, first - dt.timedelta(days=32), first - dt.timedelta(days=2), 500)
        notes = " || ".join(f"{x['timestamp'][5:16]} {(x.get('comment') or '').strip()[:90]}"
                            for x in reversed(around) if (x.get("comment") or "").strip())[:900]
        out.append([r["rank"], r["event_id"], r["title"], lang, title, len(around), len(before), notes])
        print(f"{r['rank']:>3} {lang} {title[:40]:<40} edits around spike {len(around):>3}, 30 days before {len(before):>3}")
    with open(VAL / "top50_evidence.tsv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["rank", "event_id", "title", "lang", "lead_title", "edits_around", "edits_30d_before", "summaries"])
        w.writerows(out)
    print("written: data/validation/top50_evidence.tsv")


if __name__ == "__main__":
    main()
