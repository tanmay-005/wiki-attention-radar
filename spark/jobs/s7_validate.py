#!/usr/bin/env python3
"""S7 validate (step 7.x): how many known events the radar found, how fast, at what rank, and how
many of its top 50 worldwide stories are real events (docs/09_TESTING_QA.md section 5).

Inputs
  /radar/output/events                         S5 events
  data/validation/known_events_2026-10.csv     id, name, date, scope, expected_titles, notes
                                               (expected_titles: English Wikipedia titles or QIDs, separated by |)
  data/validation/labels_top50.csv             optional: event_id, is_real (1/0), note
Outputs
  MongoDB radar.validation                     one document per known event (read by the Validation page)
  MongoDB radar.meta  _id "validation"         recall, delay, ranks, precision@50
  benchmarks/results/validation_2026-10.csv    the same table for the report
  data/validation/top50_to_label.csv           the top 50 worldwide stories, to label for precision@50

Run inside edge (MongoDB up, internet for Wikidata):
  spark-submit --master local[2] spark/jobs/s7_validate.py 2>/dev/null
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import conn, load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402
from spark.common.validation import precision, summary, top_events, validate  # noqa: E402

VAL = ROOT / "data" / "validation"
QID = re.compile(r"^Q\d+$")


def read_known(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not (r.get("id") or "").strip() or r["id"].startswith("#"):
                continue
            rows.append({"id": r["id"].strip(), "name": r["name"].strip(),
                         "date": dt.date.fromisoformat(r["date"].strip()), "scope": (r.get("scope") or "G").strip(),
                         "expected_titles": r["expected_titles"].strip(), "group": (r.get("group") or "").strip(),
                         "notes": (r.get("notes") or "").strip()})
    return rows


def resolve(known: list[dict], client: WikidataClient) -> dict[str, set[str]]:
    """Known event id -> Wikidata items of its expected titles (QIDs are taken as they are)."""
    titles = sorted({t.strip() for k in known for t in k["expected_titles"].split("|")
                     if t.strip() and not QID.match(t.strip())})
    found = client.titles_to_qids("en", titles)
    out = {}
    for k in known:
        qs = set()
        for t in (x.strip() for x in k["expected_titles"].split("|")):
            if QID.match(t):
                qs.add(t)
            elif found.get(t):
                qs.add(found[t])
            elif t:
                print(f"  WARNING {k['id']}: no Wikidata item for the title {t!r}")
        out[k["id"]] = qs
    return out


def as_mongo(v):
    return dt.datetime.combine(v, dt.time()) if isinstance(v, dt.date) and not isinstance(v, dt.datetime) else v


def explain_misses(spark, cfg, res: list[dict]) -> None:
    """Why a known event was not found: detected at another time, or how many views its English
    article got per hour at most (the English threshold is min_views; the article may not exist yet)."""
    from pyspark.sql import functions as F
    from spark.common.spikes import params
    per = cfg["period"]
    todo = [r for r in res if not r["detected"] and not r["why_missed"]]
    titles = sorted({t.strip() for r in todo for t in r["expected_titles"].split("|") if t.strip() and not QID.match(t)})
    if not titles:
        return
    wh = f"{cfg['paths']['warehouse']}/radar.db"
    peak = {row["page_title"]: (row["mx"], row["first"]) for row in
            spark.read.orc(f"{wh}/hourly_views")
                 .where(F.col("dt").between(str(per["detect_start"]), str(per["hourly_end"])))
                 .where((F.col("lang") == "en") & F.col("page_title").isin(titles))
                 .groupBy("page_title").agg(F.max("views").alias("mx"), F.min("hour_ts").alias("first")).collect()}
    floor = params(cfg, "global_hourly", "en")["min_views"]
    for r in todo:
        got = [(t, peak[t]) for t in (x.strip() for x in r["expected_titles"].split("|")) if t in peak]
        if not got:
            r["why_missed"] = "no English Wikipedia views in the period (article missing or created later)"
            continue
        t, (mx, first) = max(got, key=lambda x: x[1][0])
        r["why_missed"] = (f"peak {mx:,} views/h on English Wikipedia ({t}), below the {floor:,}/h threshold"
                           if mx < floor else f"peak {mx:,} views/h on English Wikipedia ({t}), but no spike "
                           "against its own normal level (busy before the day too)")


def report(res: list[dict], s: dict) -> None:
    print(f"\nknown events: {s['known']}, found: {s['found']}  ->  recall {100 * s['recall']:.0f}%  "
          f"{'PASS' if s['recall'] >= 0.75 else 'FAIL'} (target >= 75%)")
    for g, v in s["by_group"].items():
        print(f"  {g:<28} {v['found']:>3} of {v['known']:<3} = {100 * v['found'] / v['known']:.0f}%")
    print(f"median detection delay: {s['median_delay_hours']} h after the start of the listed day (UTC); "
          f"median rank in its scope: {s['median_rank']}; in the top 10 that day: {s['top10']}")
    print(f"Indian events found: {s['india_found']}, ranked higher in R:IN than in G: "
          f"{s['india_higher_in_r_in']} of {s['india_in_r_in']} that are in R:IN")
    print(f"\n{'id':<6}{'date':<12}{'scope':<9}{'found':<7}{'delay h':>8}{'rank':>6}{'rank G':>8}{'R:IN':>6}  name")
    for r in res:
        f = lambda v: "-" if v is None else str(v)  # noqa: E731
        print(f"{r['_id']:<6}{r['date']!s:<12}{r['scope']:<9}{'yes' if r['detected'] else 'NO':<7}"
              f"{f(r['delay_hours']):>8}{f(r['rank']):>6}{f(r['rank_g']):>8}{f(r['rank_r_in']):>6}  {r['name']}"
              + ("" if r["detected"] else f"   <- {r['why_missed'] or 'no Wikidata item for the title'}"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--known", default=str(VAL / "known_events_2026-10.csv"))
    ap.add_argument("--labels", default=str(VAL / "labels_top50.csv"))
    a = ap.parse_args()

    cfg = load()
    out_dir = cfg["paths"]["output"]
    spark = get_spark("S7 validate", shuffle_partitions=8)
    events = [r.asDict(recursive=True) for r in spark.read.parquet(f"{out_dir}/events").collect()]
    print(f"events: {len(events):,}")

    from pymongo import MongoClient
    db = MongoClient(conn(cfg)["mongo_uri"], serverSelectionTimeoutMS=5000)["radar"]

    # 1. known events -------------------------------------------------------------
    if not Path(a.known).exists():
        print(f"no {a.known} yet: only the top 50 list is written")
        known = []
    else:
        known = read_known(Path(a.known))
    client = WikidataClient(cfg["wikimedia"]["user_agent"], min_interval=cfg["wikimedia"]["wikidata_min_interval_s"])
    qids = resolve(known, client) if known else {}
    res = validate(known, qids, events)
    if known:
        explain_misses(spark, cfg, res)
    spark.stop()
    s = summary(res)
    if known:
        report(res, s)

    # 2. top 50 worldwide stories and precision@50 --------------------------------------
    top = top_events(events, 50)
    labels = {}
    if Path(a.labels).exists():
        with open(a.labels, encoding="utf-8") as f:
            labels = {r["event_id"]: r for r in csv.DictReader(f)}
    docs = {d["_id"]: d for d in db["events"].find({"_id": {"$in": [e["event_id"] for e in top]}},
                                                   {"about": 1, "news": {"$slice": 1}})}
    with open(VAL / "top50_to_label.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "event_id", "title", "first_detected", "ratio", "peak_views", "suspicious",
                    "description", "news", "is_real", "note"])
        for i, e in enumerate(top, start=1):
            d = docs.get(e["event_id"], {})
            news = (d.get("news") or [{}])[0].get("text") or ""
            lab = labels.get(e["event_id"], {})
            w.writerow([i, e["event_id"], e["title"], f"{e['first_detected']:%Y-%m-%d %H:%M}", round(e["ratio"]),
                        e["peak_views"], int(bool(e["suspicious"])), (d.get("about") or {}).get("description") or "",
                        news[:200], lab.get("is_real", ""), lab.get("note", "")])
    p = precision([labels[e["event_id"]] for e in top if e["event_id"] in labels])
    print(f"\ntop 50 worldwide stories written to data/validation/top50_to_label.csv")
    if p["labelled"]:
        print(f"precision@50: {p['real']} of {p['labelled']} labelled are real = {100 * p['precision']:.0f}%  "
              f"{'PASS' if p['precision'] >= 0.8 else 'FAIL'} (target >= 80%)")
    else:
        print("precision@50: not labelled yet (fill is_real in labels_top50.csv)")

    # 3. write ------------------------------------------------------------------------
    run = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    if not known:
        return
    db["validation"].delete_many({})
    db["validation"].insert_many([{k: as_mongo(v) for k, v in r.items()} | {"run": run} for r in res])
    db["meta"].replace_one({"_id": "validation"}, {"_id": "validation", **s, "precision50": p, "run": run},
                           upsert=True)
    res_dir = ROOT / "benchmarks" / "results"
    res_dir.mkdir(parents=True, exist_ok=True)
    cols = ["_id", "name", "date", "scope", "group", "detected", "matched_title", "first_detected", "delay_hours", "rank",
            "rank_g", "rank_r_in", "why_missed", "expected_titles", "expected_qids", "notes"]
    with open(res_dir / "validation_2026-10.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id" if c == "_id" else c for c in cols])
        for r in res:
            w.writerow(["|".join(r[c]) if isinstance(r[c], list) else ("" if r[c] is None else r[c]) for c in cols])
    print(f"\nwritten: MongoDB radar.validation ({len(res)}), radar.meta 'validation', "
          f"benchmarks/results/validation_2026-10.csv")


if __name__ == "__main__":
    main()
