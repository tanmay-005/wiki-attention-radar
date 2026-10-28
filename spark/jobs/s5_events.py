#!/usr/bin/env python3
"""S5 events (step 4.11): group spike episodes into events, score them, give them scopes.

Inputs : spikes_global (S2), spikes_country (S3), page_meta (S4), series_global (S1, for curves)
Output : /radar/output/events (Parquet), one row per event; see spark/common/events.py
         for the grouping rules and docs/03_DATA_MODEL.md section 5.1 for the fields.

Spike episodes are few (thousands), so grouping runs on the driver with union-find.
The hourly curve of each event's lead article (48 hours either side of the peak)
is cut from series_global with a broadcast join.

Run inside edge (after S1-S4):
  spark-submit --master local[3] --driver-memory 3g spark/jobs/s5_events.py
"""
import datetime as dt
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import conn, load  # noqa: E402
from spark.common.events import build_event, group_events, n_items  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

SCHEMA = (
    "event_id string, title string, category string, first_detected timestamp, peak_time timestamp, "
    "day date, score double, ratio double, peak_views long, baseline_views double, langs array<string>, "
    "loc_country string, loc_region string, loc_region_name string, loc_city_qid string, "
    "articles array<struct<qid:string,lang:string,title:string,peak_views:long,ratio:double,role:string>>, "
    "countries array<struct<code:string,ratio:double,peak_views:long,day:date>>, "
    "states array<struct<code:string,lang:string,ratio:double,peak_views:long,baseline:double,"
    "first:timestamp,peak:timestamp>>, scopes array<string>, "
    "suspicious boolean, members int, lead_lang string, lead_title string, "
    "curve array<struct<t:timestamp,v:long,b:double>>")
META = ["qid", "label", "category", "country", "region_code", "region_name", "city_qid", "links"]


def country_qids(cfg) -> set[str]:
    """Items that are countries (have an ISO 3166-1 code), from the S4 cache; used as 'generic' links."""
    try:
        from pymongo import MongoClient
        db = MongoClient(conn(cfg)["mongo_uri"], serverSelectionTimeoutMS=3000)["radar"]
        return {d["_id"] for d in db["wikidata_cache"].find({"iso3166_1": {"$ne": None}}, {"_id": 1})}
    except Exception as e:  # noqa: BLE001
        print(f"note: no Mongo cache for country items ({e}); relying on the hub rule")
        return set()


def main() -> None:
    t0 = time.time()
    cfg = load()
    out = cfg["paths"]["output"]
    ev_cfg = cfg["events"]
    cap = cfg["spikes"]["ratio_cap"]

    spark = get_spark("S5 events", shuffle_partitions=8)
    meta = spark.read.parquet(f"{out}/page_meta")
    g = spark.read.parquet(f"{out}/spikes_global").join(meta, ["lang", "page_title"], "left")
    c = (spark.read.parquet(f"{out}/spikes_country").withColumnRenamed("qid", "c_qid")
         .withColumnRenamed("country", "reader_country").drop("country_name")
         .join(meta.drop("qid"), ["lang", "page_title"], "left"))

    nodes = []
    for r in g.collect():
        d = r.asDict()
        nodes.append({"kind": "global", "lang": d["lang"], "page_title": d["page_title"], "start": d["start"],
                      "peak": d["peak_hour"], "peak_views": d["peak_views"], "baseline": d["baseline"],
                      "ratio": d["ratio"], "suspicious": d["suspicious"], **{k: d.get(k) for k in META}})
    for r in c.collect():
        d = r.asDict()
        nodes.append({"kind": "country", "lang": d["lang"], "page_title": d["page_title"],
                      "start": dt.datetime.combine(d["start"], dt.time()), "peak": dt.datetime.combine(d["peak_day"], dt.time(12)),
                      "peak_views": d["peak_views"], "baseline": d["baseline"], "ratio": d["ratio"],
                      "suspicious": False, "reader_country": d["reader_country"], "qid": d["c_qid"],
                      **{k: d.get(k) for k in META if k != "qid"}})
    for n in nodes:
        n["links"] = list(n.get("links") or [])
    print(f"nodes: {sum(n['kind'] == 'global' for n in nodes):,} global episodes + "
          f"{sum(n['kind'] == 'country' for n in nodes):,} country episodes")

    stats = {}
    groups = group_events(nodes, ev_cfg["group_window_hours"], ev_cfg["same_entity_window_hours"], country_qids(cfg),
                          max_members=ev_cfg["max_members"], stats=stats)
    events, seen = [], Counter()
    for idx in groups:
        e = build_event([nodes[i] for i in idx], cap)
        e["_items"] = n_items(nodes, idx)
        seen[e["event_id"]] += 1
        if seen[e["event_id"]] > 1:
            e["event_id"] += f"_{seen[e['event_id']]}"
        events.append(e)
    print(f"events: {len(events):,} ({stats['split']} oversized groups split back into single items)")

    # curve of each lead article: 48 hours either side of the peak, with its baseline
    leads = [(e["lead_lang"], e["lead_title"], e["peak_time"], e["baseline_views"], e["event_id"])
             for e in events if "G" in e["scopes"]]
    curves = {}
    if leads:
        ldf = spark.createDataFrame(leads, "lang string, page_title string, peak timestamp, b double, event_id string")
        pts = (spark.read.parquet(f"{out}/series_global").join(F.broadcast(ldf), ["lang", "page_title"])
               .where(F.col("hour_ts").between(F.col("peak") - F.expr("INTERVAL 48 HOURS"),
                                               F.col("peak") + F.expr("INTERVAL 48 HOURS")))
               .select("event_id", "hour_ts", "views", "b").collect())
        for p in pts:
            curves.setdefault(p["event_id"], []).append({"t": p["hour_ts"], "v": int(p["views"]), "b": float(p["b"])})
    for e in events:
        e["curve"] = sorted(curves.get(e["event_id"], []), key=lambda x: x["t"])

    df = spark.createDataFrame([{k: e.get(k) for k in [c.split()[0] for c in SCHEMA.split(", ")]} for e in events],
                               schema=SCHEMA)
    df.coalesce(1).write.mode("overwrite").parquet(f"{out}/events")

    # checks and a look at the result
    sizes = Counter(e["members"] for e in events)
    biggest = max(events, key=lambda e: (e["_items"], e["members"])) if events else None
    dup = sum(v > 1 for v in seen.values())
    print(f"\nwrote {out}/events in {time.time() - t0:.0f} s")
    print(f"T-D8 unique event ids: {'PASS' if dup == 0 else f'{dup} collisions renamed'}; "
          f"largest event: {biggest['_items'] if biggest else 0} different items "
          f"({'PASS' if not biggest or biggest['_items'] <= ev_cfg['max_members'] else 'FAIL'}, limit {ev_cfg['max_members']}); "
          f"most spikes in one event: {max(e['members'] for e in events) if events else 0}")
    print("events by size:", ", ".join(f"{k} member(s): {v:,}" for k, v in sorted(sizes.items())[:8]))
    if biggest:
        print(f"largest event: {biggest['title']} ({biggest['day']}), {biggest['_items']} items, {biggest['members']} spikes, "
              f"articles: {', '.join(a['title'] for a in (biggest['articles'] or [])[:12])}")
    print("by category:", ", ".join(f"{k} {v:,}" for k, v in Counter(e["category"] for e in events).most_common()))
    print(f"with a location: {sum(1 for e in events if e['loc_country']):,}, with a region: "
          f"{sum(1 for e in events if e['loc_region']):,}, suspicious: {sum(e['suspicious'] for e in events):,}")

    show = spark.read.parquet(f"{out}/events")
    print("\ntop 20 events (global, not suspicious):")
    (show.where(F.array_contains("scopes", "G") & ~F.col("suspicious")).orderBy(F.col("score").desc())
         .select("day", "category", F.col("title").substr(1, 40).alias("title"), "langs",
                 F.round("ratio", 1).alias("ratio"), "peak_views", "members",
                 F.concat_ws(" ", "scopes").substr(1, 30).alias("scopes"), F.round("score", 0).alias("score"))
         .show(20, truncate=False))
    print("about India (A:IN), top 10:")
    (show.where(F.array_contains("scopes", "A:IN")).orderBy(F.col("score").desc())
         .select("day", "category", F.col("title").substr(1, 45).alias("title"), "loc_region",
                 F.round("score", 0).alias("score")).show(10, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
