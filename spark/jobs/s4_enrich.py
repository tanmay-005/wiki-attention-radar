#!/usr/bin/env python3
"""S4 enrich (step 4.9): Wikidata item, category, country and region for every spiking page.

The set of spiking pages is small (thousands), so it is collected to the driver and
looked up with enrichment/wikidata_client.py. Results are cached in MongoDB:
  radar.pages           one document per page ("<lang>:<title>"), the enrichment result
  radar.wikidata_cache  one document per Wikidata item (claims used for categories/regions)
so a re-run only asks Wikidata about pages and items it has not seen.

Output: /radar/output/page_meta (Parquet)
  lang, page_title, qid, label, category, category_source, country, region_code,
  region_name, city_qid, links (array<string>)

Run inside edge (Mongo must be up: docker compose --profile nosql up -d):
  spark-submit --master local[2] spark/jobs/s4_enrich.py [--refresh] [--no-mongo]
"""
import argparse
import datetime as dt
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from pyspark.sql import functions as F  # noqa: E402

from enrichment.categorise import categorise, load_rules  # noqa: E402
from enrichment.regions import resolve  # noqa: E402
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import conn, load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

SCHEMA = ("lang string, page_title string, qid string, label string, category string, "
          "category_source string, country string, region_code string, region_name string, "
          "city_qid string, links array<string>")
FIELDS = [c.split()[0] for c in SCHEMA.split(", ")]


def mongo_db(uri: str):
    from pymongo import MongoClient
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client["radar"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="ignore cached pages and look everything up again")
    ap.add_argument("--no-mongo", action="store_true", help="run without the Mongo cache")
    a = ap.parse_args()
    t0 = time.time()
    cfg = load()
    out_dir = cfg["paths"]["output"]

    spark = get_spark("S4 enrich", shuffle_partitions=4)
    g = spark.read.parquet(f"{out_dir}/spikes_global").select("lang", "page_title", F.lit(None).cast("string").alias("qid"))
    c = spark.read.parquet(f"{out_dir}/spikes_country").select("lang", "page_title", "qid")
    pages = {}
    for r in g.unionByName(c).collect():
        key = (r["lang"], r["page_title"])
        if r["qid"] or key not in pages:
            pages[key] = r["qid"]
    print(f"spiking pages to enrich: {len(pages):,} (global + country)")

    db = None
    if not a.no_mongo:
        try:
            db = mongo_db(conn(cfg)["mongo_uri"])
        except Exception as e:  # noqa: BLE001
            print(f"WARNING: MongoDB not reachable ({e}); running without the cache")
    client = WikidataClient(cfg["wikimedia"]["user_agent"], cache=db["wikidata_cache"] if db is not None else None,
                            min_interval=cfg["wikimedia"]["wikidata_min_interval_s"])
    rules = load_rules()
    city = set(rules.get("city_classes") or {})

    results, todo = {}, dict(pages)
    if db is not None and not a.refresh:
        ids = [f"{l}:{t}" for l, t in pages]
        for d in db["pages"].find({"_id": {"$in": ids}}):
            lang, title = d["_id"].split(":", 1)
            results[(lang, title)] = {k: d.get(k) for k in FIELDS if k not in ("lang", "page_title")}
            todo.pop((lang, title), None)
    print(f"from cache: {len(results):,}, to look up: {len(todo):,}")

    by_lang = defaultdict(list)
    for (lang, title), qid in todo.items():
        if not qid:
            by_lang[lang].append(title)
    for lang, titles in sorted(by_lang.items()):
        found = client.titles_to_qids(lang, titles)
        for t, q in found.items():
            todo[(lang, t)] = q
        print(f"  {lang}: {len(titles):,} titles -> {sum(1 for q in found.values() if q):,} items")

    items = client.entities([q for q in todo.values() if q])
    # fetch everything categories and regions will ask about in a few big batches
    # (classes, countries, and the P131 chain level by level) instead of item by item
    live = [it for it in items.values() if not it.get("missing")]
    client.entities({q for it in live for k in ("instance_of", "country", "citizenship") for q in (it.get(k) or [])[:5]})
    classes = {q for it in live for k in ("instance_of", "occupation") for q in (it.get(k) or [])[:5]}
    seen_cls = set(classes)
    for _ in range(4):  # classes, occupations and their parents up to three levels, one batch per level
        recs = client.entities(classes)
        classes = {q for r in recs.values() for q in (r.get("subclass_of") or [])} - seen_cls
        seen_cls |= classes
        if not classes:
            break
    frontier = {q for it in live for k in ("located_in", "location", "headquarters") for q in (it.get(k) or [])}
    seen = set()
    for _ in range(5):
        recs = client.entities(frontier - seen)
        seen |= frontier
        frontier = {q for r in recs.values() for q in (r.get("located_in") or [])} - seen
        if not frontier:
            break
    client.entities({q for q in seen if q in client.mem for q in (client.mem[q].get("country") or [])[:1]})
    get = client.entities
    docs = []
    for (lang, title), qid in todo.items():
        item = items.get(qid) if qid else None
        if item and not item.get("missing"):
            category, source = categorise(item, rules, get)
            where = resolve(item, get, city)
            rec = {"qid": qid, "label": item.get("label"), "category": category, "category_source": source,
                   **where, "links": item.get("links") or []}
        else:
            rec = {"qid": qid, "label": None, "category": rules.get("default", "Other"),
                   "category_source": "no item", "country": None, "region_code": None,
                   "region_name": None, "city_qid": None, "links": []}
        results[(lang, title)] = rec
        docs.append({"_id": f"{lang}:{title}", **rec, "fetched_at": dt.datetime.now(dt.timezone.utc)})
    if db is not None and docs:
        from pymongo import ReplaceOne
        db["pages"].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs])

    rows = [{"lang": l, "page_title": t, **{k: rec.get(k) for k in FIELDS if k not in ("lang", "page_title")}}
            for (l, t), rec in results.items()]
    spark.createDataFrame(rows, schema=SCHEMA).coalesce(1).write.mode("overwrite").parquet(f"{out_dir}/page_meta")

    n = len(rows)
    with_qid = sum(1 for r in rows if r["qid"])
    cats = Counter(r["category"] for r in rows)
    print(f"\nwrote {out_dir}/page_meta: {n:,} pages in {time.time() - t0:.0f} s ({client.calls} Wikidata/Wikipedia API calls)")
    print(f"T-D7 pages with a Wikidata item: {with_qid:,} / {n:,} = {100 * with_qid / max(n, 1):.1f}%  "
          f"{'PASS' if with_qid >= 0.9 * n else 'FAIL'} (needs >= 90%)")
    print("categories: " + ", ".join(f"{k} {v:,} ({100 * v / max(n, 1):.0f}%)" for k, v in cats.most_common()))
    print(f"with a country: {sum(1 for r in rows if r['country']):,}, with a region: {sum(1 for r in rows if r['region_code']):,}")
    src = Counter(r["category_source"] for r in rows)
    print("category source: " + ", ".join(f"{k} {v:,}" for k, v in src.most_common()))
    print("\n'Other' pages, a sample to improve category_rules.yaml:")
    for r in [r for r in rows if r["category"] == "Other" and r["qid"]][:25]:
        print(f"  {r['lang']}  {r['qid']:<11} {r['page_title']}")
    spark.stop()


if __name__ == "__main__":
    main()
