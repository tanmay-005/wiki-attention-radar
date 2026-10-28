#!/usr/bin/env python3
"""S5b context: what each event is about, and the news that explains it (enrichment/context.py).

For every event in /radar/output/events:
  about  short description, first sentences and thumbnail of its English article if it has
         one, otherwise of its lead article (cached in Mongo radar.page_info)
  news   matching lines from Wikipedia's "Portal:Current events" day pages and "Deaths in
         <month>" pages, matched through Wikidata items
Result: Mongo radar.event_context (_id = event_id); S6 copies it into radar.events.

Run inside edge after S5, then reload the events:
  spark-submit --master local[2] spark/jobs/s5b_context.py 2>/dev/null
  bash scripts/run_s6.sh --skip-series 2>/dev/null
"""
import argparse
import datetime as dt
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from enrichment.context import (deaths_title, fetch_about, fetch_wikitext, match_news,  # noqa: E402
                                parse_current_events, parse_deaths, portal_title)
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import conn, load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402


def about_target(e: dict) -> tuple[str, str]:
    en = next((a for a in e["articles"] or [] if a["lang"] == "en"), None)
    return (en["lang"], en["title"]) if en else (e["lead_lang"], e["lead_title"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="fetch article descriptions again")
    a = ap.parse_args()
    t0 = time.time()
    cfg = load()
    per = cfg["period"]
    spark = get_spark("S5b context", shuffle_partitions=4)
    events = [r.asDict(recursive=True) for r in spark.read.parquet(f"{cfg['paths']['output']}/events").collect()]
    spark.stop()
    print(f"events: {len(events):,}")

    from pymongo import MongoClient, ReplaceOne
    db = MongoClient(conn(cfg)["mongo_uri"], serverSelectionTimeoutMS=5000)["radar"]
    client = WikidataClient(cfg["wikimedia"]["user_agent"], cache=db["wikidata_cache"],
                            min_interval=cfg["wikimedia"]["wikidata_min_interval_s"])

    # 1. about: one article per event, 20 per API call, cached
    targets = {e["event_id"]: about_target(e) for e in events}
    cache = {} if a.refresh else {d["_id"]: d for d in db["page_info"].find(
        {"_id": {"$in": [f"{l}:{t}" for l, t in set(targets.values())]}})}
    todo = defaultdict(list)
    for lang, title in set(targets.values()):
        if f"{lang}:{title}" not in cache:
            todo[lang].append(title)
    for lang, titles in sorted(todo.items()):
        got = fetch_about(client, lang, titles)
        docs = [{"_id": f"{lang}:{t}", **got.get(t, {"lang": lang, "title": t, "description": None,
                                                     "qid": None, "extract": None, "thumbnail": None})}
                for t in titles]
        if docs:
            db["page_info"].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs])
        cache.update({d["_id"]: d for d in docs})
        print(f"  about {lang}: {len(titles):,} articles, {len(got):,} found")

    # 1b. Japanese, Hindi, Tamil and Portuguese Wikipedia have no short descriptions of their own:
    #     use the English description of the Wikidata item instead (50 items per call, cached)
    needed = {}
    for key in {"%s:%s" % t for t in targets.values()}:
        d = cache.get(key)
        if d and not d.get("description") and d.get("qid") and not d.get("wd_checked"):
            needed.setdefault(d["qid"], []).append(key)
    qids = sorted(needed)
    filled = 0
    for i in range(0, len(qids), 50):
        chunk = qids[i:i + 50]
        ents = client._get({"action": "wbgetentities", "ids": "|".join(chunk), "props": "descriptions",
                            "languages": "en"}).get("entities") or {}
        updates = []
        for q in chunk:
            desc = (((ents.get(q) or {}).get("descriptions") or {}).get("en") or {}).get("value")
            for key in needed[q]:
                cache[key].update({"description": desc or None, "wd_checked": True})
                filled += bool(desc)
                updates.append(cache[key])
        db["page_info"].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in updates])
    if qids:
        print(f"  descriptions from Wikidata for other languages: {filled:,} of {sum(len(v) for v in needed.values()):,}")

    # 2. the day's news and deaths, as Wikipedia recorded them
    start = dt.date.fromisoformat(str(per["detect_start"])) - dt.timedelta(days=3)
    end = min(dt.date.fromisoformat(str(per["hourly_end"])) + dt.timedelta(days=2), dt.date.today())
    days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    months = sorted({(d.year, d.month) for d in days})
    pages = fetch_wikitext(client, [portal_title(d) for d in days] + [deaths_title(y, m) for y, m in months])
    news, deaths = [], []
    for d in days:
        news += parse_current_events(pages.get(portal_title(d), ""), d)
    for y, m in months:
        deaths += [x for x in parse_deaths(pages.get(deaths_title(y, m), ""), y, m) if start <= x["date"] <= end]
    print(f"news pages: {sum(portal_title(d) in pages for d in days)} of {len(days)} days, {len(news):,} news lines; "
          f"deaths: {len(deaths):,}")
    titles = sorted({t for it in news for t in it["links"] + it["topic_links"]} | {x["person"] for x in deaths})
    qid_of = client.titles_to_qids("en", titles)

    # 3. match and store
    docs, with_news, kinds = [], 0, Counter()
    for e in events:
        about = cache.get("%s:%s" % targets[e["event_id"]])
        qids = {x["qid"] for x in e["articles"] or [] if x.get("qid")}
        if about and about.get("qid"):
            qids.add(about["qid"])
        first, peak = e["first_detected"].date(), e["peak_time"].date()
        found = [dict(x, kind="death") for x in match_news(qids, first, peak, deaths, qid_of, before=3, after=1, limit=1)]
        found += [dict(x, kind="news") for x in match_news(qids, first, peak, news, qid_of)]
        for x in found:
            kinds[x["kind"]] += 1
        with_news += bool(found)
        docs.append({"_id": e["event_id"],
                     "about": {k: v for k, v in (about or {}).items() if k not in ("_id", "wd_checked")} or None,
                     "news": [{"date": dt.datetime.combine(x["date"], dt.time()), "kind": x["kind"],
                               "section": x.get("section"), "topic": x.get("topic"), "text": x["text"]}
                              for x in found][:3]})
    coll = db["event_context"]
    coll.delete_many({})
    if docs:
        coll.insert_many(docs, ordered=False)

    with_about = sum(1 for d in docs if d["about"] and (d["about"].get("description") or d["about"].get("extract")))
    print(f"\nevent_context: {len(docs):,} events, {with_about:,} with a description ({100 * with_about / len(docs):.0f}%), "
          f"{with_news:,} with a news line ({100 * with_news / len(docs):.0f}%: {kinds['news']:,} news, {kinds['death']:,} deaths)")
    print(f"{client.calls} API calls, {time.time() - t0:.0f} s")
    print("\ntop 15 global events and their context:")
    top = sorted((e for e in events if "G" in e["scopes"] and not e["suspicious"]), key=lambda e: -e["score"])[:15]
    ctx = {d["_id"]: d for d in docs}
    for e in top:
        c = ctx[e["event_id"]]
        desc = (c["about"] or {}).get("description") or "-"
        line = c["news"][0]["text"] if c["news"] else "(no news line)"
        print(f"  {e['title'][:34]:<34} | {desc[:34]:<34} | {line[:90]}")


if __name__ == "__main__":
    main()
