#!/usr/bin/env python3
"""S6 load serving (step 5.x): /radar/output -> Cassandra (keyspace radar) and MongoDB (radar.events).

Cassandra, written with the Spark Cassandra connector (large tables):
  article_hourly         hourly views + baseline for every global article in an event (all 504 hours)
  article_country_daily  daily views + baseline for every (item, country) with a country spike (31 days)
Cassandra, written with the Python driver and prepared statements (small tables):
  event_rank             every scope x active day x ('ALL', category)
  trending_now           top 20 per scope on the last day, INSERT ... USING TTL
  spike_counts           counters, UPDATE ... SET spikes = spikes + 1
  region_index           regions that have events about them
MongoDB: events (ReplaceOne upsert by _id; documents from earlier runs are removed), meta.

The small tables are truncated first, so a re-run gives the same result.
Baselines follow S2/S3: median of the previous 168 hours (hourly) or 14 days (country).

Run inside edge (Cassandra and MongoDB up):   bash scripts/run_s6.sh [--skip-series] [--dry-run]
"""
import argparse
import datetime as dt
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import conn, load  # noqa: E402
from spark.common.serving import (counter_keys, mongo_doc, rank_rows, region_index,  # noqa: E402
                                  trending)
from spark.common.session import get_spark  # noqa: E402
from spark.common.spikes import robust_baseline  # noqa: E402

KEYSPACE = "radar"


def hourly_baselines(window: int, detect_start: dt.datetime):
    def per_article(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("hour_ts")
        hours = pd.to_datetime(pdf["hour_ts"]).dt.tz_localize(None).to_numpy()
        views = pdf["views"].to_numpy(dtype=float)
        base = np.full(len(views), np.nan)
        first = int((hours < np.datetime64(detect_start)).sum())
        for start in range(first, len(views), 24):           # one baseline per day, as in S2
            base[start:start + 24] = robust_baseline(views[max(0, start - window):start])[0]
        return pd.DataFrame({"lang": pdf["lang"], "title": pdf["page_title"], "hour": pdf["hour_ts"],
                             "views": views, "baseline": base})
    return per_article


def country_baselines(window: int, days: list[dt.date]):
    def per_pair(pdf: pd.DataFrame) -> pd.DataFrame:
        got = dict(zip(pd.to_datetime(pdf["day"]).dt.date, pdf["views"].astype(float)))
        views = np.array([got.get(d, 0.0) for d in days])      # a missing day was under the 90-view floor
        base = np.full(len(days), np.nan)
        for i in range(window, len(days)):
            base[i] = robust_baseline(views[i - window:i])[0]
        return pd.DataFrame({"qid": pdf["qid"].iat[0], "country": pdf["country"].iat[0], "day": days,
                             "views": views, "baseline": base})
    return per_pair


def write_cassandra(df, table: str) -> None:
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table).mode("append").save())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-series", action="store_true", help="only events (Cassandra small tables + MongoDB)")
    ap.add_argument("--dry-run", action="store_true", help="build everything, write nothing")
    a = ap.parse_args()

    t0 = time.time()
    cfg = load()
    c = conn(cfg)
    out = cfg["paths"]["output"]
    per = cfg["period"]
    last_day = dt.date.fromisoformat(str(per["hourly_end"]))
    spark = get_spark("S6 load_serving", shuffle_partitions=12)

    events = [r.asDict(recursive=True) for r in spark.read.parquet(f"{out}/events").collect()]
    if not events:
        raise SystemExit(f"no events in {out}/events: run S5 first")
    country_names = {r["country"]: r["country_name"] for r in
                     spark.read.parquet(f"{out}/spikes_country").select("country", "country_name").distinct().collect()}
    print(f"events: {len(events):,}")

    from pymongo import MongoClient, ReplaceOne
    mdb = MongoClient(c["mongo_uri"], serverSelectionTimeoutMS=5000)[KEYSPACE]
    city_qids = sorted({e["loc_city_qid"] for e in events if e.get("loc_city_qid")})
    try:          # city names come from the Wikidata cache S4 filled
        city_names = {d["_id"]: d.get("label") for d in
                      mdb["wikidata_cache"].find({"_id": {"$in": city_qids}}, {"label": 1})}
    except Exception as e:  # noqa: BLE001
        if not a.dry_run:
            raise
        print(f"note: MongoDB not reachable ({type(e).__name__}); city names left empty in the dry run")
        city_names = {}

    ranks = [row for e in events for row in rank_rows(e, country_names)]
    counters = [k for e in events for k in counter_keys(e)]
    regions = region_index(events)
    trend = trending(events, last_day)
    run = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    try:          # what each event is about and its news lines, from S5b (optional)
        context = {d["_id"]: d for d in mdb["event_context"].find({})}
    except Exception:  # noqa: BLE001
        context = {}
    docs = [mongo_doc(e, country_names, city_names, run, context.get(e["event_id"])) for e in events]
    print(f"context from S5b: {sum(1 for e in events if e['event_id'] in context):,} of {len(events):,} events")
    print(f"event_rank rows: {len(ranks):,}, spike_counts increments: {len(counters):,}, "
          f"regions: {len(regions):,}, trending_now rows ({last_day}): {len(trend):,}, Mongo documents: {len(docs):,}")

    if not a.dry_run:
        from cassandra.cluster import Cluster
        from cassandra.concurrent import execute_concurrent_with_args
        cluster = Cluster([c["cassandra_host"]], port=int(c["cassandra_port"]))
        s = cluster.connect(KEYSPACE)
        s.default_timeout = 60

        def run_all(cql: str, params: list[tuple], what: str) -> None:
            t = time.time()
            stmt = s.prepare(cql)
            execute_concurrent_with_args(s, stmt, params, concurrency=64, raise_on_first_error=True)
            print(f"  {what}: {len(params):,} in {time.time() - t:.0f} s")

        print("Cassandra (Python driver, prepared statements):")
        for table in ("event_rank", "trending_now", "spike_counts", "region_index"):
            s.execute(f"TRUNCATE {table}", timeout=120)
        run_all("INSERT INTO event_rank (scope, day, category, score, event_id, title, ratio, peak_views, first_seen, "
                "langs, place, sparkline, suspicious) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ranks, "event_rank")
        run_all("INSERT INTO trending_now (scope, score, event_id, title, ratio) VALUES (?, ?, ?, ?, ?) USING TTL 172800",
                trend, "trending_now (TTL 48 h)")
        run_all("UPDATE spike_counts SET spikes = spikes + 1 WHERE scope = ? AND day = ? AND category = ?",
                counters, "spike_counts (counter)")
        run_all("INSERT INTO region_index (country, region_code, region_name, event_count) VALUES (?, ?, ?, ?)",
                regions, "region_index")

        t = time.time()
        mdb["events"].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
        stale = mdb["events"].delete_many({"pipeline_run": {"$ne": run}}).deleted_count
        mdb["meta"].replace_one({"_id": "serving"}, {
            "_id": "serving", "pipeline_run": run, "as_of": str(last_day),
            "detect_start": str(per["detect_start"]), "data_start": str(per["hourly_start"]),
            "categories": sorted({e["category"] for e in events}), "events": len(docs)}, upsert=True)
        print(f"MongoDB: {len(docs):,} events upserted, {stale:,} from earlier runs removed, meta updated "
              f"({time.time() - t:.0f} s)")

    if not a.skip_series:
        detect_start = dt.datetime.fromisoformat(f"{per['detect_start']}T00:00:00")
        window = cfg["spikes"]["global_hourly"]["baseline_hours"]
        arts = spark.createDataFrame(sorted({(x["lang"], x["title"]) for e in events for x in (e["articles"] or [])}),
                                     "lang string, page_title string")
        hourly = (spark.read.parquet(f"{out}/series_global").join(F.broadcast(arts), ["lang", "page_title"])
                  .groupBy("lang", "page_title")
                  .applyInPandas(hourly_baselines(window, detect_start),
                                 "lang string, title string, hour timestamp, views double, baseline double")
                  .select("lang", "title", "hour", F.col("views").cast("int").alias("views"),
                          F.col("baseline").cast("int").alias("baseline")))

        c_start = dt.date.fromisoformat(str(per["country_start"]))
        days = [c_start + dt.timedelta(days=i) for i in range((last_day - c_start).days + 1)]
        pairs = spark.read.parquet(f"{out}/spikes_country").select("qid", "country").distinct()
        cwin = cfg["spikes"]["country_daily"]["baseline_days"]
        country = (spark.read.orc(f"{cfg['paths']['warehouse']}/radar.db/country_daily")
                   .where(F.col("dt").between(str(per["country_start"]), str(last_day)))
                   .join(F.broadcast(pairs), ["qid", "country"])
                   .groupBy("qid", "country", F.to_date("dt").alias("day")).agg(F.sum("views").alias("views"))
                   .groupBy("qid", "country")
                   .applyInPandas(country_baselines(cwin, days),
                                  "qid string, country string, day date, views double, baseline double")
                   .select("qid", "country", "day", F.col("views").cast("int").alias("views"),
                           F.col("baseline").cast("int").alias("baseline")))

        print("Cassandra (Spark Cassandra connector):")
        for df, table in ((hourly, "article_hourly"), (country, "article_country_daily")):
            t = time.time()
            df = df.cache()
            n = df.count()
            if not a.dry_run:
                write_cassandra(df, table)
            print(f"  {table}: {n:,} rows in {time.time() - t:.0f} s")
            df.unpersist()

    if not a.dry_run:
        check_day = last_day - dt.timedelta(days=1)
        print(f"\ncheck: top 5 for scope G, {check_day}, ALL")
        spark_top = sorted((e for e in events if "G" in e["scopes"] and check_day in
                            {r[1] for r in rank_rows(e, {}) if r[0] == "G"}), key=lambda e: -e["score"])[:5]
        cass_top = list(s.execute("SELECT event_id, title, score FROM event_rank "
                                  "WHERE scope = 'G' AND day = %s AND category = 'ALL' LIMIT 5", (check_day,)))
        for x, y in zip(spark_top, cass_top):
            print(f"  {x['event_id']:<32} {y.event_id:<32} {'same' if x['event_id'] == y.event_id else 'DIFFERENT'}"
                  f"  {y.title[:40]}")
        print(f"  MongoDB events: {mdb['events'].count_documents({}):,} (expected {len(docs):,})")
        cluster.shutdown()
    print(f"\ndone in {time.time() - t0:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
