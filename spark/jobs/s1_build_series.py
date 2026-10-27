#!/usr/bin/env python3
"""S1 build_series (step 4.2): dense hourly series for the articles that could spike.

There are tens of millions of (lang, title) pairs, but only articles that reach the
spike floor at least once in the detection period can ever spike. Those "candidates"
are found first (a cheap filter), then only their series are built:

  candidates = titles with views >= min_views(lang) in some hour on or after detect_start
  dense      = every candidate x every hour of the whole period (baseline + detection),
               with 0 where the dumps have no row (no row means nobody read the page)

Output: /radar/output/series_global, Parquet partitioned by lang
        (lang, page_title, hour_ts, views, views_desktop)

Run inside edge (needs hourly_views loaded for the whole period):
  spark-submit --master local[3] --driver-memory 3g spark/jobs/s1_build_series.py
"""
import datetime as dt
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402
from spark.common.spikes import params  # noqa: E402

t_all = time.time()
cfg = load()
per = cfg["period"]
start = dt.datetime.fromisoformat(f"{per['hourly_start']}T00:00:00")
end = dt.datetime.fromisoformat(f"{per['hourly_end']}T23:00:00")
detect = dt.datetime.fromisoformat(f"{per['detect_start']}T00:00:00")
n_hours = int((end - start).total_seconds() // 3600) + 1
langs = cfg["languages"]
min_views = {lang: params(cfg, "global_hourly", lang)["min_views"] for lang in langs}
wh = f"{cfg['paths']['warehouse']}/radar.db"
out = f"{cfg['paths']['output']}/series_global"

spark = get_spark("S1 build_series", shuffle_partitions=32)
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

hv_all = (spark.read.orc(f"{wh}/hourly_views")
          .where(F.col("dt").between(str(per["hourly_start"]), str(per["hourly_end"]))))
hv = hv_all.select("lang", "page_title", "hour_ts", "views", "views_desktop")

days_found = hv_all.select("dt").distinct().count()
print(f"period {start:%Y-%m-%d} .. {end:%Y-%m-%d} ({n_hours} hours), detection from {detect:%Y-%m-%d}")
print(f"days present in hourly_views: {days_found}")
if days_found * 24 < n_hours:
    print("WARNING: some days are missing; their hours will look like zero views and can create fake spikes")

floor = F.create_map(*[x for lang in langs for x in (F.lit(lang), F.lit(min_views[lang]))])
cands = (hv.where(F.col("hour_ts") >= F.lit(detect))
           .where(F.col("views") >= floor[F.col("lang")])
           .select("lang", "page_title").distinct()
           .cache())
per_lang = {r["lang"]: r["count"] for r in cands.groupBy("lang").count().collect()}
n_cands = sum(per_lang.values())
print("candidates per language:", ", ".join(f"{k} {per_lang.get(k, 0):,}" for k in langs), f"(total {n_cands:,})")
print("min_views per language:", min_views)

series = hv.join(F.broadcast(cands), ["lang", "page_title"])
hours = spark.range(n_hours).select(
    F.timestamp_seconds(F.lit(int(start.replace(tzinfo=dt.timezone.utc).timestamp())) + F.col("id") * 3600)
    .alias("hour_ts"))
dense = (cands.crossJoin(F.broadcast(hours))
         .join(series, ["lang", "page_title", "hour_ts"], "left")
         .fillna(0, subset=["views", "views_desktop"])
         .select("lang", "page_title", "hour_ts",
                 F.col("views").cast("long").alias("views"),
                 F.col("views_desktop").cast("long").alias("views_desktop")))

(dense.repartition(32, "lang", "page_title")
      .sortWithinPartitions("lang", "page_title", "hour_ts")
      .write.mode("overwrite").partitionBy("lang").parquet(out))

# T-D5: every candidate has every hour, no nulls
chk = spark.read.parquet(out)
rows = chk.count()
nulls = chk.where(F.col("views").isNull()).count()
expected = n_cands * n_hours
print(f"\nwrote {out}: {rows:,} rows (expected {n_cands:,} x {n_hours} = {expected:,}), null views: {nulls}")
print(f"T-D5 dense series: {'PASS' if rows == expected and nulls == 0 else 'FAIL'}")
print(f"done in {time.time() - t_all:.0f} s")
spark.stop()
