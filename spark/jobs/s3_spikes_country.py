#!/usr/bin/env python3
"""S3 spikes_country (step 4.5): daily spikes per (country, Wikidata item).

Country data is daily and differentially private: a day only appears when an article
had at least 90 views in that country, so a missing day means "below 90", treated as 0.
Languages are summed per Wikidata item, so 'Virat Kohli' read in English and Hindi in
India counts as one subject. Baseline: the 14 days before each detection day, with a
floor of 90 (the release threshold) so a first appearance never becomes an infinite jump.

Output: /radar/output/spikes_country (Parquet), one row per episode (consecutive days):
  country, country_name, qid, lang, page_title, start, end, peak_day, peak_views,
  baseline, ratio, z, days, score       (lang/page_title = the most-read language version)

Run inside edge (needs country_daily for the whole country period):
  spark-submit --master local[3] --driver-memory 3g spark/jobs/s3_spikes_country.py
"""
import datetime as dt
import math
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402
from spark.common.spikes import detect, merge_episodes, params, summarise  # noqa: E402

SCHEMA = ("country string, country_name string, qid string, lang string, page_title string, "
          "start date, end date, peak_day date, peak_views long, baseline double, ratio double, "
          "z double, days int, score double")


def make_udf(cfg: dict, detect_start: dt.date):
    p = params(cfg, "country_daily")
    window = p["baseline_days"]
    cap = cfg["spikes"]["ratio_cap"]

    def per_series(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("day")
        days = list(pd.to_datetime(pdf["day"]).dt.date)
        views = pdf["views"].to_numpy(dtype=float)
        first = sum(1 for d in days if d < detect_start)
        rows = []
        for ep in merge_episodes(detect(views, first, 1, window, p), max_gap=1):
            s = summarise(ep)
            peak = pdf.iloc[s["peak_idx"]]
            rows.append({
                "country": pdf["country"].iat[0], "country_name": pdf["country_name"].iat[0], "qid": pdf["qid"].iat[0],
                "lang": peak["top_lang"], "page_title": peak["top_title"],
                "start": days[s["start_idx"]], "end": days[s["end_idx"]], "peak_day": days[s["peak_idx"]],
                "peak_views": int(s["peak_views"]), "baseline": s["baseline"], "ratio": s["ratio"], "z": s["z"],
                "days": int(s["points"]),
                "score": min(s["ratio"], cap) * math.log10(max(s["peak_views"] - s["baseline"], 0) + 10),
            })
        cols = [c.split()[0] for c in SCHEMA.split(", ")]
        return pd.DataFrame(rows, columns=cols)

    return per_series


def main() -> None:
    t0 = time.time()
    cfg = load()
    per = cfg["period"]
    c_start, c_end = str(per["country_start"]), str(per["country_end"])
    detect_start = dt.date.fromisoformat(str(per["detect_start"]))
    p = params(cfg, "country_daily")
    out = f"{cfg['paths']['output']}/spikes_country"

    spark = get_spark("S3 spikes_country")
    spark.sparkContext.addPyFile(str(ROOT / "spark" / "common" / "spikes.py"))
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    cd = (spark.read.orc(f"{cfg['paths']['warehouse']}/radar.db/country_daily")
          .where(F.col("dt").between(c_start, c_end))
          .where(F.col("qid").rlike("^Q[0-9]+$"))
          .withColumn("day", F.to_date("dt")))
    n_days = cd.select("day").distinct().count()

    # one row per (country, item, day): views summed over languages, plus the most-read version
    daily = cd.groupBy("country", "qid", "day").agg(
        F.first("country_name").alias("country_name"),
        F.sum("views").alias("views"),
        F.max_by(F.struct("lang", "page_title"), "views").alias("top"))
    cands = (daily.where((F.col("day") >= F.lit(detect_start)) & (F.col("views") >= p["min_views"]))
             .select("country", "qid").distinct().cache())
    n_cands = cands.count()

    all_days = spark.sql(f"SELECT explode(sequence(DATE'{c_start}', DATE'{c_end}', INTERVAL 1 DAY)) AS day")
    names = daily.groupBy("country").agg(F.first("country_name").alias("cname"))
    dense = (cands.crossJoin(F.broadcast(all_days))
             .join(daily.join(F.broadcast(cands), ["country", "qid"]), ["country", "qid", "day"], "left")
             .join(F.broadcast(names), "country")
             .select("country", F.col("cname").alias("country_name"), "qid", "day",
                     F.coalesce("views", F.lit(0)).alias("views"),
                     F.col("top.lang").alias("top_lang"), F.col("top.page_title").alias("top_title")))

    eps = dense.groupBy("country", "qid").applyInPandas(make_udf(cfg, detect_start), schema=SCHEMA)
    eps.write.mode("overwrite").parquet(out)

    res = spark.read.parquet(out).cache()
    print(f"\ncountry days present: {n_days} ({c_start} .. {c_end}), candidates (country, item): {n_cands:,}")
    print(f"wrote {out}: {res.count():,} episodes in {time.time() - t0:.0f} s")
    print("\ncountries with the most spike episodes:")
    res.groupBy("country", "country_name").count().orderBy(F.col("count").desc()).show(10)
    print("top 20 by score:")
    (res.orderBy(F.col("score").desc())
        .select("country", "lang", "page_title", "peak_day", "peak_views",
                F.round("baseline", 0).alias("baseline"), F.round("ratio", 1).alias("ratio"),
                "days", F.round("score", 1).alias("score"))
        .show(20, truncate=40))
    print("India, top 10:")
    (res.where("country = 'IN'").orderBy(F.col("score").desc())
        .select("lang", "page_title", "peak_day", "peak_views", F.round("ratio", 1).alias("ratio"), "days")
        .show(10, truncate=45))
    spark.stop()


if __name__ == "__main__":
    main()
