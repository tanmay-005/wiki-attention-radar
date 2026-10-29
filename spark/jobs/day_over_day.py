#!/usr/bin/env python3
"""Analysis A4: the biggest day-over-day jumps between two days (a first, simple look
at what Phase 4's spike detection will find properly).

Joins all ~8 million titles of one day with the next. This ran out of memory in
Hive's local mode, so it runs here on the same warehouse ORC files.

Run inside edge:
  spark-submit --master local[3] --driver-memory 3g spark/jobs/day_over_day.py [DAY1] [DAY2]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402

d1, d2 = (sys.argv[1:3] if len(sys.argv) >= 3 else ("2026-10-20", "2026-10-21"))
wh = f"{load()['paths']['warehouse']}/radar.db"
spark = (SparkSession.builder.appName(f"A4 day_over_day {d1} -> {d2}")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

before = spark.read.orc(f"{wh}/daily_views/dt={d1}").select("lang", "page_title", F.col("views").alias("views_before"))
after = (spark.read.orc(f"{wh}/daily_views/dt={d2}").where("views >= 20000")
         .select("lang", "page_title", F.col("views").alias("views_after")))

jumps = (after.join(before, ["lang", "page_title"], "left")          # Spark broadcasts the small side
         .fillna({"views_before": 0})
         .withColumn("growth", F.round(F.col("views_after") / F.greatest(F.col("views_before"), F.lit(100)), 1))
         .orderBy(F.col("growth").desc())
         .limit(15))
print(f"\nA4. Biggest jumps {d1} -> {d2} (pages with at least 20,000 views on {d2})")
jumps.show(15, truncate=45)
spark.stop()
