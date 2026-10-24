#!/usr/bin/env python3
"""Data-quality checks that need to group tens of millions of rows (too heavy for
Hive's local engine here), run with Spark straight on the warehouse ORC files.

  T-D3  every daily row equals the sum of its hours (every title, not a sample)
  T-D4  no (lang, title, hour) appears twice in hourly_views

Run inside edge:
  spark-submit --master local[3] --driver-memory 3g tests/dq/dq_spark.py [DAY]
Exit code 1 if any check fails.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402

day = sys.argv[1] if len(sys.argv) > 1 else "2024-06-03"
wh = f"{load()['paths']['warehouse']}/radar.db"
spark = (SparkSession.builder.appName(f"dq_spark {day}")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

hourly = spark.read.orc(f"{wh}/hourly_views/dt={day}")
daily = spark.read.orc(f"{wh}/daily_views/dt={day}")

h = hourly.groupBy("lang", "page_title").agg(F.sum("views").alias("h_views"))
j = daily.select("lang", "page_title", F.col("views").alias("d_views")).join(h, ["lang", "page_title"], "full_outer")
r3 = j.agg(F.count(F.lit(1)).alias("titles"),
           F.sum(F.when(F.coalesce("d_views", F.lit(-1)) != F.coalesce("h_views", F.lit(-1)), 1).otherwise(0))
            .alias("mismatches")).first()

r4 = (hourly.groupBy("lang", "page_title", "hour_ts").count().where("count > 1")
      .agg(F.count(F.lit(1)).alias("dups")).first())

checks = [
    ("T-D3 daily = sum of hourly", f"{r3['titles']:,} titles, {r3['mismatches']} mismatches", r3["mismatches"] == 0),
    ("T-D4 no duplicate hours", f"{r4['dups']} duplicate keys", r4["dups"] == 0),
]
print(f"\nDQ (Spark) for {day}")
for name, detail, ok in checks:
    print(f"  {'PASS' if ok else 'FAIL'}  {name:<28} {detail}")
spark.stop()
sys.exit(0 if all(ok for _, _, ok in checks) else 1)
