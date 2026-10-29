#!/usr/bin/env python3
"""B4, Spark side: the same daily aggregation MR2 does (hourly MR1 output -> one row
per language, title and day), as a Spark DataFrame job. Writes gzip text like MR2 and
checks that rows and views match MR2's output for the same day.

  spark-submit --master local[3] --driver-memory 3g benchmarks/b4_spark_daily.py DAY
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

day = sys.argv[1] if len(sys.argv) > 1 else "2026-10-20"
cfg = load()
staged = cfg["paths"]["staged"]
out = f"{cfg['paths']['hdfs_root']}/benchmarks/b4/spark_daily/dt={day}"
spark = get_spark(f"B4 spark daily {day}", shuffle_partitions=5)


def tsv(path, names):
    parts = F.split(F.col("value"), "\t", -1)
    return spark.read.text(path).select(*[parts.getItem(i).alias(n) for i, n in enumerate(names)])


t0 = time.time()
hourly = tsv(f"{staged}/hourly/dt={day}", ["lang", "platform", "page_title", "hour", "views"])
daily = (hourly.groupBy("lang", "page_title", F.substring("hour", 1, 10).alias("day"))
         .agg(F.sum(F.col("views").cast("long")).alias("views")))
(daily.select(F.concat_ws("\t", "lang", "page_title", "day", F.col("views").cast("string")))
      .write.mode("overwrite").option("compression", "gzip").text(out))
secs = time.time() - t0

mine = spark.read.text(out).select(F.split("value", "\t").getItem(3).cast("long").alias("v")).agg(F.count(F.lit(1)), F.sum("v")).first()
mr2 = tsv(f"{staged}/daily/dt={day}", ["lang", "page_title", "day", "views"]).agg(
    F.count(F.lit(1)), F.sum(F.col("views").cast("long"))).first()
print(f"\nB4 spark_daily {day}: {secs:.1f} s, {mine[0]:,} rows, {mine[1]:,} views")
print(f"MR2 output same day:      {mr2[0]:,} rows, {mr2[1]:,} views -> {'SAME' if tuple(mine) == tuple(mr2) else 'DIFFERENT'}")
print(f"RESULT,spark_dataframe,{secs:.1f},{mine[0]},{mine[1]}")
spark.stop()
