#!/usr/bin/env python3
"""Benchmark B3, step 1: copy one day of hourly_views (ORC) as plain text and as Parquet.
The copies are written by Spark (a full-day sort is too heavy for Hive's local engine here);
benchmarks/b3_formats.sh then times the same Hive queries on all three formats.

Run inside edge:
  spark-submit --master local[3] --driver-memory 3g benchmarks/b3_make_copies.py [DAY]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession  # noqa: E402

from spark.common.config import load  # noqa: E402

day = sys.argv[1] if len(sys.argv) > 1 else "2024-06-03"
cfg = load()
src = f"{cfg['paths']['warehouse']}/radar.db/hourly_views/dt={day}"
dst = f"{cfg['paths']['hdfs_root']}/benchmarks/b3"
spark = (SparkSession.builder.appName(f"b3 copies {day}")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.orc(src)   # already sorted by lang, page_title, hour_ts; writing keeps that order
(df.write.mode("overwrite")
   .option("sep", "\t").option("quote", "").option("escapeQuotes", "false")
   .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
   .csv(f"{dst}/hourly_views_text/dt={day}"))                  # uncompressed text
(df.write.mode("overwrite").option("compression", "snappy")
   .parquet(f"{dst}/hourly_views_parquet/dt={day}"))
print(f"wrote text and Parquet copies of {src} under {dst}")
spark.stop()
