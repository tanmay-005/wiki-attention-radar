#!/usr/bin/env python3
"""Step 3.5: fill the ORC warehouse tables that Hive defines in hive/02_warehouse.hql.

Why Spark and not a Hive INSERT: HiveServer2 here runs Tez in local mode, so every
task of a query runs inside one 3 GB JVM. Grouping a full day (about 65 million
hourly rows) in that JVM ran out of memory and stalled. Spark sorts and spills to
disk per task and handles the same day in a few minutes. Hive still owns the
tables: this job only writes ORC files into the table folders, and re-running
hive/02_warehouse.hql (MSCK REPAIR) registers the new partitions.

Writes, one folder per day (dt=YYYY-MM-DD), replacing that day only:
  hourly_views   <- MR1 output  /radar/staged/hourly    desktop + mobile side by side
  daily_views    <- MR2 output  /radar/staged/daily
  country_daily  <- raw country files, cleaned with the same title rules as MR1

Run inside edge:
  spark-submit --master local[3] --driver-memory 3g spark/jobs/load_warehouse.py --dev
  spark-submit ... spark/jobs/load_warehouse.py --day 2024-06-03 --day 2024-06-04
  spark-submit ... spark/jobs/load_warehouse.py --full
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "mapreduce"))

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

from spark.common.config import country_days, hourly_days, load  # noqa: E402

COUNTRY_LANG_RE = r"^([a-z]+(-[a-z]+)*)(\.wikipedia(\.org)?)?$"


def session() -> SparkSession:
    return (SparkSession.builder.appName("load_warehouse")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.orc.compression.codec", "zlib")
            .getOrCreate())


def exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(p)


def read_tsv(spark: SparkSession, path: str, names: list[str]):
    """Tab-separated text without any quoting rules (titles may contain quotes)."""
    parts = F.split(F.col("value"), "\t", -1)
    return (spark.read.text(path)
            .where(F.size(parts) == len(names))
            .select(*[parts.getItem(i).alias(n) for i, n in enumerate(names)]))


def summary(spark: SparkSession, path: str, label: str, t0: float) -> None:
    r = spark.read.orc(path).agg(F.count(F.lit(1)).alias("n"), F.sum("views").alias("v")).first()
    print(f"  {label:<14} {r['n']:>12,} rows  {r['v'] or 0:>15,} views   ({time.time() - t0:.0f} s)", flush=True)


def load_hourly(spark, cfg, day: str) -> None:
    src = f"{cfg['paths']['staged']}/hourly/dt={day}"
    if not exists(spark, src):
        print(f"  hourly_views   skipped: no MR1 output at {src}")
        return
    t0 = time.time()
    st = (read_tsv(spark, src, ["lang", "platform", "page_title", "hour", "views"])
          .withColumn("views", F.col("views").cast("long")))
    hv = (st.groupBy("lang", "page_title", "hour")
          .agg(F.sum(F.when(F.col("platform") == "desktop", F.col("views")).otherwise(F.lit(0))).alias("views_desktop"),
               F.sum(F.when(F.col("platform") == "mobile", F.col("views")).otherwise(F.lit(0))).alias("views_mobile"),
               F.sum("views").alias("views"))
          .withColumn("hour_ts", F.to_timestamp("hour", "yyyy-MM-dd'T'HH"))
          .select("lang", "page_title", "hour_ts", "views_desktop", "views_mobile", "views"))
    out = f"{cfg['paths']['warehouse']}/radar.db/hourly_views/dt={day}"
    # range-partition and sort so each language sits together: ORC min/max stats can skip stripes
    (hv.repartitionByRange(8, "lang", "page_title")
       .sortWithinPartitions("lang", "page_title", "hour_ts")
       .write.mode("overwrite").orc(out))
    summary(spark, out, "hourly_views", t0)


def load_daily(spark, cfg, day: str) -> None:
    src = f"{cfg['paths']['staged']}/daily/dt={day}"
    if not exists(spark, src):
        print(f"  daily_views    skipped: no MR2 output at {src}")
        return
    t0 = time.time()
    dv = (read_tsv(spark, src, ["lang", "page_title", "day", "views"])
          .select("lang", "page_title", F.to_date("day").alias("day"), F.col("views").cast("long").alias("views")))
    out = f"{cfg['paths']['warehouse']}/radar.db/daily_views/dt={day}"
    (dv.repartitionByRange(4, "lang", "page_title")
       .sortWithinPartitions("lang", "page_title")
       .write.mode("overwrite").orc(out))
    summary(spark, out, "daily_views", t0)


def load_country(spark, cfg, day: str) -> None:
    src = f"{cfg['paths']['raw_country']}/dt={day}"
    if not exists(spark, src):
        print(f"  country_daily  skipped: no raw file at {src}")
        return
    t0 = time.time()
    from common_title import is_content_page, normalize_title  # same rules as MR1 and the Hive UDFs

    content = F.udf(lambda lang, t: bool(t) and is_content_page(lang, t.replace(" ", "_")), T.BooleanType())
    norm = F.udf(lambda t: normalize_title(t) if t is not None else None, T.StringType())
    langs = list(cfg["languages"])
    raw = read_tsv(spark, src, ["country_name", "country_code", "project", "page_id",
                                "page_title", "item_id", "views"])
    cd = (raw.withColumn("lang", F.regexp_extract("project", COUNTRY_LANG_RE, 1))
             .where(F.col("lang").isin(langs))
             .where(content("lang", "page_title"))
             .select(F.col("country_code").alias("country"), "country_name", "lang",
                     F.col("page_id").cast("long").alias("page_id"),
                     norm("page_title").alias("page_title"),
                     F.col("item_id").alias("qid"),
                     F.col("views").cast("long").alias("views")))
    out = f"{cfg['paths']['warehouse']}/radar.db/country_daily/dt={day}"
    (cd.coalesce(1).sortWithinPartitions("country", "lang", F.col("views").desc())
       .write.mode("overwrite").orc(out))
    summary(spark, out, "country_daily", t0)


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dev", action="store_true", help="dev days from settings.yaml")
    g.add_argument("--full", action="store_true", help="the whole configured period")
    g.add_argument("--day", action="append", help="one day (YYYY-MM-DD); repeat for more")
    ap.add_argument("--only", choices=["hourly", "daily", "country"], action="append",
                    help="load only these tables (default: all three)")
    a = ap.parse_args()

    cfg = load()
    if a.day:
        hdays = cdays = sorted(set(a.day))
    else:
        hdays = [d.isoformat() for d in hourly_days(cfg, dev=a.dev)]
        cdays = [d.isoformat() for d in country_days(cfg, dev=a.dev)]
    only = set(a.only or ["hourly", "daily", "country"])

    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(str(ROOT / "mapreduce" / "common_title.py"))
    t_all = time.time()
    for day in sorted(set(hdays) | set(cdays)):
        print(f"== {day}", flush=True)
        if day in hdays and "hourly" in only:
            load_hourly(spark, cfg, day)
        if day in hdays and "daily" in only:
            load_daily(spark, cfg, day)
        if day in cdays and "country" in only:
            load_country(spark, cfg, day)
    print(f"done in {time.time() - t_all:.0f} s. Now register the partitions in Hive:")
    print("  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/02_warehouse.hql")
    spark.stop()


if __name__ == "__main__":
    main()
