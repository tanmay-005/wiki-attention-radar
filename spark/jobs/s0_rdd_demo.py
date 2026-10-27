#!/usr/bin/env python3
"""S0: RDD basics on one raw hourly dump (step 4.1).

Shows transformations (map, filter, reduceByKey) and actions (count, take, top),
the lineage Spark builds (toDebugString), and what caching saves on a second action.

Run inside edge:
  spark-submit --master local[3] spark/jobs/s0_rdd_demo.py [DAY] [HOUR]
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

day = sys.argv[1] if len(sys.argv) > 1 else "2024-06-03"
hour = sys.argv[2] if len(sys.argv) > 2 else "12"
cfg = load()
langs = set(cfg["languages"])
path = f"{cfg['paths']['raw_hourly']}/dt={day}/pageviews-{day.replace('-', '')}-{hour}0000.gz"

spark = get_spark("S0 rdd_demo")
sc = spark.sparkContext

lines = sc.textFile(path)                                       # transformation (lazy)
parts = lines.map(lambda l: l.split(" ")).filter(lambda p: len(p) == 4 and p[2].isdigit())
ours = parts.filter(lambda p: p[0] in langs or (p[0].endswith(".m") and p[0][:-2] in langs))  # en, en.m: Wikipedia only, like MR1
by_domain = ours.map(lambda p: (p[0], int(p[2]))).reduceByKey(lambda a, b: a + b)

print(f"\nInput: {path}")
print("\nLineage of by_domain (each indent = a shuffle boundary):")
print(by_domain.toDebugString().decode())

t0 = time.time()
n = lines.count()                                               # action: reads the file
t_first = time.time() - t0
print(f"\nlines in file: {n:,}   (first count, no cache: {t_first:.1f} s)")

ours.cache()
t0 = time.time(); n_ours = ours.count(); t_fill = time.time() - t0
t0 = time.time(); ours.count(); t_cached = time.time() - t0
print(f"rows for {sorted(langs)}: {n_ours:,}")
print(f"count while filling the cache: {t_fill:.1f} s, same count from the cache: {t_cached:.1f} s")

print("\nViews per domain this hour (desktop = 'xx', mobile = 'xx.m'):")
for dom, v in sorted(by_domain.collect(), key=lambda kv: -kv[1]):
    print(f"  {dom:<6} {v:>12,}")

print("\nTop 10 English articles this hour (desktop + mobile):")
top = (ours.filter(lambda p: p[0] in ("en", "en.m") and p[1] not in ("Main_Page", "-") and ":" not in p[1])
           .map(lambda p: (p[1], int(p[2]))).reduceByKey(lambda a, b: a + b)
           .top(10, key=lambda kv: kv[1]))
for title, v in top:
    print(f"  {v:>9,}  {title}")
print(f"\npartitions: {lines.getNumPartitions()} (a .gz file cannot be split, so it is read by one task)")
spark.stop()
