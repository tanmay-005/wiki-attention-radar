#!/usr/bin/env python3
"""Benchmark B5 (step 4.14): the same aggregation with the RDD API and the DataFrame API.

Input: MR1 output for one day (gzip text, about 65M rows). Task: total views per
(lang, title) for the day. RDD: map + reduceByKey (Python functions on every row).
DataFrame: split + groupBy (runs inside the JVM, planned by Catalyst/Tungsten).
Shuffle sizes are read from the Spark UI's REST API.

  spark-submit --master local[3] --driver-memory 3g benchmarks/b5_rdd_vs_df.py [DAY] [--hold]
--hold keeps the Spark UI (http://localhost:4040) open at the end for screenshots.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

args = [a for a in sys.argv[1:] if not a.startswith("--")]
day = args[0] if args else "2026-10-20"
hold = "--hold" in sys.argv
cfg = load()
src = f"{cfg['paths']['staged']}/hourly/dt={day}"
spark = get_spark(f"B5 rdd vs df {day}", shuffle_partitions=16)
sc = spark.sparkContext


def shuffle_bytes(group: str) -> tuple[int, int]:
    """(shuffle write, shuffle read) bytes of all stages run under this job group."""
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    get = lambda p: json.load(urllib.request.urlopen(base + p, timeout=10))  # noqa: E731
    stage_ids = {s for j in get("/jobs") if j.get("jobGroup") == group for s in j["stageIds"]}
    w = r = 0
    for st in get("/stages"):
        if st["stageId"] in stage_ids:
            w += st.get("shuffleWriteBytes", 0)
            r += st.get("shuffleReadBytes", 0)
    return w, r


results = []

sc.setJobGroup("rdd", "RDD reduceByKey")
t0 = time.time()
rdd = (sc.textFile(src).map(lambda l: l.split("\t"))
       .filter(lambda p: len(p) == 5)
       .map(lambda p: ((p[0], p[2]), int(p[4])))
       .reduceByKey(lambda a, b: a + b))
n_rdd, v_rdd = rdd.map(lambda kv: (1, kv[1])).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
results.append(("rdd_reduceByKey", time.time() - t0, n_rdd, v_rdd))

sc.setJobGroup("df", "DataFrame groupBy")
t0 = time.time()
parts = F.split(F.col("value"), "\t")
df = (spark.read.text(src)
      .select(parts.getItem(0).alias("lang"), parts.getItem(2).alias("page_title"),
              parts.getItem(4).cast("long").alias("views"))
      .groupBy("lang", "page_title").agg(F.sum("views").alias("views")))
agg = df.agg(F.count(F.lit(1)).alias("n"), F.sum("views").alias("v")).first()
results.append(("dataframe_groupBy", time.time() - t0, agg["n"], agg["v"]))
sc.setJobGroup("none", "")

out = Path(__file__).resolve().parent / "results" / "b5.csv"
lines = ["api,seconds,rows,views,shuffle_write_mb,shuffle_read_mb"]
print(f"\nB5 on {src}")
for (name, secs, n, v), group in zip(results, ["rdd", "df"]):
    w, r = shuffle_bytes(group)
    lines.append(f"{name},{secs:.1f},{n},{v},{w / 1e6:.1f},{r / 1e6:.1f}")
    print(f"  {name:<18} {secs:7.1f} s   {n:,} rows   {v:,} views   shuffle write {w / 1e6:,.1f} MB")
same = results[0][2:] == results[1][2:]
print(f"  same result: {'yes' if same else 'NO'}")
try:
    out.write_text("\n".join(lines) + "\n")
    print(f"saved {out}")
except OSError as e:
    print(f"could not save {out}: {e}")
if hold:
    input(f"\nSpark UI: http://localhost:4040 (Jobs, Stages, SQL tabs). Press Enter to finish... ")
spark.stop()
