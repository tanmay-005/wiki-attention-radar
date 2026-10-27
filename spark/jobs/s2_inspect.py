#!/usr/bin/env python3
"""Step 4.12 helper: how many S2 episodes would survive stricter thresholds, per language.
  spark-submit --master local[3] spark/jobs/s2_inspect.py 2>/dev/null
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

cfg = load()
spark = get_spark("S2 inspect", shuffle_partitions=8)
eps = spark.read.parquet(f"{cfg['paths']['hdfs_root']}/output/spikes_global").where(~F.col("suspicious")).cache()

print("\nepisodes per language at stricter ratio / length settings:")
rules = {"now": F.lit(True), "ratio>=10": F.col("ratio") >= 10, "ratio>=20": F.col("ratio") >= 20,
         "hours>=2": F.col("hours") >= 2, "ratio>=10 & hours>=2": (F.col("ratio") >= 10) & (F.col("hours") >= 2)}
eps.groupBy("lang").agg(*[F.sum(c.cast("int")).alias(n) for n, c in rules.items()]).orderBy("lang").show()

print("score distribution (rank 100, 500, 1000, 2000, 5000):")
ranked = [r["score"] for r in eps.select("score").orderBy(F.col("score").desc()).limit(5000).collect()]
print("  " + ", ".join(f"#{k}: {ranked[k - 1]:.0f}" for k in (100, 500, 1000, 2000, 5000) if k <= len(ranked)))

print("\nhigh-ratio episodes from a near-zero baseline (possible automated or odd traffic):")
(eps.where((F.col("baseline") < 5) & (F.col("peak_views") > 5000))
    .select("lang", F.substring("page_title", 1, 40).alias("page_title"), "peak_hour", "peak_views",
            "baseline", "hours", F.round("desktop_share", 2).alias("desktop"), F.round("score", 0).alias("score"))
    .orderBy(F.col("peak_views").desc()).show(25, truncate=False))
spark.stop()
