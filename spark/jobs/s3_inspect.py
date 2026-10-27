#!/usr/bin/env python3
"""Step 4.12 helper for S3: how many country episodes survive stricter thresholds.
  spark-submit --master local[3] spark/jobs/s3_inspect.py 2>/dev/null
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import functions as F  # noqa: E402

from spark.common.config import load  # noqa: E402
from spark.common.session import get_spark  # noqa: E402

cfg = load()
spark = get_spark("S3 inspect", shuffle_partitions=8)
eps = spark.read.parquet(f"{cfg['paths']['output']}/spikes_country").cache()
r, v = F.col("ratio"), F.col("peak_views")

print("\nepisodes at stricter settings (all countries):")
rules = {"now": F.lit(True), "baseline 0": F.col("baseline") == 0,
         "ratio>=5": r >= 5, "ratio>=10": r >= 10, "ratio>=20": r >= 20,
         "peak>=1000": v >= 1000, "peak>=2000": v >= 2000,
         "ratio>=10 & peak>=1000": (r >= 10) & (v >= 1000), "ratio>=10 & peak>=2000": (r >= 10) & (v >= 2000)}
eps.agg(*[F.sum(c.cast("int")).alias(n) for n, c in rules.items()]).show(truncate=False)

print("same, top 8 countries:")
(eps.groupBy("country").agg(F.count(F.lit(1)).alias("now"),
                            F.sum(((r >= 10) & (v >= 1000)).cast("int")).alias("ratio>=10 & peak>=1000"),
                            F.sum(((r >= 10) & (v >= 2000)).cast("int")).alias("ratio>=10 & peak>=2000"))
    .orderBy(F.col("now").desc()).show(8))

print("peak views of episodes with a zero baseline (the item was under the 90-view floor before):")
eps.where(F.col("baseline") == 0).select(F.expr("percentile(peak_views, array(0.25, 0.5, 0.75, 0.9))").alias("p25_p50_p75_p90")).show(truncate=False)
spark.stop()
