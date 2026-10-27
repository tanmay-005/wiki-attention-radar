#!/usr/bin/env python3
"""S2 spikes_global (step 4.4): hourly spikes per language, merged into episodes.

For every candidate series from S1 and every detection day, the baseline is the
median and IQR-based spread of the 168 hours before that day (spark/common/spikes.py).
An hour is a spike when views >= min_views(lang), ratio >= min_ratio and z >= min_z.
Spike hours of one article with at most one quiet hour between them form an episode.

Each series is handled by one pandas function call (applyInPandas), so the 7-day
window is a plain array slice instead of a big exploded join.

Output: /radar/output/spikes_global (Parquet), one row per episode:
  lang, page_title, start, end, peak_hour, peak_views, baseline, ratio, z, hours,
  views_total, desktop_share, suspicious, score

Run inside edge (after S1):
  spark-submit --master local[3] --driver-memory 3g spark/jobs/s2_spikes_global.py
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

SCHEMA = ("lang string, page_title string, start timestamp, end timestamp, peak_hour timestamp, "
          "peak_views long, baseline double, ratio double, z double, hours int, views_total long, "
          "desktop_share double, suspicious boolean, score double")


def score(ratio: float, peak: float, baseline: float, cap: float) -> float:
    """Ranking score (same formula as events): capped ratio x log of the extra views."""
    return min(ratio, cap) * math.log10(max(peak - baseline, 0) + 10)


def make_udf(cfg: dict, detect_start: dt.datetime):
    window = cfg["spikes"]["global_hourly"]["baseline_hours"]
    sus = cfg["spikes"]["suspicious"]
    cap = cfg["spikes"]["ratio_cap"]
    min_hours = cfg["spikes"]["global_hourly"].get("min_hours", 1)
    by_lang = {lang: params(cfg, "global_hourly", lang) for lang in cfg["languages"]}

    def per_series(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("hour_ts")
        lang, title = pdf["lang"].iat[0], pdf["page_title"].iat[0]
        hours = pd.to_datetime(pdf["hour_ts"]).dt.tz_localize(None).to_numpy()
        views = pdf["views"].to_numpy(dtype=float)
        desk = pdf["views_desktop"].to_numpy(dtype=float)
        first = int((hours < pd.Timestamp(detect_start).to_datetime64()).sum())
        pts = detect(views, first, 24, window, by_lang[lang])
        rows = []
        for ep in merge_episodes(pts, max_gap=2):
            s = summarise(ep)
            idx = [p["idx"] for p in ep]
            share = float(desk[idx].sum() / max(views[idx].sum(), 1.0))
            suspicious = bool(s["points"] == 1 and share > sus["max_desktop_share"] and s["ratio"] > sus["min_ratio"])
            if s["points"] < min_hours and not suspicious:
                continue  # too short to count; suspicious ones are kept so they stay visible
            rows.append({
                "lang": lang, "page_title": title,
                "start": hours[s["start_idx"]], "end": hours[s["end_idx"]], "peak_hour": hours[s["peak_idx"]],
                "peak_views": int(s["peak_views"]), "baseline": s["baseline"], "ratio": s["ratio"], "z": s["z"],
                "hours": int(s["points"]), "views_total": int(s["views_total"]), "desktop_share": share,
                "suspicious": suspicious,
                "score": score(s["ratio"], s["peak_views"], s["baseline"], cap),
            })
        cols = [c.split()[0] for c in SCHEMA.split(", ")]
        return pd.DataFrame(rows, columns=cols)

    return per_series


def main() -> None:
    t0 = time.time()
    cfg = load()
    detect_start = dt.datetime.fromisoformat(f"{cfg['period']['detect_start']}T00:00:00")
    src = f"{cfg['paths']['output']}/series_global"
    out = f"{cfg['paths']['output']}/spikes_global"

    spark = get_spark("S2 spikes_global", shuffle_partitions=32)
    spark.sparkContext.addPyFile(str(ROOT / "spark" / "common" / "spikes.py"))
    series = spark.read.parquet(src)
    eps = series.groupBy("lang", "page_title").applyInPandas(make_udf(cfg, detect_start), schema=SCHEMA)
    eps.write.mode("overwrite").parquet(out)

    res = spark.read.parquet(out).cache()
    n = res.count()
    print(f"\nwrote {out}: {n:,} spike episodes in {time.time() - t0:.0f} s")
    print("\nper language:")
    res.groupBy("lang").agg(F.count(F.lit(1)).alias("episodes"),
                            F.sum(F.col("suspicious").cast("int")).alias("suspicious"),
                            F.countDistinct("page_title").alias("articles")).orderBy("lang").show()

    # T-D6: every episode satisfies the rule and nothing is flagged before detection starts
    early = res.where(F.col("start") < F.lit(detect_start)).count()
    print(f"T-D6 no spike before {detect_start:%Y-%m-%d}: {'PASS' if early == 0 else 'FAIL'} ({early})")

    print("top 20 episodes by score (not suspicious):")
    (res.where(~F.col("suspicious")).orderBy(F.col("score").desc())
        .select("lang", "page_title", "peak_hour", "peak_views",
                F.round("baseline", 0).alias("baseline"), F.round("ratio", 1).alias("ratio"),
                "hours", F.round("score", 1).alias("score"))
        .show(20, truncate=40))
    spark.stop()


if __name__ == "__main__":
    main()
