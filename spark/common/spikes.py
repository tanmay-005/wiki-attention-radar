"""Spike rules shared by S2 (hourly, per language) and S3 (daily, per country).

Plain numpy, no Spark imports, so the rules are unit-tested on their own
(tests/unit/test_spikes.py) and run inside Spark through applyInPandas.

Baseline: for each detection period (a day), the median and a robust spread of the
values in the window just before it. The spread is IQR / 1.349, which equals the
standard deviation for normal data but is not inflated by an earlier spike.

Rule for one value:  b = max(median, floor), ratio = v / b, z = (v - median) / (sigma + 1)
                     spike = v >= min_views and ratio >= min_ratio and z >= min_z
"""
from __future__ import annotations

import numpy as np


def params(cfg: dict, kind: str, lang: str | None = None) -> dict:
    """Thresholds for 'global_hourly' or 'country_daily', with per-language overrides applied."""
    p = {k: v for k, v in cfg["spikes"][kind].items() if k != "per_lang"}
    p.update(cfg["spikes"][kind].get("per_lang", {}).get(lang, {}) or {})
    return p


def robust_baseline(values) -> tuple[float, float]:
    """(median, sigma) of the baseline window; sigma = IQR / 1.349."""
    v = np.asarray(values, dtype=float)
    if v.size == 0:
        return 0.0, 0.0
    q25, q50, q75 = np.percentile(v, [25, 50, 75])
    return float(q50), float((q75 - q25) / 1.349)


def spike_rule(views: float, median: float, sigma: float, p: dict) -> tuple[bool, float, float]:
    """(is_spike, ratio, z) for one value against its baseline."""
    b = max(median, float(p["baseline_floor"]))
    ratio = views / b
    z = (views - median) / (sigma + 1.0)
    ok = views >= p["min_views"] and ratio >= p["min_ratio"] and z >= p["min_z"]
    return bool(ok), float(ratio), float(z)


def detect(values, first_detect: int, period: int, window: int, p: dict) -> list[dict]:
    """Spike points in a dense series.

    values       : the full series (zeros where there was no data)
    first_detect : index of the first value that may be flagged
    period       : how many values share one baseline (24 for hourly series, 1 for daily)
    window       : how many values before the period form its baseline (168 hours, 14 days)
    Returns one dict per spike: idx, views, median, sigma, ratio, z.
    """
    v = np.asarray(values, dtype=float)
    out = []
    for start in range(first_detect, len(v), period):
        lo = max(0, start - window)
        median, sigma = robust_baseline(v[lo:start])
        for i in range(start, min(start + period, len(v))):
            ok, ratio, z = spike_rule(v[i], median, sigma, p)
            if ok:
                out.append({"idx": i, "views": float(v[i]), "median": median, "sigma": sigma,
                            "ratio": ratio, "z": z})
    return out


def merge_episodes(points: list[dict], max_gap: int) -> list[list[dict]]:
    """Group spike points (sorted by idx) into episodes; a step of at most max_gap stays
    in the same episode (max_gap=2 for hours lets one quiet hour sit inside an episode)."""
    episodes: list[list[dict]] = []
    for pt in sorted(points, key=lambda d: d["idx"]):
        if episodes and pt["idx"] - episodes[-1][-1]["idx"] <= max_gap:
            episodes[-1].append(pt)
        else:
            episodes.append([pt])
    return episodes


def summarise(episode: list[dict]) -> dict:
    """start/end/peak of one episode; the peak is the point with the most views."""
    peak = max(episode, key=lambda d: d["views"])
    return {
        "start_idx": episode[0]["idx"],
        "end_idx": episode[-1]["idx"],
        "peak_idx": peak["idx"],
        "peak_views": peak["views"],
        "baseline": peak["median"],
        "ratio": peak["ratio"],
        "z": peak["z"],
        "points": len(episode),
        "views_total": float(sum(d["views"] for d in episode)),
    }
