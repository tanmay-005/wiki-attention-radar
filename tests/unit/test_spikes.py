"""T-U3 (spike rule) and T-U4 (episodes) from 09_TESTING_QA.md."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.common.spikes import detect, merge_episodes, params, robust_baseline, spike_rule, summarise  # noqa: E402

P = {"min_views": 1000, "min_ratio": 4.0, "min_z": 5.0, "baseline_floor": 50}


def hourly(base, days=8):
    return np.full(days * 24, float(base))


def test_flat_series_has_no_spike():
    assert detect(hourly(2000), 168, 24, 168, P) == []


def test_ten_times_hour_is_a_spike():
    v = hourly(2000)
    v[180] = 20000
    pts = detect(v, 168, 24, 168, P)
    assert [p["idx"] for p in pts] == [180]
    assert pts[0]["ratio"] == 10.0


def test_tiny_page_below_min_views_is_ignored():
    v = hourly(20)
    v[180] = 200                    # 10x, but only 200 views
    assert detect(v, 168, 24, 168, P) == []


def test_old_spike_does_not_inflate_baseline():
    v = hourly(2000)
    v[100:106] = 50000              # a big spike earlier, inside the baseline window
    median, sigma = robust_baseline(v[0:168])
    assert median == 2000 and sigma == 0
    v[180] = 20000
    assert [p["idx"] for p in detect(v, 168, 24, 168, P)] == [180]


def test_zero_median_uses_floor():
    ok, ratio, z = spike_rule(5000, 0.0, 0.0, P)
    assert ok and ratio == 100.0     # 5000 / floor 50, no division by zero


def test_episode_merging():
    pts = [{"idx": i, "views": float(v), "median": 1.0, "sigma": 0.0, "ratio": 1.0, "z": 1.0}
           for i, v in [(10, 5), (11, 9), (12, 7), (14, 3), (18, 4)]]
    eps = merge_episodes(pts, max_gap=2)
    assert [[p["idx"] for p in e] for e in eps] == [[10, 11, 12, 14], [18]]   # gap of 1 hour joins, gap of 3 splits
    s = summarise(eps[0])
    assert (s["start_idx"], s["end_idx"], s["peak_idx"], s["points"]) == (10, 14, 11, 4)


def test_per_language_override():
    cfg = {"spikes": {"global_hourly": {**P, "per_lang": {"hi": {"min_views": 40}}}}}
    assert params(cfg, "global_hourly", "hi")["min_views"] == 40
    assert params(cfg, "global_hourly", "en")["min_views"] == 1000
    assert "per_lang" not in params(cfg, "global_hourly", "hi")
