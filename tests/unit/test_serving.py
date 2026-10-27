"""T-U10: rows for Cassandra and MongoDB built from one event (spark/common/serving.py)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.common.serving import (active_days, counter_keys, mongo_doc, place_label, rank_rows,  # noqa: E402
                                  region_index, sparkline, trending)

T = dt.datetime(2024, 6, 4, 3)
NAMES = {"IN": "India", "US": "United States"}


def event(**kw):
    e = {"event_id": "evt_20240604_Q1", "title": "Election", "category": "Politics", "first_detected": T,
         "peak_time": T + dt.timedelta(hours=30), "day": dt.date(2024, 6, 5), "score": 400.0, "ratio": 42.0,
         "peak_views": 9000, "baseline_views": 100.0, "langs": ["en", "hi"], "loc_country": "IN",
         "loc_region": "IN-RJ", "loc_region_name": "Rajasthan", "loc_city_qid": "Q66485",
         "articles": [{"qid": "Q1", "lang": "en", "title": "Election", "peak_views": 9000, "ratio": 42.0, "role": "lead"}],
         "countries": [{"code": "US", "ratio": 5.0, "peak_views": 1200, "day": dt.date(2024, 6, 4)}],
         "curve": [{"t": T + dt.timedelta(hours=h), "v": h, "b": 1.0} for h in range(97)],
         "scopes": ["G", "R:US", "A:IN", "A:IN:IN-RJ"], "suspicious": False, "members": 3,
         "lead_lang": "en", "lead_title": "Election"}
    e.update(kw)
    return e


def test_sparkline_has_12_points_with_bucket_peaks():
    s = sparkline(event()["curve"])
    assert len(s) == 12 and s[0] == 7 and s[-1] == 96 and s == sorted(s)
    assert sparkline([]) == [] and sparkline(None) == []


def test_place_label():
    assert place_label("Rajasthan", "IN", NAMES) == "Rajasthan, India"
    assert place_label(None, "IN", NAMES) == "India"
    assert place_label(None, "XX", NAMES) == "XX" and place_label("A", None, NAMES) is None


def test_active_days_from_first_detection_to_peak():
    assert active_days(T, T + dt.timedelta(hours=30)) == [dt.date(2024, 6, 4), dt.date(2024, 6, 5)]
    assert active_days(T, T - dt.timedelta(hours=5)) == [dt.date(2024, 6, 4)]
    assert len(active_days(T, T + dt.timedelta(days=20))) == 7


def test_rank_rows_cover_scopes_days_and_both_categories():
    rows = rank_rows(event(), NAMES)
    assert len(rows) == 4 * 2 * 2
    assert {r[2] for r in rows} == {"ALL", "Politics"}
    first = rows[0]
    assert first[:3] == ("G", dt.date(2024, 6, 4), "ALL") and first[10] == "Rajasthan, India"
    assert first[9] == {"en", "hi"} and len(first[11]) == 12


def test_counters_region_index_and_trending():
    assert len(counter_keys(event())) == 8
    evs = [event(), event(event_id="b", loc_region="IN-RJ"), event(event_id="c", loc_region=None),
           event(event_id="d", loc_region="IN.AP.EG")]          # not an ISO 3166-2 code: skipped
    assert region_index(evs) == [("IN", "IN-RJ", "Rajasthan", 2)]
    rows = trending([event(), event(event_id="z", score=500.0, suspicious=True)], dt.date(2024, 6, 5))
    assert {r[0] for r in rows} == {"G", "R:US", "A:IN", "A:IN:IN-RJ"} and all(r[2] != "z" for r in rows)


def test_mongo_doc_has_no_plain_dates():
    run = dt.datetime(2026, 10, 28)
    d = mongo_doc(event(), NAMES, {"Q66485": "Jaipur"}, run)
    assert d["_id"] == "evt_20240604_Q1" and d["location"]["city_name"] == "Jaipur"
    assert d["location"]["country_name"] == "India" and d["countries"][0]["name"] == "United States"

    def walk(x):
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        else:
            assert not (isinstance(x, dt.date) and not isinstance(x, dt.datetime)), x
    walk(d)
    assert mongo_doc(event(loc_country=None), NAMES, {}, run)["location"] is None
