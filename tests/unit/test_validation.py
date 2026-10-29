"""Step 7.x: matching known events to detected events (spark/common/validation.py)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.common.validation import (delay_hours, find_match, in_window, precision, scope_ranks,  # noqa: E402
                                     summary, top_events, validate)

D = dt.date(2026, 10, 20)


def ev(eid, score, first, scopes=("G",), qids=(), title=None, countries=()):
    first = dt.datetime.combine(D, dt.time()) + dt.timedelta(hours=first)
    return {"event_id": eid, "title": title or eid, "score": float(score), "ratio": 10.0, "peak_views": 1000,
            "first_detected": first, "peak_time": first + dt.timedelta(hours=3), "scopes": list(scopes),
            "articles": [{"qid": q} for q in qids], "countries": list(countries), "suspicious": False}


def test_window_is_day_before_to_48h_after():
    start = dt.datetime.combine(D, dt.time())
    assert in_window(start - dt.timedelta(hours=24), D) and not in_window(start - dt.timedelta(hours=25), D)
    assert in_window(start + dt.timedelta(hours=71), D) and not in_window(start + dt.timedelta(hours=72), D)
    assert delay_hours(start + dt.timedelta(hours=5, minutes=30), D) == 5.5
    assert delay_hours(start - dt.timedelta(hours=3), D) == -3.0


def test_match_by_article_or_event_item_and_best_score():
    events = [ev("evt_20261020_Q1", 50, 2), ev("evt_20261020_Q9", 90, 4, qids=["Q1"]),
              ev("evt_20261010_Q1", 500, -240)]                         # same item, too early
    assert find_match({"Q1"}, D, events)["event_id"] == "evt_20261020_Q9"
    assert find_match({"Q7"}, D, events) is None


def test_ranks_per_day_best_position():
    events = [ev("evt_a_Q1", 100, 1), ev("evt_b_Q2", 200, 1), ev("evt_c_Q3", 50, 30)]
    r = scope_ranks(events, "G")
    assert r["evt_b_Q2"] == 1 and r["evt_a_Q1"] == 2
    assert r["evt_c_Q3"] == 1                                            # alone on the next day


def test_validate_summary_and_india_relevance():
    events = [ev("evt_x_Q1", 300, 3, scopes=("G", "R:IN"), countries=[
                  {"code": "IN", "ratio": 40.0, "peak_views": 50000, "day": D}]),
              ev("evt_y_Q2", 900, 2, scopes=("G",)), ev("evt_z_Q3", 10, 2)]
    known = [{"id": "k1", "name": "Indian thing", "date": D, "scope": "R:IN", "expected_titles": "A", "notes": ""},
             {"id": "k2", "name": "Missed", "date": D, "scope": "G", "expected_titles": "B", "notes": ""}]
    res = validate(known, {"k1": {"Q1"}, "k2": {"Q5"}}, events)
    assert res[0]["detected"] and res[0]["rank"] == 1 and res[0]["rank_g"] == 2 and res[0]["delay_hours"] == 3.0
    assert not res[1]["detected"] and res[1]["rank"] is None
    s = summary(res)
    assert (s["known"], s["found"], s["recall"], s["india_higher_in_r_in"]) == (2, 1, 0.5, 1)


def test_top_events_dedupe_and_precision():
    events = [ev("evt_20261020_Q1", 100, 0, title="A"), ev("evt_20261022_Q1", 300, 48, title="A"),
              ev("evt_20261020_Q2", 200, 0, title="a"), ev("evt_20261020_Q3", 50, 0, scopes=("R:IN",))]
    top = top_events(events, 5)
    assert [e["event_id"] for e in top] == ["evt_20261022_Q1"]
    p = precision([{"is_real": "1"}, {"is_real": "0"}, {"is_real": "1"}, {"is_real": ""}])
    assert p == {"labelled": 3, "real": 2, "precision": 0.667}
