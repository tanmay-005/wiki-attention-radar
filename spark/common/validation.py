"""Detection validation (step 7.x): known events against detected events. Plain Python, unit-tested
on its own (tests/unit/test_validation.py); spark/jobs/s7_validate.py runs it on the real output.

A known event (data/validation/known_events_2026-10.csv) is found when a detected event
  1. contains one of its expected Wikidata items (an article of the event, or the item it is named
     after), and
  2. was first detected between the start of the day before the listed date and 48 hours after the
     listed day ends (a 4-day window: the day before covers time zones and build-up, the 48 hours
     cover late spikes such as a death reported the next day).
When several events match, the one with the highest score is kept.

Rank: the position of the matched event in the ranked list of a scope (G, R:IN, A:IN, ...) on the
days it is listed there, best position kept. It is the rank the dashboard shows for that day.
"""
from __future__ import annotations

import datetime as dt
import statistics
from collections import defaultdict

from spark.common.serving import scope_numbers

BEFORE = dt.timedelta(days=1)
AFTER = dt.timedelta(days=3)          # the listed day itself + 48 hours


def item_key(event_id: str) -> str:
    """'evt_20261025_Q123' -> 'Q123' (the page hash 'P...' for an event without a Wikidata item)."""
    return event_id.rsplit("_", 1)[-1]


def event_items(e: dict) -> set[str]:
    return {a["qid"] for a in (e.get("articles") or []) if a.get("qid")} | {item_key(e["event_id"])}


def in_window(first_detected: dt.datetime, day: dt.date) -> bool:
    start = dt.datetime.combine(day, dt.time())
    return start - BEFORE <= first_detected < start + AFTER


def delay_hours(first_detected: dt.datetime, day: dt.date) -> float:
    """Hours from the start of the listed day (UTC) to the first detection; negative = before it."""
    return round((first_detected - dt.datetime.combine(day, dt.time())).total_seconds() / 3600, 1)


def find_match(qids: set[str], day: dt.date, events: list[dict]) -> dict | None:
    hits = [e for e in events if qids & event_items(e) and in_window(e["first_detected"], day)]
    return max(hits, key=lambda e: e["score"]) if hits else None


def outside_window(qids: set[str], day: dt.date, events: list[dict]) -> str | None:
    """For a missed known event: was the same item detected at another time? (e.g. a death that was
    only announced days later). Returns a short note, or None."""
    hits = [e for e in events if qids & event_items(e)]
    if not hits:
        return None
    e = min(hits, key=lambda e: abs(delay_hours(e["first_detected"], day)))
    h = delay_hours(e["first_detected"], day)
    when = f"{h / 24:.1f} days after the listed day" if h >= 0 else f"{-h / 24:.1f} days before the listed day"
    return f"detected {when} ({e['first_detected']:%d %b %H:%M}), outside the window"


def scope_ranks(events: list[dict], scope: str) -> dict[str, int]:
    """event_id -> best rank (1 = top) in a scope's daily lists; suspicious events are listed too,
    as the dashboard lists them (with a warning)."""
    by_day = defaultdict(list)
    for e in events:
        if scope in e["scopes"]:
            score, _, _, days = scope_numbers(e, scope)
            for d in days:
                by_day[d].append((score, e["event_id"]))
    best = {}
    for rows in by_day.values():
        for i, (_, eid) in enumerate(sorted(rows, key=lambda x: (-x[0], x[1])), start=1):
            best[eid] = min(best.get(eid, i), i)
    return best


def validate(known: list[dict], qids_of: dict[str, set[str]], events: list[dict]) -> list[dict]:
    """One result per known event. known: {id, name, date, scope, expected_titles, notes};
    qids_of: known id -> its expected Wikidata items."""
    ranks = {}
    out = []
    for k in known:
        m = find_match(qids_of.get(k["id"], set()), k["date"], events)
        r = {"_id": k["id"], "name": k["name"], "date": k["date"], "scope": k["scope"],
             "expected_titles": k["expected_titles"], "expected_qids": sorted(qids_of.get(k["id"], set())),
             "detected": m is not None, "matched_event_id": m["event_id"] if m else None,
             "matched_title": m["title"] if m else None, "first_detected": m["first_detected"] if m else None,
             "delay_hours": delay_hours(m["first_detected"], k["date"]) if m else None,
             "rank": None, "rank_g": None, "rank_r_in": None, "group": k.get("group") or "",
             "why_missed": None if m else outside_window(qids_of.get(k["id"], set()), k["date"], events),
             "notes": k.get("notes") or ""}
        if m:
            for scope, field in ((k["scope"], "rank"), ("G", "rank_g"), ("R:IN", "rank_r_in")):
                if scope not in ranks:
                    ranks[scope] = scope_ranks(events, scope)
                r[field] = ranks[scope].get(m["event_id"])
        out.append(r)
    return out


def summary(results: list[dict]) -> dict:
    found = [r for r in results if r["detected"]]
    delays = [r["delay_hours"] for r in found if r["delay_hours"] is not None]
    ranks = [r["rank"] for r in found if r["rank"] is not None]
    india = [r for r in found if r["scope"].startswith(("R:IN", "A:IN"))]
    both = [r for r in india if r["rank_r_in"] is not None]
    better = [r for r in both if r["rank_g"] is None or r["rank_r_in"] < r["rank_g"]]
    groups = {}
    for r in results:
        g = groups.setdefault(r.get("group") or "all", {"known": 0, "found": 0})
        g["known"] += 1
        g["found"] += int(r["detected"])
    return {
        "known": len(results), "found": len(found), "by_group": groups,
        "recall": round(len(found) / len(results), 3) if results else None,
        "median_delay_hours": statistics.median(delays) if delays else None,
        "median_rank": statistics.median(ranks) if ranks else None,
        "top10": sum(1 for x in ranks if x <= 10),
        "india_found": len(india), "india_in_r_in": len(both), "india_higher_in_r_in": len(better),
    }


def top_events(events: list[dict], n: int = 50) -> list[dict]:
    """The top n worldwide stories of the detection period, as the dashboard would list them over
    the whole period: one row per Wikidata item (highest score), then per title."""
    best = {}
    for e in events:
        if "G" in e["scopes"]:
            k = item_key(e["event_id"])
            if k not in best or e["score"] > best[k]["score"]:
                best[k] = e
    by_title = {}
    for e in best.values():
        t = e["title"].casefold()
        if t not in by_title or e["score"] > by_title[t]["score"]:
            by_title[t] = e
    return sorted(by_title.values(), key=lambda e: (-e["score"], e["event_id"]))[:n]


def precision(labels: list[dict]) -> dict:
    """labels: rows with is_real in {0, 1} (blank = not labelled yet)."""
    done = [r for r in labels if str(r.get("is_real", "")).strip() in ("0", "1")]
    real = sum(1 for r in done if str(r["is_real"]).strip() == "1")
    return {"labelled": len(done), "real": real, "precision": round(real / len(done), 3) if done else None}
