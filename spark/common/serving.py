"""Rows for the serving stores (step 5.x), built from one S5 event. Plain Python, unit-tested
on its own (tests/unit/test_serving.py); spark/jobs/s6_load_serving.py writes them.

An event here is a dict with the fields of /radar/output/events (see s5_events.py SCHEMA).
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from enrichment.regions import is_iso_region

MAX_ACTIVE_DAYS = 7


def as_datetime(v):
    """BSON has no date type: dates become midnight UTC datetimes, everything else is unchanged."""
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return dt.datetime.combine(v, dt.time())
    return v


def sparkline(curve: list[dict] | None, points: int = 12) -> list[int]:
    """The ±48 h curve squeezed into `points` buckets (the highest hour of each)."""
    values = [int(p["v"]) for p in sorted(curve or [], key=lambda p: p["t"])]
    if not values:
        return []
    n = min(points, len(values))
    cuts = [round(i * len(values) / n) for i in range(n + 1)]   # n buckets of (almost) equal length
    return [max(values[cuts[i]:cuts[i + 1]]) for i in range(n)]


def place_label(region_name: str | None, country: str | None, country_names: dict[str, str]) -> str | None:
    """'Rajasthan, India', 'India', or None."""
    if not country:
        return None
    name = country_names.get(country, country)
    return f"{region_name}, {name}" if region_name else name


def active_days(first_detected: dt.datetime, peak_time: dt.datetime) -> list[dt.date]:
    """Days an event is listed on: from its first detection to its peak (at most a week)."""
    start = first_detected.date()
    end = max(start, peak_time.date())
    n = min((end - start).days + 1, MAX_ACTIVE_DAYS)
    return [start + dt.timedelta(days=i) for i in range(n)]


def rank_rows(e: dict, country_names: dict[str, str]) -> list[tuple]:
    """event_rank rows: every scope x every active day x ('ALL', the event's category)."""
    region_name = e.get("loc_region_name") if is_iso_region(e.get("loc_region")) else None
    place = place_label(region_name, e.get("loc_country"), country_names)
    spark = sparkline(e.get("curve"))
    rows = []
    for scope in e["scopes"]:
        for day in active_days(e["first_detected"], e["peak_time"]):
            for category in ("ALL", e["category"]):
                rows.append((scope, day, category, float(e["score"]), e["event_id"], e["title"], float(e["ratio"]),
                             int(e["peak_views"]), e["first_detected"], set(e.get("langs") or []), place, spark,
                             bool(e["suspicious"])))
    return rows


def counter_keys(e: dict) -> list[tuple]:
    """spike_counts increments: one per scope on the day of first detection, for 'ALL' and the category."""
    day = e["first_detected"].date()
    return [(scope, day, c) for scope in e["scopes"] for c in ("ALL", e["category"])]


def region_index(events: list[dict]) -> list[tuple]:
    """(country, region_code, region_name, event_count) for every region that has events about it."""
    names, counts = {}, defaultdict(int)
    for e in events:
        if e.get("loc_country") and is_iso_region(e.get("loc_region")):
            key = (e["loc_country"], e["loc_region"])
            counts[key] += 1
            names.setdefault(key, e.get("loc_region_name") or e["loc_region"])
    return [(c, r, names[(c, r)], n) for (c, r), n in sorted(counts.items())]


def trending(events: list[dict], day: dt.date, top: int = 20) -> list[tuple]:
    """trending_now rows: the top events per scope among those listed on `day`."""
    by_scope = defaultdict(list)
    for e in events:
        if day in active_days(e["first_detected"], e["peak_time"]) and not e["suspicious"]:
            for scope in e["scopes"]:
                by_scope[scope].append(e)
    rows = []
    for scope, evs in sorted(by_scope.items()):
        for e in sorted(evs, key=lambda x: -x["score"])[:top]:
            rows.append((scope, float(e["score"]), e["event_id"], e["title"], float(e["ratio"])))
    return rows


def mongo_doc(e: dict, country_names: dict[str, str], city_names: dict[str, str], run: dt.datetime) -> dict:
    """The MongoDB `events` document (docs/03_DATA_MODEL.md section 5.1)."""
    loc = None
    if e.get("loc_country"):
        ok = is_iso_region(e.get("loc_region"))
        loc = {"country": e["loc_country"], "country_name": country_names.get(e["loc_country"], e["loc_country"]),
               "region": e["loc_region"] if ok else None, "region_name": e.get("loc_region_name") if ok else None,
               "city_qid": e.get("loc_city_qid"), "city_name": city_names.get(e.get("loc_city_qid")),
               "source": "wikidata"}
    return {
        "_id": e["event_id"],
        "title": e["title"],
        "category": e["category"],
        "first_detected": e["first_detected"],
        "peak_time": e["peak_time"],
        "day": as_datetime(e["day"]),
        "score": float(e["score"]),
        "ratio": float(e["ratio"]),
        "peak_views": int(e["peak_views"]),
        "baseline_views": float(e["baseline_views"]),
        "langs": list(e.get("langs") or []),
        "location": loc,
        "articles": [dict(a) for a in (e.get("articles") or [])],
        "countries": [{**dict(c), "name": country_names.get(c["code"], c["code"]), "day": as_datetime(c["day"])}
                      for c in (e.get("countries") or [])],
        "curve": [{"t": p["t"], "v": int(p["v"]), "b": float(p["b"])} for p in (e.get("curve") or [])],
        "scopes": list(e["scopes"]),
        "suspicious": bool(e["suspicious"]),
        "members": int(e["members"]),
        "lead": {"lang": e["lead_lang"], "title": e["lead_title"]},
        "pipeline_run": run,
    }
