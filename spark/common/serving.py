"""Rows for the serving stores (step 5.x), built from one S5 event. Plain Python, unit-tested
on its own (tests/unit/test_serving.py); spark/jobs/s6_load_serving.py writes them.

An event here is a dict with the fields of /radar/output/events (see s5_events.py SCHEMA).
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from enrichment.regions import STATE_NAMES, is_iso_region
from spark.common.events import event_score

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


def scope_numbers(e: dict, scope: str) -> tuple[float, float, int, list[dt.date]]:
    """(score, ratio, peak views, days listed) of an event in one scope.

    Worldwide and "Events about" scopes use the event's own hourly numbers. A "Readers in" scope
    (R:<CC>) uses that country's daily spike instead, so the list for India is ranked by how much
    more people *in India* read about it than they normally do, on the day they did. A state scope
    (R:<CC>:<STATE>) uses the hourly spike on that state's language Wikipedia."""
    if scope.startswith("R:") and scope.count(":") == 2:
        s = next((x for x in e.get("states") or [] if x["code"] == scope.split(":")[2]), None)
        if s:
            ratio, peak = float(s["ratio"]), int(s["peak_views"])
            return (float(event_score(ratio, peak, float(s["baseline"]))), ratio, peak,
                    active_days(s["first"], s["peak"]))
    elif scope.startswith("R:"):
        c = next((x for x in e.get("countries") or [] if x["code"] == scope[2:]), None)
        if c:
            ratio = min(float(c["ratio"]), 100.0)
            peak = int(c["peak_views"])
            day = c["day"].date() if isinstance(c["day"], dt.datetime) else c["day"]
            return float(event_score(ratio, peak, peak / max(float(c["ratio"]), 1e-9))), ratio, peak, [day]
    return float(e["score"]), float(e["ratio"]), int(e["peak_views"]), active_days(e["first_detected"], e["peak_time"])


def rank_rows(e: dict, country_names: dict[str, str]) -> list[tuple]:
    """event_rank rows: every scope x every day it is listed x ('ALL', the event's category)."""
    region_name = e.get("loc_region_name") if is_iso_region(e.get("loc_region")) else None
    place = place_label(region_name, e.get("loc_country"), country_names)
    spark = sparkline(e.get("curve"))
    rows = []
    for scope in e["scopes"]:
        score, ratio, peak, days = scope_numbers(e, scope)
        for day in days:
            for category in ("ALL", e["category"]):
                rows.append((scope, day, category, score, e["event_id"], e["title"], ratio, peak,
                             e["first_detected"], set(e.get("langs") or []), place, spark, bool(e["suspicious"])))
    return rows


def counter_keys(e: dict) -> list[tuple]:
    """spike_counts increments: one per scope and category ('ALL' and the event's own), on the day of
    first detection, or for a "Readers in" scope the day readers in that country spiked."""
    return [(scope, scope_numbers(e, scope)[3][0], c) for scope in e["scopes"] for c in ("ALL", e["category"])]


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
        if e["suspicious"]:
            continue
        for scope in e["scopes"]:
            score, ratio, _, days = scope_numbers(e, scope)
            if day in days:
                by_scope[scope].append((score, ratio, e))
    rows = []
    for scope, evs in sorted(by_scope.items()):
        for score, ratio, e in sorted(evs, key=lambda x: -x[0])[:top]:
            rows.append((scope, score, e["event_id"], e["title"], ratio))
    return rows


def mongo_doc(e: dict, country_names: dict[str, str], city_names: dict[str, str], run: dt.datetime,
              context: dict | None = None) -> dict:
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
        "states": [{**dict(s), "name": STATE_NAMES.get(s["code"], s["code"])} for s in (e.get("states") or [])],
        "curve": [{"t": p["t"], "v": int(p["v"]), "b": float(p["b"])} for p in (e.get("curve") or [])],
        "scopes": list(e["scopes"]),
        "suspicious": bool(e["suspicious"]),
        "members": int(e["members"]),
        "lead": {"lang": e["lead_lang"], "title": e["lead_title"]},
        "about": (context or {}).get("about"),           # from S5b: description, extract, thumbnail
        "news": (context or {}).get("news") or [],        # from S5b: matching lines of Wikipedia's news pages
        "pipeline_run": run,
    }
