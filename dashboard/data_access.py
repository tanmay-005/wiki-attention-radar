"""All dashboard reads go through here (docs/02_ARCHITECTURE.md section 4).

Cassandra answers the ranked lists, series and counters; MongoDB holds the full event
documents. Every function is cached for 5 minutes, so moving between filters that were
already seen is instant. The dashboard runs on Windows and reaches both databases through
the ports Docker publishes on 127.0.0.1 (settings.yaml, connections.on_host).
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys
from collections import Counter
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("RADAR_ENV", "on_host")

from enrichment.regions import STATE_NAMES, state_langs  # noqa: E402
from spark.common.config import conn, load  # noqa: E402  (plain YAML loader, no Spark needed)

KEYSPACE = "radar"
CFG = load()
LANGS = list(CFG["languages"])
CATEGORIES = ["Politics", "Sports", "Entertainment", "Science & Tech", "Business", "Disasters", "Other"]


class DataUnavailable(RuntimeError):
    """Cassandra or MongoDB cannot be reached."""


# ---- connections ---------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _cassandra():
    from cassandra.cluster import Cluster
    kw = {}
    try:                                   # Python 3.12+ has no asyncore; use the asyncio reactor there
        import asyncore  # noqa: F401
    except ImportError:
        from cassandra.io.asyncioreactor import AsyncioConnection
        kw["connection_class"] = AsyncioConnection
    c = conn(CFG)
    try:
        cluster = Cluster([c["cassandra_host"]], port=int(c["cassandra_port"]), connect_timeout=5, **kw)
        session = cluster.connect(KEYSPACE)
    except Exception as e:  # noqa: BLE001
        raise DataUnavailable(f"Cassandra at {c['cassandra_host']}:{c['cassandra_port']}: {e}") from e
    session.default_timeout = 20
    return session


@st.cache_resource(show_spinner=False)
def _mongo():
    from pymongo import MongoClient
    c = conn(CFG)
    client = MongoClient(c["mongo_uri"], serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as e:  # noqa: BLE001
        raise DataUnavailable(f"MongoDB at {c['mongo_uri']}: {e}") from e
    return client[KEYSPACE]


def reset_connections() -> None:
    _cassandra.clear()
    _mongo.clear()
    st.cache_data.clear()


def health() -> dict:
    """{'cassandra': bool, 'mongo': bool, 'has_data': bool, 'error': str | None}"""
    out = {"cassandra": False, "mongo": False, "has_data": False, "error": None}
    try:
        s = _cassandra()
        out["cassandra"] = True
        out["has_data"] = s.execute("SELECT event_id FROM event_rank LIMIT 1").one() is not None
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
    try:
        _mongo()
        out["mongo"] = True
    except Exception as e:  # noqa: BLE001
        out["error"] = (out["error"] + " | " if out["error"] else "") + str(e)
    return out


# ---- metadata ------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_meta() -> dict:
    """Data range and as-of day written by the loader (Mongo meta), with settings.yaml as fallback."""
    m = _mongo()["meta"].find_one({"_id": "serving"}) or {}
    p = CFG["period"]
    return {"as_of": dt.date.fromisoformat(m.get("as_of", str(p["hourly_end"]))),
            "detect_start": dt.date.fromisoformat(m.get("detect_start", str(p["detect_start"]))),
            "data_start": dt.date.fromisoformat(m.get("data_start", str(p["hourly_start"]))),
            "country_start": dt.date.fromisoformat(str(p["country_start"])),
            "pipeline_run": m.get("pipeline_run"), "events": m.get("events")}


def country_name(code: str, name: str | None) -> str:
    """Countries that appear only as a story's location were stored with their code as the name."""
    if name and name != code:
        return name
    if code == "XK":                              # Kosovo: used by Wikimedia, not in ISO 3166-1
        return "Kosovo"
    try:
        import pycountry
        hit = pycountry.countries.get(alpha_2=code)
        return getattr(hit, "common_name", None) or hit.name if hit else (name or code)
    except ImportError:
        return name or code


@st.cache_data(ttl=300, show_spinner=False)
def list_countries() -> list[dict]:
    """Countries present in the data: [{code, name, readers, about}], India first, then by name.
    readers = there are country (Readers in) spikes; about = events are located there."""
    db = _mongo()
    names, readers, about = {}, set(), set()
    for row in db["events"].aggregate([{"$unwind": "$countries"},
                                       {"$group": {"_id": "$countries.code", "name": {"$first": "$countries.name"}}}]):
        names[row["_id"]] = row["name"]
        readers.add(row["_id"])
    for row in db["events"].aggregate([{"$match": {"location.country": {"$ne": None}}},
                                       {"$group": {"_id": "$location.country", "name": {"$first": "$location.country_name"}}}]):
        names.setdefault(row["_id"], row["name"])
        about.add(row["_id"])
    out = [{"code": c, "name": country_name(c, names[c]), "readers": c in readers, "about": c in about}
           for c in names if c]
    return sorted(out, key=lambda d: (d["code"] != "IN", d["name"]))


@st.cache_data(ttl=300, show_spinner=False)
def list_regions(country: str) -> list[dict]:
    rows = _cassandra().execute(
        "SELECT region_code, region_name, event_count FROM region_index WHERE country = %s", (country,))
    return sorted(({"code": r.region_code, "name": r.region_name, "label": r.region_name, "events": r.event_count}
                   for r in rows),
                  key=lambda d: d["name"])


def reader_regions(country: str, langs=None) -> list[dict]:
    """States whose language Wikipedia is processed: the "Readers in <state>" choices."""
    langs = set(langs or LANGS)
    out = []
    for code, name in STATE_NAMES.items():
        ls = [l for l in state_langs(code) if l in langs]
        if code.startswith(country + "-") and ls:
            out.append({"code": code, "name": name, "langs": ls})
    names = {"ta": "Tamil", "te": "Telugu", "mr": "Marathi", "kn": "Kannada", "ml": "Malayalam", "bn": "Bengali",
             "gu": "Gujarati", "pa": "Punjabi", "or": "Odia", "as": "Assamese", "hi": "Hindi"}
    for r in out:
        r["label"] = f"{r['name']} · {' / '.join(names.get(l, l) for l in r['langs'])} Wikipedia"
    return sorted(out, key=lambda r: r["name"])


# ---- ranked events ---------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _prepared() -> dict:
    s = _cassandra()
    return {
        "rank": s.prepare("SELECT event_id, title, score, ratio, peak_views, first_seen, langs, place, sparkline, "
                          "suspicious FROM event_rank WHERE scope = ? AND day = ? AND category = ? LIMIT 2000"),
        "counts": s.prepare("SELECT category, spikes FROM spike_counts WHERE scope = ? AND day = ?"),
    }


def _per_day(name: str, args: list[tuple]) -> list:
    """Run one prepared query for many partitions (one per day) in parallel, results in input order.
    A window of 30 days is 30 partitions: sent together they cost about one round trip, not 30."""
    from cassandra.concurrent import execute_concurrent_with_args
    res = execute_concurrent_with_args(_cassandra(), _prepared()[name], args, concurrency=32,
                                       raise_on_first_error=True)
    return [list(r.result_or_exc) for r in res]


@st.cache_data(ttl=300, show_spinner=False)
def _rank_days(scope: str, days: tuple[dt.date, ...], category: str) -> list[dict]:
    out = []
    for rows in _per_day("rank", [(scope, d, category) for d in days]):
        out += [{"event_id": r.event_id, "title": r.title, "score": r.score, "ratio": r.ratio,
                 "peak_views": r.peak_views, "first_seen": r.first_seen, "langs": sorted(r.langs or []),
                 "place": r.place, "sparkline": list(r.sparkline or []), "suspicious": bool(r.suspicious),
                 "category": category} for r in rows]
    return out


def item_key(event_id: str) -> str:
    """'evt_20261025_Q123' -> 'Q123': the Wikidata item (or, without one, the page hash) an event is about."""
    tail = event_id.rsplit("_", 1)[-1]
    return tail if tail[:1] in ("Q", "P") else event_id


def get_top_events(scope: str, days: list[dt.date], category: str = "ALL",
                   langs: list[str] | None = None, limit: int | None = 50) -> list[dict]:
    """Ranked events for a scope over the given days: one partition per day, merged by
    Wikidata item (highest score kept), filtered by language, sorted by score."""
    best: dict[str, dict] = {}
    for e in _rank_days(scope, tuple(days), category):
        key = item_key(e["event_id"])                     # the same item spiking twice is one story
        if key not in best or e["score"] > best[key]["score"]:
            best[key] = e
    by_title: dict[str, dict] = {}                       # and two items with the same name read as one story
    for e in best.values():
        t = e["title"].casefold()
        if t not in by_title or e["score"] > by_title[t]["score"]:
            by_title[t] = e
    out = [e for e in by_title.values() if not langs or set(langs) & set(e["langs"])]
    out.sort(key=lambda e: (-e["score"], e["event_id"]))
    return out if limit is None else out[:limit]


@st.cache_data(ttl=300, show_spinner=False)
def get_daily_category_counts(scope: str, days: tuple[dt.date, ...]) -> dict[dt.date, dict[str, int]]:
    """{day: {category: new stories}} from the counter table spike_counts, all days in parallel."""
    return {d: {r.category: r.spikes for r in rows if r.category != "ALL"}
            for d, rows in zip(days, _per_day("counts", [(scope, d) for d in days]))}


def get_category_counts(scope: str, days: tuple[dt.date, ...]) -> dict[str, int]:
    """Spikes per category over the days."""
    counts = Counter()
    for c in get_daily_category_counts(scope, tuple(days)).values():
        counts.update(c)
    return dict(counts)


@st.cache_data(ttl=60, show_spinner=False)
def get_trending_now(scope: str, limit: int = 10) -> list[dict]:
    """Rows from the TTL table; empty once the 48 h TTL has passed since the last load."""
    rows = _cassandra().execute(
        "SELECT event_id, title, score, ratio, TTL(title) AS ttl FROM trending_now WHERE scope = %s LIMIT %s",
        (scope, limit))
    return [{"event_id": r.event_id, "title": r.title, "score": r.score, "ratio": r.ratio, "ttl": r.ttl} for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def get_categories(ids: tuple[str, ...]) -> dict[str, str]:
    """Category of each event (event_rank rows for category 'ALL' do not carry it)."""
    return {d["_id"]: d.get("category", "Other")
            for d in _mongo()["events"].find({"_id": {"$in": list(ids)}}, {"category": 1})}


@st.cache_data(ttl=300, show_spinner=False)
def get_cards(ids: tuple[str, ...]) -> dict[str, dict]:
    """What a story card needs from MongoDB: category, description, thumbnail, first lines, first news line."""
    out = {}
    for d in _mongo()["events"].find({"_id": {"$in": list(ids)}},
                                     {"category": 1, "about": 1, "news": {"$slice": 1}}):
        about = d.get("about") or {}
        out[d["_id"]] = {"category": d.get("category", "Other"), "description": about.get("description"),
                         "thumbnail": about.get("thumbnail"), "extract": about.get("extract"),
                         "news": (d.get("news") or [None])[0]}
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_country_counts(ids: tuple[str, ...]) -> list[dict]:
    """For these events: in how many of them readers in each country spiked."""
    rows = _mongo()["events"].aggregate([
        {"$match": {"_id": {"$in": list(ids)}}}, {"$unwind": "$countries"},
        {"$group": {"_id": "$countries.code", "name": {"$first": "$countries.name"}, "stories": {"$sum": 1}}},
        {"$sort": {"stories": -1}}])
    return [{"code": r["_id"], "name": r["name"], "stories": r["stories"]} for r in rows]


# ---- one event -------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_event(event_id: str) -> dict | None:
    return _mongo()["events"].find_one({"_id": event_id})


def lead_qid(event: dict) -> str | None:
    for a in event.get("articles") or []:
        if a.get("role") == "lead" and a.get("qid"):
            return a["qid"]
    m = re.search(r"_(Q\d+)(?:_\d+)?$", event["_id"])
    return m.group(1) if m else None


@st.cache_data(ttl=300, show_spinner=False)
def get_article_hourly(lang: str, title: str, start: dt.datetime, end: dt.datetime) -> list[dict]:
    rows = _cassandra().execute(
        "SELECT hour, views, baseline FROM article_hourly WHERE lang = %s AND title = %s AND hour >= %s AND hour <= %s",
        (lang, title, start, end))
    return [{"t": r.hour, "views": r.views, "baseline": r.baseline} for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def get_article_country_daily(qid: str, country: str, start: dt.date, end: dt.date) -> list[dict]:
    rows = _cassandra().execute(
        "SELECT day, views, baseline FROM article_country_daily WHERE qid = %s AND country = %s AND day >= %s AND day <= %s",
        (qid, country, start, end))
    return [{"day": r.day.date() if hasattr(r.day, "date") else dt.date.fromisoformat(str(r.day)),
             "views": r.views, "baseline": r.baseline} for r in rows]


# ---- validation and benchmarks pages ----------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_validation() -> list[dict]:
    return list(_mongo()["validation"].find({}, {"_id": 1, "name": 1, "date": 1, "scope": 1, "detected": 1,
                                                 "matched_event_id": 1, "matched_title": 1, "rank": 1,
                                                 "rank_g": 1, "rank_r_in": 1, "delay_hours": 1, "group": 1, "why_missed": 1,
                                                 "notes": 1}).sort("date", 1))


def get_validation_summary() -> dict:
    """Recall, delay, ranks and precision@50 written by S7 (empty before Phase 7 runs)."""
    return _mongo()["meta"].find_one({"_id": "validation"}) or {}
