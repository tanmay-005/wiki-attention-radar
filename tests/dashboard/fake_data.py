"""A stand-in for dashboard/data_access.py with a few made-up events (the names and news lines are
invented), so the Radar page can be tested and previewed without Cassandra or MongoDB."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from enrichment.regions import STATE_NAMES, state_langs  # noqa: E402

LANGS = ["en", "hi", "ta", "ja", "pt"]
CATEGORIES = ["Politics", "Sports", "Entertainment", "Science & Tech", "Business", "Disasters", "Other"]
AS_OF = dt.date(2026, 10, 26)
T = dt.datetime(2024, 6, 4, 6)


def _ev(eid, title, cat, scopes, langs, ratio=100.0, score=500.0, day=dt.date(2026, 10, 20), loc=None, susp=False,
        countries=(), desc=None, news=None, extract=None):
    first = dt.datetime.combine(day, dt.time(6))
    return {"_id": eid, "title": title, "category": cat, "first_detected": first, "peak_time": first + dt.timedelta(hours=4),
            "score": score, "ratio": ratio, "peak_views": 90000, "baseline_views": 900.0, "langs": langs,
            "location": loc, "scopes": scopes, "suspicious": susp,
            "articles": [{"qid": eid.split("_")[-1], "lang": langs[0], "title": title, "peak_views": 90000,
                          "ratio": ratio, "role": "lead"}],
            "countries": [{"code": c, "name": n, "ratio": 20.0, "peak_views": 30000,
                           "day": dt.datetime.combine(day, dt.time())} for c, n in countries],
            "lead": {"lang": langs[0], "title": title},
            "about": {"lang": langs[0], "title": title, "description": desc, "thumbnail": None,
                      "extract": extract or (f"{title} is an example article used to preview the dashboard. " * 3).strip()},
            "news": [{"date": first, "kind": "news", "section": "Politics and elections", "topic": None, "text": news}]
            if news else []}


IN = {"country": "IN", "country_name": "India", "region": "IN-RJ", "region_name": "Rajasthan", "city_name": None}
EVENTS = [
    _ev("evt_20261020_Q1", "2026 Exampleland general election", "Politics", ["G", "R:IN", "A:IN"], ["en", "hi"],
        score=510, countries=[("IN", "India"), ("US", "United States")], loc=dict(IN, region=None, region_name=None),
        desc="General election in Exampleland", news="Counting begins in the Exampleland general election; "
        "the governing alliance leads in early results."),
    _ev("evt_20261020_Q2", "Jodhpur Example constituency", "Politics", ["G", "R:IN", "A:IN", "A:IN:IN-RJ"], ["en", "hi"],
        score=420, loc=IN, countries=[("IN", "India")], desc="Constituency in Rajasthan, India"),
    _ev("evt_20261025_Q3", "Sample Player", "Sports", ["G", "R:US", "R:IN"], ["en", "ja", "pt"], score=524,
        day=dt.date(2026, 10, 25), countries=[("US", "United States"), ("IN", "India")],
        desc="Example tennis player (born 2003)", news="Sample Player wins the Example Open final in four sets."),
    _ev("evt_20261023_Q4", "Bot page", "Other", ["G"], ["en"], score=480, day=dt.date(2026, 10, 23), susp=True,
        desc="Disambiguation page"),
]
for i in range(5, 12):
    EVENTS.append(_ev(f"evt_20261024_Q{i}", f"Example story {i}", ["Entertainment", "Science & Tech", "Business"][i % 3],
                      ["G"], ["en", "pt"], score=470 - i, day=dt.date(2026, 10, 24), desc=f"Example description {i}",
                      news=f"Example news line for story {i}." if i % 2 else None,
                      countries=[("US", "United States"), ("GB", "United Kingdom"), ("BR", "Brazil"),
                                 ("JP", "Japan"), ("DE", "Germany")][: 1 + i % 5]))
TN = _ev("evt_20261022_Q20", "Example Tamil film", "Entertainment", ["R:IN:IN-TN"], ["ta"], score=300,
         day=dt.date(2026, 10, 22), desc="2026 Tamil-language film")
TN["states"] = [{"code": "IN-TN", "name": "Tamil Nadu", "lang": "ta", "ratio": 40.0, "peak_views": 900,
                 "baseline": 20.0, "first": TN["first_detected"], "peak": TN["peak_time"]}]
EVENTS.append(TN)
BY_ID = {e["_id"]: e for e in EVENTS}


def health():
    return {"cassandra": True, "mongo": True, "has_data": True, "error": None}


def reset_connections():
    pass


def get_meta():
    return {"as_of": AS_OF, "detect_start": dt.date(2026, 10, 13), "data_start": dt.date(2026, 10, 6),
            "country_start": dt.date(2026, 9, 26), "pipeline_run": dt.datetime(2026, 10, 28, 3, 0), "events": len(EVENTS)}


def list_countries():
    return [{"code": "IN", "name": "India", "readers": True, "about": True},
            {"code": "FR", "name": "France", "readers": False, "about": True},
            {"code": "US", "name": "United States", "readers": True, "about": False}]


def list_regions(country):
    return [{"code": "IN-RJ", "name": "Rajasthan", "label": "Rajasthan", "events": 1}] if country == "IN" else []


def reader_regions(country, langs=None):
    langs = set(langs or LANGS)
    out = [{"code": c, "name": n, "langs": [l for l in state_langs(c) if l in langs]}
           for c, n in STATE_NAMES.items() if c.startswith(country + "-")]
    return [dict(r, label=f"{r['name']} · Wikipedia") for r in sorted(out, key=lambda r: r["name"]) if r["langs"]]


def get_top_events(scope, days, category="ALL", langs=None, limit=50):
    out = []
    for e in EVENTS:
        if scope in e["scopes"] and e["first_detected"].date() in days and category in ("ALL", e["category"]):
            if not langs or set(langs) & set(e["langs"]):
                out.append({"event_id": e["_id"], "title": e["title"], "score": e["score"], "ratio": e["ratio"],
                            "peak_views": e["peak_views"], "first_seen": e["first_detected"], "langs": e["langs"],
                            "place": "India", "sparkline": [1, 2, 9, 4], "suspicious": e["suspicious"],
                            "category": category})
    out.sort(key=lambda e: -e["score"])
    return out if limit is None else out[:limit]


def get_categories(ids):
    return {i: BY_ID[i]["category"] for i in ids}


def get_cards(ids):
    out = {}
    for i in ids:
        e = BY_ID[i]
        out[i] = {"category": e["category"], "description": e["about"]["description"], "thumbnail": None,
                  "extract": e["about"]["extract"], "news": (e["news"] or [None])[0]}
    return out


def get_category_counts(scope, days):
    c = {}
    for e in get_top_events(scope, list(days), limit=None):
        k = BY_ID[e["event_id"]]["category"]
        c[k] = c.get(k, 0) + 1
    return c


def get_trending_now(scope, limit=10):
    return []


def get_event(eid):
    return BY_ID.get(eid)


def lead_qid(e):
    return e["articles"][0]["qid"]


def get_article_hourly(lang, title, start, end):
    return [{"t": start + dt.timedelta(hours=h), "views": 1000 * (h == 24) + 100, "baseline": 100} for h in range(49)]


def get_article_country_daily(qid, country, start, end):
    n = (end - start).days + 1
    return [{"day": start + dt.timedelta(days=i), "views": 500 + 5000 * (i == n - 3), "baseline": 500} for i in range(n)]


def get_validation():
    return []


def get_country_counts(ids):
    c = {}
    for i in ids:
        for x in BY_ID[i]["countries"]:
            c.setdefault(x["code"], {"code": x["code"], "name": x["name"], "stories": 0})["stories"] += 1
    return sorted(c.values(), key=lambda d: -d["stories"])
