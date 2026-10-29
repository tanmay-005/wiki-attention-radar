#!/usr/bin/env python3
"""Benchmark B6 (T-P2, T-P3): how fast the serving stores answer the dashboard's own queries.

Cassandra: each query runs 30 times with a prepared statement; the client time is the median, and
the server time comes from query tracing (TRACING ON) on one run. MongoDB: the same, plus
explain("executionStats") to show which index the query used.

  benchmarks/results/b6_latency.csv   store, query, rows, median_ms, p95_ms, server_ms, plan

Run inside edge (Cassandra and MongoDB up):   python3 benchmarks/b6_latency.py
"""
from __future__ import annotations

import csv
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from spark.common.config import conn, load  # noqa: E402

RUNS = 30
OUT = ROOT / "benchmarks" / "results" / "b6_latency.csv"


def timed(fn, runs: int = RUNS) -> tuple[float, float, int]:
    fn()                                                    # warm-up (connection, prepared statement)
    ts, n = [], 0
    for _ in range(runs):
        t = time.perf_counter()
        n = fn()
        ts.append(1000 * (time.perf_counter() - t))
    ts.sort()
    return statistics.median(ts), ts[int(0.95 * (len(ts) - 1))], n


def main() -> None:
    cfg = load()
    c = conn(cfg)
    from cassandra.cluster import Cluster
    from pymongo import MongoClient

    s = Cluster([c["cassandra_host"]], port=int(c["cassandra_port"])).connect("radar")
    db = MongoClient(c["mongo_uri"], serverSelectionTimeoutMS=5000)["radar"]
    meta = db["meta"].find_one({"_id": "serving"}) or {}
    import datetime as dt
    day = dt.date.fromisoformat(meta.get("as_of", str(cfg["period"]["hourly_end"])))
    top = s.execute("SELECT event_id, title FROM event_rank WHERE scope='G' AND day=%s AND category='ALL' LIMIT 1",
                    (day,)).one()
    ev = db["events"].find_one({"_id": top.event_id}, {"lead": 1, "peak_time": 1, "articles": 1})
    lead = ev["lead"]
    peak = ev["peak_time"]
    qid = next((a["qid"] for a in ev.get("articles") or [] if a.get("qid")), None)

    cq = [
        ("Q1 top stories: one scope, day, category",
         "SELECT event_id, title, score FROM event_rank WHERE scope=? AND day=? AND category=? LIMIT 100",
         ("G", day, "ALL")),
        ("Q1 top stories, readers in India",
         "SELECT event_id, title, score FROM event_rank WHERE scope=? AND day=? AND category=? LIMIT 100",
         ("R:IN", day, "ALL")),
        ("Q2 stories per category (counters)",
         "SELECT category, spikes FROM spike_counts WHERE scope=? AND day=?", ("G", day)),
        ("Q3 hourly series of one article (48 h)",
         "SELECT hour, views, baseline FROM article_hourly WHERE lang=? AND title=? AND hour>=? AND hour<=?",
         (lead["lang"], lead["title"], peak - dt.timedelta(hours=24), peak + dt.timedelta(hours=24))),
        ("Q4 trending now (TTL table)",
         "SELECT event_id, title, score FROM trending_now WHERE scope=? LIMIT 10", ("G",)),
    ]
    rows = []
    for name, cql, params in cq:
        st = s.prepare(cql)
        med, p95, n = timed(lambda: len(list(s.execute(st, params))))
        tr = s.execute(st, params, trace=True).get_query_trace(max_wait_sec=5)
        server = tr.duration.total_seconds() * 1000 if tr and tr.duration else None
        rows.append(["cassandra", name, n, f"{med:.2f}", f"{p95:.2f}", f"{server:.2f}" if server else "",
                     "single partition read (partition key in WHERE)"])

    def plan(cur_explain) -> tuple[str, float]:
        stats = cur_explain.get("executionStats", {})
        stages = []

        def walk(p):
            if not isinstance(p, dict):
                return
            stages.append(p.get("stage") or p.get("inputStage", {}).get("stage", ""))
            if p.get("indexName"):
                stages[-1] += f" ({p['indexName']})"
            for k in ("inputStage", "queryPlan"):
                walk(p.get(k))
            for x in p.get("inputStages") or []:
                walk(x)
        walk((cur_explain.get("queryPlanner") or {}).get("winningPlan"))
        return " > ".join(x for x in stages if x), stats.get("executionTimeMillis", 0)

    mq = [
        ("Q5 one story by _id (detail panel)", {"_id": top.event_id}, None),
        ("Q6 stories located in India", {"location.country": "IN"}, None),
        ("Q7 stories containing one Wikidata item", {"articles.qid": qid} if qid else {"_id": top.event_id}, None),
        ("Q8 stories in one category, newest first", {"category": "Politics"}, [("first_detected", -1)]),
    ]
    for name, flt, sort in mq:
        def run(f=flt, so=sort):
            cur = db["events"].find(f, {"title": 1})
            if so:
                cur = cur.sort(so).limit(50)
            return len(list(cur))
        med, p95, n = timed(run)
        cur = db["events"].find(flt, {"title": 1})
        if sort:
            cur = cur.sort(sort).limit(50)
        p, server = plan(cur.explain())
        rows.append(["mongodb", name, n, f"{med:.2f}", f"{p95:.2f}", f"{server:.2f}", p])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["store", "query", "rows", "median_ms", "p95_ms", "server_ms", "plan"])
        w.writerows(rows)
    print(f"day {day}, story {top.title!r}\n")
    print(f"{'store':<10}{'rows':>6}{'median ms':>11}{'p95 ms':>9}{'server ms':>11}  query / plan")
    for r in rows:
        print(f"{r[0]:<10}{r[2]:>6}{r[3]:>11}{r[4]:>9}{r[5]:>11}  {r[1]}  [{r[6]}]")
    print(f"\nsaved {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
