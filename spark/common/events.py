"""Event grouping, score and scopes (step 4.10). Plain Python, unit-tested on its own.

A node is one spike episode: a global (per-language, hourly) episode or a country
(daily) episode. Two nodes join the same event when
  1. they are about the same Wikidata item and peak within `same_entity_hours`
     (country spikes are daily, so for them "within one day" is used), or
  2. both are global (hourly) episodes, one item links to the other in its Wikidata
     claims and they peak within `window_hours`; very common link targets (countries,
     "human", hubs that many spiking items link to) are ignored, or everything would
     merge. Country episodes are daily, too coarse to be evidence of one story, so they
     join an event only through rule 1.
Connected nodes form an event (union-find). A component about more than `max_members`
different items is split again using rule 1 only (a long chain of links is not one story).
Many spikes of one item (one per language and country) are fine: that is one story.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import math
from collections import Counter, defaultdict

from enrichment.regions import LANG_STATES

GENERIC = {"Q5", "Q6256", "Q3624078", "Q7275", "Q515"}   # human, country, sovereign state, state, city


class UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def _close(a: dict, b: dict, hours: float) -> bool:
    if a["kind"] == "country" or b["kind"] == "country":
        return abs((a["peak"].date() - b["peak"].date()).days) <= 1
    return abs((a["peak"] - b["peak"]).total_seconds()) <= hours * 3600


def _same_entity(nodes: list[dict], idx: list[int], hours: float, uf: UnionFind) -> None:
    by_qid = defaultdict(list)
    for i in idx:
        if nodes[i].get("qid"):
            by_qid[nodes[i]["qid"]].append(i)
    for same in by_qid.values():
        for x in range(len(same)):
            for y in range(x + 1, len(same)):
                if _close(nodes[same[x]], nodes[same[y]], hours):
                    uf.union(same[x], same[y])


def _components(idx: list[int], uf: UnionFind) -> list[list[int]]:
    groups = defaultdict(list)
    for i in idx:
        groups[uf.find(i)].append(i)
    return list(groups.values())


def n_items(nodes: list[dict], idx: list[int]) -> int:
    """Number of different Wikidata items among these nodes (a node without one counts alone)."""
    return len({nodes[i].get("qid") or f"#{i}" for i in idx})


def group_events(nodes: list[dict], window_hours: float, same_entity_hours: float,
                 generic: set[str] = frozenset(), hub_limit: int = 25,
                 max_members: int | None = None, stats: dict | None = None) -> list[list[int]]:
    """Indices of nodes, grouped into events. Each node needs: kind, qid (or None),
    peak (datetime), links (list of QIDs). If `stats` is given, stats['split'] counts
    the oversized components that were split."""
    everyone = list(range(len(nodes)))
    uf = UnionFind(len(nodes))
    _same_entity(nodes, everyone, same_entity_hours, uf)        # rule 1: same item

    glob = [i for i in everyone if nodes[i]["kind"] == "global"]
    by_qid = defaultdict(list)
    for i in glob:
        if nodes[i].get("qid"):
            by_qid[nodes[i]["qid"]].append(i)
    # link targets that many spiking items point to are hubs, not evidence of one story
    target_count = Counter(t for i in glob for t in set(nodes[i].get("links") or []))
    skip = set(generic) | GENERIC | {t for t, c in target_count.items() if c > hub_limit}
    for i in glob:                                              # rule 2: one links to the other
        for t in set(nodes[i].get("links") or []) - skip:
            for j in by_qid.get(t, []):
                if j != i and _close(nodes[i], nodes[j], window_hours):
                    uf.union(i, j)

    out, split = [], 0
    for comp in _components(everyone, uf):
        if max_members and n_items(nodes, comp) > max_members:
            split += 1
            only_same = UnionFind(len(nodes))
            _same_entity(nodes, comp, same_entity_hours, only_same)
            out.extend(_components(comp, only_same))
        else:
            out.append(comp)
    if stats is not None:
        stats["split"] = split
    return out


def event_score(ratio: float, peak_views: float, baseline: float, cap: float = 100) -> float:
    """min(ratio, cap) x log10(extra views + 10): a big jump only ranks high with real volume."""
    return min(ratio, cap) * math.log10(max(peak_views - baseline, 0) + 10)


def scope_keys(has_global: bool, reader_countries, loc_country: str | None, loc_region: str | None,
               reader_states=()) -> list[str]:
    """G (worldwide, per language), R:<CC> (readers in a country), R:<CC>:<STATE> (readers of that
    state's language Wikipedia), A:<CC>[:<REGION>] (events about a place)."""
    keys = ["G"] if has_global else []
    keys += [f"R:{c}" for c in sorted(set(c for c in reader_countries if c))]
    keys += [f"R:{s.split('-')[0]}:{s}" for s in sorted(set(reader_states))]
    if loc_country:
        keys.append(f"A:{loc_country}")
        if loc_region and loc_region.startswith(loc_country + "-"):
            keys.append(f"A:{loc_country}:{loc_region}")
    return keys


def lang_states(glob: list[dict], cap: float = 100) -> list[dict]:
    """One entry per state whose language Wikipedia had a (non-bot) spike in this event, with the
    numbers of that language's biggest spike: used for the "Readers in <state>" lists."""
    best = {}
    for m in glob:
        if m.get("lang") in LANG_STATES and not m.get("suspicious"):
            if m["lang"] not in best or m["peak_views"] > best[m["lang"]]["peak_views"]:
                best[m["lang"]] = m
    out = []
    for lang, m in sorted(best.items()):
        for code in LANG_STATES[lang]:
            out.append({"code": code, "lang": lang, "ratio": float(min(m["ratio"], cap)),
                        "peak_views": int(m["peak_views"]), "baseline": float(m["baseline"]),
                        "first": m["start"], "peak": m["peak"]})
    return out


def build_event(members: list[dict], cap: float = 100) -> dict:
    """One event from its member nodes (global and/or country episodes with page_meta joined)."""
    glob = [m for m in members if m["kind"] == "global"]
    ctry = [m for m in members if m["kind"] == "country"]
    pool = glob or ctry
    lead = max(pool, key=lambda m: (m["peak_views"], m["ratio"]))

    cats = Counter(m.get("category") or "Other" for m in members)
    top = max(cats.values())
    category = lead.get("category") if cats.get(lead.get("category"), 0) == top else cats.most_common(1)[0][0]

    if lead.get("country"):
        loc = lead
    else:
        located = [m for m in members if m.get("country")]
        loc = Counter((m["country"], m.get("region_code")) for m in located).most_common(1)
        loc = next((m for m in located if (m["country"], m.get("region_code")) == loc[0][0]), None) if loc else None
    loc = loc or {}

    first = min(m["start"] for m in members)
    peak_member = max(pool, key=lambda m: m["peak_views"])
    ratio = float(min(max(m["ratio"] for m in members), cap))
    key = lead.get("qid") or "P" + hashlib.md5(f"{lead['lang']}:{lead['page_title']}".encode()).hexdigest()[:10]
    reader = sorted({m["reader_country"] for m in ctry})
    states = lang_states(glob, cap)
    return {
        "event_id": f"evt_{first:%Y%m%d}_{key}",
        "title": lead.get("label") or lead["page_title"],
        "category": category,
        "first_detected": first,
        "peak_time": peak_member["peak"],
        "day": peak_member["peak"].date() if isinstance(peak_member["peak"], dt.datetime) else peak_member["peak"],
        "score": float(event_score(ratio, lead["peak_views"], lead["baseline"], cap)),
        "ratio": ratio,
        "peak_views": int(lead["peak_views"]),
        "baseline_views": float(lead["baseline"]),
        "langs": sorted({m["lang"] for m in members if m.get("lang")}),
        "loc_country": loc.get("country"), "loc_region": loc.get("region_code"),
        "loc_region_name": loc.get("region_name"), "loc_city_qid": loc.get("city_qid"),
        "articles": [{"qid": m.get("qid"), "lang": m["lang"], "title": m["page_title"],
                      "peak_views": int(m["peak_views"]), "ratio": float(m["ratio"]),
                      "role": "lead" if m is lead else "member"}
                     for m in sorted(glob, key=lambda m: -m["peak_views"])],
        "countries": [{"code": m["reader_country"], "ratio": float(m["ratio"]), "peak_views": int(m["peak_views"]),
                       "day": m["peak"].date() if isinstance(m["peak"], dt.datetime) else m["peak"]}
                      for m in sorted(ctry, key=lambda m: -m["peak_views"])],
        "states": states,
        "scopes": scope_keys(bool(glob), reader, loc.get("country"), loc.get("region_code"),
                             [s["code"] for s in states]),
        "suspicious": bool(glob) and all(m.get("suspicious") for m in glob),
        "members": len(members),
        "lead_lang": lead["lang"], "lead_title": lead["page_title"],
    }
