"""Region resolution (step 4.8): where in the world an item is about.

  region : walk up "located in the administrative territorial entity" (P131) from the
           item, or from its location (P276) / headquarters (P159), at most 5 hops,
           until an item with an ISO 3166-2 code (P300, e.g. IN-UP) is found.
  country: the ISO 3166-1 code (P297) of the item's country (P17), else of the
           citizenship (P27, for people), else the prefix of the region code.
  city   : the first hop, when it is an instance of a city/town class.
"""
from __future__ import annotations

from typing import Callable

MAX_HOPS = 5


def resolve(item: dict, get: Callable[[list[str]], dict[str, dict]], city_classes: set[str]) -> dict:
    out = {"region_code": None, "region_name": None, "city_qid": None, "country": None}

    starts = list(item.get("located_in") or []) + list(item.get("location") or []) + list(item.get("headquarters") or [])
    if item.get("iso3166_2"):                       # the item itself is a state / region
        out["region_code"], out["region_name"] = item["iso3166_2"], item.get("label")
    elif starts:
        first = get(starts)
        for q in starts:
            if q in first and set(first[q].get("instance_of") or []) & city_classes:
                out["city_qid"] = q
                break
        frontier, seen = starts, set()
        for _ in range(MAX_HOPS):
            recs = get([q for q in frontier if q not in seen])
            seen.update(frontier)
            hit = next((recs[q] for q in frontier if q in recs and recs[q].get("iso3166_2")), None)
            if hit:
                out["region_code"], out["region_name"] = hit["iso3166_2"], hit.get("label")
                break
            frontier = [p for q in frontier if q in recs for p in (recs[q].get("located_in") or [])]
            if not frontier:
                break

    for prop in ("country", "citizenship"):
        qs = item.get(prop) or []
        if qs:
            recs = get(qs[:1])
            code = (recs.get(qs[0]) or {}).get("iso3166_1")
            if code:
                out["country"] = code
                break
    if not out["country"] and item.get("iso3166_1"):           # the item is a country
        out["country"] = item["iso3166_1"]
    if not out["country"] and out["region_code"]:
        out["country"] = out["region_code"].split("-")[0]
    return out
