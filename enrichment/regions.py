"""Region resolution (step 4.8): where in the world an item is about.

  region : walk up "located in the administrative territorial entity" (P131) from the
           item, or from its location (P276) / headquarters (P159), at most 5 hops,
           until an item with an ISO 3166-2 code (P300, e.g. IN-UP) is found.
           Politicians have none of these, so for them the walk starts from the seat
           they hold (electoral district, P768). Place of birth is not used: being born
           somewhere does not make a story about that place.
  country: the ISO 3166-1 code (P297) of the item's country (P17), else of the
           citizenship (P27, for people), else the prefix of the region code.
  city   : the first hop, when it is an instance of a city/town class.
"""
from __future__ import annotations

import re

from typing import Callable

MAX_HOPS = 5


# "Readers in" a state. Wikimedia publishes reader locations only per country, so a state's
# readers are approximated by the Wikipedia in its main language: a spike on Tamil Wikipedia
# is listed under Tamil Nadu. Hindi Wikipedia is read across north India, so those states
# share one list. Codes are ISO 3166-2:IN (2023 edition).
LANG_STATES = {
    "ta": ["IN-TN"], "te": ["IN-AP", "IN-TG"], "mr": ["IN-MH"], "kn": ["IN-KA"], "ml": ["IN-KL"],
    "bn": ["IN-WB"], "gu": ["IN-GJ"], "pa": ["IN-PB"], "or": ["IN-OD"], "as": ["IN-AS"],
    "hi": ["IN-UP", "IN-BR", "IN-MP", "IN-RJ", "IN-DL", "IN-HR", "IN-JH", "IN-CG", "IN-UK", "IN-HP"],
}
STATE_NAMES = {
    "IN-TN": "Tamil Nadu", "IN-AP": "Andhra Pradesh", "IN-TG": "Telangana", "IN-MH": "Maharashtra",
    "IN-KA": "Karnataka", "IN-KL": "Kerala", "IN-WB": "West Bengal", "IN-GJ": "Gujarat", "IN-PB": "Punjab",
    "IN-OD": "Odisha", "IN-AS": "Assam", "IN-UP": "Uttar Pradesh", "IN-BR": "Bihar",
    "IN-MP": "Madhya Pradesh", "IN-RJ": "Rajasthan", "IN-DL": "Delhi", "IN-HR": "Haryana",
    "IN-JH": "Jharkhand", "IN-CG": "Chhattisgarh", "IN-UK": "Uttarakhand", "IN-HP": "Himachal Pradesh",
}


def state_langs(code: str) -> list[str]:
    """Wikipedia languages whose spikes are listed under this state."""
    return [lang for lang, codes in LANG_STATES.items() if code in codes]


ISO_REGION = re.compile(r"^[A-Z]{2}-[A-Z0-9]{1,3}$")


def is_iso_region(code: str | None) -> bool:
    """True for ISO 3166-2 codes like 'IN-RJ'; Wikidata also holds a few other formats ('IN.AP.EG')."""
    return bool(code and ISO_REGION.match(code))


def _walk(frontier: list[str], get: Callable[[list[str]], dict[str, dict]]) -> dict | None:
    """Follow P131 up from these items until one has an ISO 3166-2 code (at most MAX_HOPS)."""
    seen = set()
    for _ in range(MAX_HOPS):
        if not frontier:
            return None
        recs = get([q for q in frontier if q not in seen])
        seen.update(frontier)
        hit = next((recs[q] for q in frontier if q in recs and is_iso_region(recs[q].get("iso3166_2"))), None)
        if hit:
            return hit
        frontier = [p for q in frontier if q in recs for p in (recs[q].get("located_in") or [])]
    return None


def resolve(item: dict, get: Callable[[list[str]], dict[str, dict]], city_classes: set[str]) -> dict:
    out = {"region_code": None, "region_name": None, "city_qid": None, "country": None}

    starts = list(item.get("located_in") or []) + list(item.get("location") or []) + list(item.get("headquarters") or [])
    if is_iso_region(item.get("iso3166_2")):         # the item itself is a state / region
        out["region_code"], out["region_name"] = item["iso3166_2"], item.get("label")
    else:
        if starts:
            first = get(starts)
            for q in starts:
                if q in first and set(first[q].get("instance_of") or []) & city_classes:
                    out["city_qid"] = q
                    break
        for begin in (starts, list(item.get("electoral_district") or [])):
            hit = _walk(begin, get)
            if hit:
                out["region_code"], out["region_name"] = hit["iso3166_2"], hit.get("label")
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
