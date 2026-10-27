"""Wikidata client with a MongoDB cache (step 4.6).

Two lookups, both batched 50 at a time (the API limit):
  titles_to_qids(lang, titles)  Wikipedia title -> QID, via that Wikipedia's own API
                                (prop=pageprops), which also follows redirects
  entities(qids)                QID -> a slim record of the claims this project uses

Every entity fetched is stored in Mongo radar.wikidata_cache (_id = QID), so a re-run
only asks Wikidata about things it has not seen. Title lookups are cached in the
enrichment results (radar.pages), see spark/jobs/s4_enrich.py.

Etiquette (https://meta.wikimedia.org/wiki/User-Agent_policy): a User-Agent with a
contact address from settings.yaml, at most one request per second, and back-off on
HTTP 429/5xx honouring Retry-After.
"""
from __future__ import annotations

import time
from typing import Iterable

import requests

API = "https://www.wikidata.org/w/api.php"
BATCH = 50

# claims kept in the slim record; values are QIDs unless noted
ITEM_PROPS = {
    "P31": "instance_of", "P279": "subclass_of", "P106": "occupation",
    "P17": "country", "P27": "citizenship", "P131": "located_in", "P276": "location",
    "P159": "headquarters",
}
STRING_PROPS = {"P300": "iso3166_2", "P297": "iso3166_1"}
# properties whose targets count as "linked" items for event grouping
LINK_PROPS = ("P361", "P1344", "P710", "P1923", "P793", "P102", "P54", "P1441", "P179", "P1001", "P276")


def slim(entity: dict) -> dict:
    """The parts of a Wikidata entity JSON this project uses."""
    claims = entity.get("claims", {}) or {}

    def ids(prop):
        out = []
        for c in claims.get(prop, []):
            v = (c.get("mainsnak", {}).get("datavalue") or {}).get("value")
            if isinstance(v, dict) and v.get("id"):
                out.append(v["id"])
        return out

    def first_string(prop):
        for c in claims.get(prop, []):
            v = (c.get("mainsnak", {}).get("datavalue") or {}).get("value")
            if isinstance(v, str):
                return v
        return None

    rec = {"_id": entity["id"], "label": (entity.get("labels", {}).get("en") or {}).get("value")}
    for p, name in ITEM_PROPS.items():
        rec[name] = ids(p)
    for p, name in STRING_PROPS.items():
        rec[name] = first_string(p)
    links = []
    for p in LINK_PROPS:
        links.extend(ids(p))
    rec["links"] = sorted(set(links))
    return rec


class WikidataClient:
    def __init__(self, user_agent: str, cache=None, min_interval: float = 1.0, retries: int = 5):
        self.http = requests.Session()
        self.http.headers["User-Agent"] = user_agent
        self.cache = cache            # a pymongo collection, or None for no cache
        self.min_interval = min_interval
        self.retries = retries
        self._last = 0.0
        self.calls = 0
        self.mem: dict[str, dict] = {}

    # ---- HTTP -------------------------------------------------------------------
    def _get(self, params: dict, url: str = API) -> dict:
        params = {**params, "format": "json", "formatversion": "2"}
        last = ""
        for attempt in range(self.retries):
            wait = self.min_interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()
            self.calls += 1
            try:
                if "titles" in params:  # long title lists (Hindi, Tamil) do not fit in a URL
                    r = self.http.post(url, data=params, timeout=60)
                else:
                    r = self.http.get(url, params=params, timeout=60)
            except requests.RequestException as e:
                time.sleep(2 ** attempt)
                last = str(e)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(float(r.headers.get("Retry-After", 2 ** (attempt + 1))))
                last = f"HTTP {r.status_code}"
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Wikidata request failed after {self.retries} attempts ({last})")

    # ---- entities -----------------------------------------------------------------
    def _remember(self, records: Iterable[dict]) -> None:
        records = list(records)
        for rec in records:
            self.mem[rec["_id"]] = rec
        if self.cache is not None and records:
            from pymongo import ReplaceOne
            self.cache.bulk_write([ReplaceOne({"_id": r["_id"]}, r, upsert=True) for r in records])

    def entities(self, qids: Iterable[str]) -> dict[str, dict]:
        """Slim records for these QIDs: memory, then Mongo, then Wikidata."""
        want = [q for q in dict.fromkeys(qids) if q and q.startswith("Q")]
        missing = [q for q in want if q not in self.mem]
        if missing and self.cache is not None:
            for rec in self.cache.find({"_id": {"$in": missing}}):
                self.mem[rec["_id"]] = rec
            missing = [q for q in missing if q not in self.mem]
        for i in range(0, len(missing), BATCH):
            chunk = missing[i:i + BATCH]
            data = self._get({"action": "wbgetentities", "ids": "|".join(chunk),
                              "props": "labels|claims", "languages": "en"})
            got = [slim(e) for e in (data.get("entities") or {}).values() if "missing" not in e]
            self._remember(got)
            for q in chunk:                          # remember deleted items so we do not ask again
                self.mem.setdefault(q, {"_id": q, "label": None, "missing": True,
                                        **{n: [] for n in ITEM_PROPS.values()},
                                        **{n: None for n in STRING_PROPS.values()}, "links": []})
        return {q: self.mem[q] for q in want if q in self.mem}

    def titles_to_qids(self, lang: str, titles: Iterable[str]) -> dict[str, str | None]:
        """Wikipedia titles -> QID (None when the page has no Wikidata item or does not exist).
        Redirects are followed, so an old title still finds the item of the page it points to."""
        titles = list(dict.fromkeys(t for t in titles if t))
        out: dict[str, str | None] = {t: None for t in titles}
        url = f"https://{lang}.wikipedia.org/w/api.php"
        for i in range(0, len(titles), BATCH):
            chunk = titles[i:i + BATCH]
            q = self._get({"action": "query", "prop": "pageprops", "ppprop": "wikibase_item",
                           "redirects": "1", "titles": "|".join(chunk)}, url=url).get("query", {})
            step = {n["from"]: n["to"] for n in q.get("normalized", [])}
            step.update({r["from"]: r["to"] for r in q.get("redirects", [])})
            qid_of = {p["title"]: (p.get("pageprops") or {}).get("wikibase_item")
                      for p in q.get("pages", []) if not p.get("missing")}
            for t in chunk:
                cur, hops = t, 0
                while cur in step and hops < 5:
                    cur, hops = step[cur], hops + 1
                out[t] = qid_of.get(cur)
        return out
