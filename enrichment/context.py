"""What an event is about, and why it may have spiked.

About: for one article, Wikipedia's short description ("Spanish tennis player"), the first
sentences of the article and its thumbnail image, 20 articles per API call.

Why: Wikipedia's own daily news pages, matched to events through Wikidata items:
  Portal:Current events/2026 October 13   the day's news, one bullet per story, with links
  Deaths in October 2026                  notable deaths by day
A news line belongs to an event when one of the line's linked articles is the same
Wikidata item as one of the event's articles and the dates are close.

The parsers below work on wikitext and are tested on their own (tests/unit/test_context.py).
"""
from __future__ import annotations

import datetime as dt
import re
from typing import Iterable

EXTRACT_BATCH = 20                    # the API returns intro extracts for at most 20 pages per call

_LINK = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|([^\]]*))?\]\]")
_EXT_LINK = re.compile(r"\[(?:https?:)?//[^\s\]]+(?:\s+([^\]]*))?\]")
_REF = re.compile(r"<ref[^>/]*/>|<ref[^>]*>.*?</ref>", re.S)
_TEMPLATE = re.compile(r"\{\{[^{}]*\}\}")
_COMMENT = re.compile(r"<!--.*?-->", re.S)
_TAG = re.compile(r"<[^>]+>")
_BOLD = re.compile(r"'{2,}")
_SOURCE = re.compile(r"\s*\((?:[^()]*?\b(?:Reuters|AP|BBC|AFP|CNN|Al Jazeera|The Guardian|NPR|ESPN)\b[^()]*)\)\s*$")
_EMPTY_PARENS = re.compile(r"\(\s*\)")
_SKIP_LINK_PREFIXES = ("File:", "Image:", "Category:", "wikt:", "Wikipedia:", "Portal:", "Template:")

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]


def norm_title(t: str) -> str:
    t = t.strip().replace("_", " ")
    return t[:1].upper() + t[1:] if t else t


def links_in(wikitext: str) -> list[str]:
    """Article titles linked from a piece of wikitext, in order, without duplicates."""
    out = []
    for m in _LINK.finditer(wikitext):
        t = norm_title(m.group(1))
        if t and not t.startswith(_SKIP_LINK_PREFIXES) and t not in out:
            out.append(t)
    return out


def plain(wikitext: str) -> str:
    """Readable text from a line of wikitext: links become their label, sources, refs and markup go."""
    s = _COMMENT.sub("", wikitext)
    s = _REF.sub("", s)
    for _ in range(3):
        s = _TEMPLATE.sub("", s)
    s = _LINK.sub(lambda m: (m.group(2) if m.group(2) is not None else m.group(1)).strip(), s)
    s = _EXT_LINK.sub("", s)
    s = _TAG.sub("", s)
    s = _BOLD.sub("", s)
    s = _EMPTY_PARENS.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _SOURCE.sub("", s).strip()
    return s.lstrip("*: ").strip()


def portal_title(day: dt.date) -> str:
    return f"Portal:Current events/{day.year} {MONTHS[day.month - 1]} {day.day}"


def deaths_title(year: int, month: int) -> str:
    return f"Deaths in {MONTHS[month - 1]} {year}"


def parse_current_events(wikitext: str, day: dt.date) -> list[dict]:
    """News items of one Portal:Current events day page.

    A top-level bullet is usually a topic ("* [[2026 Bihar Legislative Assembly election]]") and
    the bullets under it are the news. Each news item keeps its topic, and the links of both."""
    items, section, parents = [], None, {}
    for raw in wikitext.splitlines():
        line = raw.strip()
        head = re.fullmatch(r"'''(.+?)'''", line) or re.fullmatch(r";\s*(.+)", line)
        if head:
            section, parents = plain(head.group(1)), {}
            continue
        m = re.match(r"(\*+)\s*(.*)", line)
        if not m:
            continue
        depth, body = len(m.group(1)), m.group(2)
        parents = {d: v for d, v in parents.items() if d < depth}
        parents[depth] = body
        children_follow = False   # decided by the next line; record every bullet, drop pure topics later
        topic = " – ".join(plain(parents[d]) for d in sorted(parents) if d < depth)
        topic_links = [t for d in sorted(parents) if d < depth for t in links_in(parents[d])]
        items.append({"date": day, "section": section, "depth": depth, "topic": topic or None,
                      "text": plain(body), "links": links_in(body), "topic_links": topic_links,
                      "_children": children_follow})
    # a bullet that has deeper bullets right under it is a topic heading, not a news item
    for i, it in enumerate(items):
        nxt = items[i + 1] if i + 1 < len(items) else None
        it["_children"] = bool(nxt and nxt["depth"] > it["depth"])
    out = []
    for it in items:
        if it["_children"] or len(it["text"]) < 25:
            continue
        it.pop("_children")
        out.append(it)
    return out


_LEVEL2 = re.compile(r"==(?!=)\s*(.*?)\s*(?<!=)==")
_DAY_HEAD = re.compile(r"=+\s*(?:\[\[)?(?:[A-Za-z]+\s+)?(\d{1,2})(?:\]\])?\s*=+")


def month_section(wikitext: str, month: int) -> str:
    """The part of a yearly 'Deaths in <Year>' page under its '== <Month> ==' heading.
    A monthly page (no such headings) is returned whole."""
    name = MONTHS[month - 1]
    lines, keep, found = wikitext.splitlines(), [], False
    for raw in lines:
        m = _LEVEL2.fullmatch(raw.strip())
        if m:
            title = plain(m.group(1))
            if found:
                break
            found = title.startswith(name)
            continue
        if found:
            keep.append(raw)
    return "\n".join(keep) if found else wikitext


def parse_deaths(wikitext: str, year: int, month: int) -> list[dict]:
    """Entries of a deaths page: day, person (first link) and the description. Works on a monthly
    'Deaths in October 2026' page and on a yearly 'Deaths in 2026' page with month sections."""
    out, day = [], None
    for raw in month_section(wikitext, month).splitlines():
        line = raw.strip()
        h = _DAY_HEAD.fullmatch(line)
        if h:
            day = int(h.group(1))
            continue
        if day is None or not line.startswith("*"):
            continue
        links = links_in(_REF.sub("", line))
        if not links:
            continue
        text = plain(line)
        try:
            date = dt.date(year, month, day)
        except ValueError:
            continue
        out.append({"date": date, "person": links[0], "text": text, "links": [links[0]]})
    return out


def match_news(event_qids: set[str], first: dt.date, peak: dt.date, items: list[dict],
               qid_of: dict[str, str | None], before: int = 1, after: int = 1, limit: int = 3) -> list[dict]:
    """News items about one of the event's items, dated from `before` days before the first
    detection to `after` days after the peak. Items whose own text links the item rank first."""
    found = []
    for it in items:
        if not (first - dt.timedelta(days=before) <= it["date"] <= peak + dt.timedelta(days=after)):
            continue
        own = {qid_of.get(t) for t in it["links"]} - {None}
        ctx = {qid_of.get(t) for t in it.get("topic_links", [])} - {None}
        if event_qids & own:
            rank = 0
        elif event_qids & ctx:
            rank = 1
        else:
            continue
        found.append((rank, abs((it["date"] - peak).days), it))
    found.sort(key=lambda x: (x[0], x[1]))
    out, seen = [], set()
    for _, _, it in found:
        key = it["text"]
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out[:limit]


def about_from_page(page: dict, lang: str) -> dict:
    """The 'about' block from one page of a prop=extracts|pageprops|pageimages query."""
    props = page.get("pageprops") or {}
    extract = (page.get("extract") or "").strip()
    return {"lang": lang, "title": page.get("title"), "description": props.get("wikibase-shortdesc"),
            "qid": props.get("wikibase_item"), "extract": extract or None,
            "thumbnail": (page.get("thumbnail") or {}).get("source")}


def fetch_about(client, lang: str, titles: Iterable[str]) -> dict[str, dict]:
    """title -> about block, following redirects; 20 titles per call."""
    titles = list(dict.fromkeys(t for t in titles if t))
    url = f"https://{lang}.wikipedia.org/w/api.php"
    out = {}
    for i in range(0, len(titles), EXTRACT_BATCH):
        chunk = titles[i:i + EXTRACT_BATCH]
        q = client._get({"action": "query", "prop": "extracts|pageprops|pageimages", "exintro": "1",
                         "explaintext": "1", "exsentences": "3", "exlimit": str(EXTRACT_BATCH),
                         "ppprop": "wikibase-shortdesc|wikibase_item", "piprop": "thumbnail",
                         "pithumbsize": "320", "pilimit": str(EXTRACT_BATCH), "redirects": "1",
                         "titles": "|".join(chunk)}, url=url).get("query", {})
        step = {n["from"]: n["to"] for n in q.get("normalized", [])}
        step.update({r["from"]: r["to"] for r in q.get("redirects", [])})
        pages = {p["title"]: p for p in q.get("pages", []) if not p.get("missing")}
        for t in chunk:
            cur, hops = t, 0
            while cur in step and hops < 5:
                cur, hops = step[cur], hops + 1
            if cur in pages:
                out[t] = about_from_page(pages[cur], lang)
    return out


def fetch_wikitext(client, titles: list[str], lang: str = "en") -> dict[str, str]:
    """Current wikitext of up to 50 pages per call."""
    """Redirects are followed (e.g. 'Deaths in October 2026' -> 'Deaths in 2026'); the text is
    returned under the title that was asked for."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    out = {}
    for i in range(0, len(titles), 50):
        chunk = titles[i:i + 50]
        q = client._get({"action": "query", "prop": "revisions", "rvprop": "content", "rvslots": "main",
                         "redirects": "1", "titles": "|".join(chunk)}, url=url).get("query", {})
        step = {n["from"]: n["to"] for n in q.get("normalized", [])}
        step.update({r["from"]: r["to"] for r in q.get("redirects", [])})
        text = {}
        for p in q.get("pages", []):
            revs = p.get("revisions") or []
            if revs:
                text[p["title"]] = revs[0].get("slots", {}).get("main", {}).get("content", "")
        for t in chunk:
            cur, hops = t, 0
            while cur in step and hops < 5:
                cur, hops = step[cur], hops + 1
            if cur in text:
                out[t] = text[cur]
    return out
