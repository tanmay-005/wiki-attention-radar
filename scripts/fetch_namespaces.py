"""Write mapreduce/namespaces_extra.py: namespace names and the main page of every configured
wiki, read from that wiki's own siteinfo API.

MR1 drops non-article pages (Special:, Category:, ...) by their prefix. The names for en, hi,
ta, ja and pt are written into common_title.py by hand (step 1.4); for the other languages
they are fetched here, so a Telugu "Category:" page is dropped like an English one.

Run inside edge (needs internet), once after changing `languages` in settings.yaml:
  python3 scripts/fetch_namespaces.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from spark.common.config import load  # noqa: E402

OUT = ROOT / "mapreduce" / "namespaces_extra.py"


def siteinfo(lang: str, ua: str) -> tuple[set[str], set[str]]:
    r = requests.get(f"https://{lang}.wikipedia.org/w/api.php",
                     params={"action": "query", "meta": "siteinfo", "format": "json", "formatversion": "2",
                             "siprop": "general|namespaces|namespacealiases"},
                     headers={"User-Agent": ua}, timeout=60)
    r.raise_for_status()
    q = r.json()["query"]
    names = set()
    for ns in q["namespaces"].values():
        if ns["id"] != 0:
            for n in (ns.get("name"), ns.get("canonical")):
                if n:
                    names.add(n.replace("_", " ").strip().casefold())
    for a in q.get("namespacealiases", []):
        if a["id"] != 0:
            names.add(a["alias"].replace("_", " ").strip().casefold())
    main = q["general"]["mainpage"].replace(" ", "_")
    return names, {main}


def main() -> None:
    cfg = load()
    ua = cfg["wikimedia"]["user_agent"]
    local, mains = {}, {}
    for lang in cfg["languages"]:
        names, main = siteinfo(lang, ua)
        local[lang], mains[lang] = sorted(names), sorted(main)
        print(f"  {lang}: {len(names)} namespace names, main page {next(iter(main))}")
    lines = ['"""Written by scripts/fetch_namespaces.py from each wiki\'s siteinfo. Do not edit by hand."""', "",
             "EXTRA_LOCAL = {"]
    lines += [f"    {lang!r}: {set(v)!r}," for lang, v in local.items()]
    lines += ["}", "", "EXTRA_MAIN = {"]
    lines += [f"    {lang!r}: {set(v)!r}," for lang, v in mains.items()]
    lines += ["}", ""]
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
