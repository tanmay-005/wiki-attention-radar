#!/usr/bin/env python3
"""Step 4.7: check every QID in category_rules.yaml against Wikidata.

Prints the label Wikidata has next to the label the rules file expects, and marks
differences. A difference usually means a wrong QID; fix the file and run again.
  docker compose exec edge python3 enrichment/verify_rules.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from enrichment.categorise import load_rules  # noqa: E402
from enrichment.wikidata_client import WikidataClient  # noqa: E402
from spark.common.config import load  # noqa: E402

rules = load_rules()
expected = {}
for r in rules["rules"]:
    for kind in ("p31", "p106"):
        for q, label in (r.get(kind) or {}).items():
            expected[q] = (r["category"], kind, label)
for q, label in (rules.get("city_classes") or {}).items():
    expected[q] = ("(city)", "class", label)

client = WikidataClient(load()["wikimedia"]["user_agent"])
got = client.entities(list(expected))
bad = 0
for q, (cat, kind, want) in expected.items():
    have = (got.get(q) or {}).get("label")
    ok = have is not None and have.lower() == want.lower()
    bad += not ok
    print(f"{'ok ' if ok else '?? '} {q:<11} {cat:<15} {kind:<5} expected '{want}'  wikidata '{have}'")
print(f"\n{len(expected)} QIDs checked, {bad} to look at ({client.calls} API calls)")
