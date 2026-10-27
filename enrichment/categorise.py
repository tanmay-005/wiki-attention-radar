"""Category rules (step 4.7): Wikidata classes and occupations -> one dashboard category."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import yaml

RULES_FILE = Path(__file__).with_name("category_rules.yaml")


def load_rules(path: Path = RULES_FILE) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def categorise(item: dict, rules: dict, get: Callable[[list[str]], dict[str, dict]] | None = None,
               max_depth: int = 3) -> tuple[str, str]:
    """(category, source) for one slim item.

    source says why: 'P31', 'P106', 'P31>P279' / 'P106>P279' (one level up the class tree),
    'P31>P279x2', 'P106>P279x3' and so on (two or three levels up), or 'default'.
    `get` fetches slim records for QIDs; it is only needed for the P279 fallback.
    Examples: 2019 Indian general election -> Lok Sabha election -> general election -> public election;
    an occupation 'baseball player' -> athlete.
    """
    p31 = set(item.get("instance_of") or [])
    p106 = set(item.get("occupation") or [])
    for rule in rules["rules"]:
        if p31 & set(rule.get("p31") or {}):
            return rule["category"], "P31"
        if p106 & set(rule.get("p106") or {}):
            return rule["category"], "P106"
    if get:
        # walk up the class tree (P279) a few levels, for the item's classes (P31) and for
        # its occupations (P106); the nearest matching level wins
        trees = {"P31": set(p31), "P106": set(p106)}
        seen = {k: set(v) for k, v in trees.items()}
        for depth in range(1, max_depth + 1):
            for prop, level in trees.items():
                parents = set()
                for cls in (get(sorted(level)).values() if level else []):
                    parents.update(cls.get("subclass_of") or [])
                parents -= seen[prop]
                for rule in rules["rules"]:
                    if parents & set(rule.get("p31") or {}) or parents & set(rule.get("p106") or {}):
                        return rule["category"], f"{prop}>P279" if depth == 1 else f"{prop}>P279x{depth}"
                seen[prop] |= parents
                trees[prop] = parents
    return rules.get("default", "Other"), "default"
