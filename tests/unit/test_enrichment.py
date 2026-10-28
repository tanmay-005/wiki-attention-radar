"""T-U5 (categorise) and T-U6 (resolve region) on hand-made Wikidata records."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from enrichment.categorise import categorise, load_rules  # noqa: E402
from enrichment.regions import resolve  # noqa: E402

ITEMS = json.loads((ROOT / "tests/fixtures/wikidata/items.json").read_text(encoding="utf-8"))
RULES = load_rules()
CITY = set(RULES["city_classes"])


def get(qids):
    return {q: ITEMS[q] for q in qids if q in ITEMS}


def cat(q):
    return categorise(ITEMS[q], RULES, get)


def test_categories():
    assert cat("Q213854") == ("Sports", "P106")                 # cricketer
    assert cat("Q900000003") == ("Politics", "P106")            # politician (before scientist)
    assert cat("Q900000004") == ("Disasters", "P31")            # stratovolcano
    assert cat("Q900000005") == ("Entertainment", "P31")        # film
    assert cat("Q900000007") == ("Politics", "P31")             # political party
    assert cat("Q900000001") == ("Politics", "P31>P279")        # general election -> election
    assert cat("Q900000002") == ("Politics", "P31>P279")        # constituency -> electoral unit
    assert cat("Q900000006") == ("Science & Tech", "P31>P279")  # chatbot -> software
    assert cat("Q900000009") == ("Sports", "P31>P279x3")       # CL final -> football final -> final -> sports competition
    assert cat("Q900000010") == ("Sports", "P106>P279")        # baseball player -> athlete
    assert cat("Q900000008") == ("Other", "default")


def test_rules_file_is_consistent():
    seen = {}
    for r in RULES["rules"]:
        for kind in ("p31", "p106"):
            for q in (r.get(kind) or {}):
                assert q.startswith("Q"), q
                assert q not in seen, f"{q} is listed under both {seen[q]} and {r['category']}"
                seen[q] = r["category"]


def test_region_through_district():
    r = resolve(ITEMS["Q900000002"], get, CITY)                 # constituency -> district -> UP
    assert (r["region_code"], r["region_name"], r["country"]) == ("IN-UP", "Uttar Pradesh", "IN")


def test_region_from_headquarters_and_city():
    r = resolve(ITEMS["Q900000007"], get, CITY)                 # party HQ in Lucknow
    assert r["region_code"] == "IN-UP" and r["city_qid"] == "Q900000104"


def test_person_country_from_citizenship_no_region():
    r = resolve(ITEMS["Q213854"], get, CITY)
    assert r == {"region_code": None, "region_name": None, "city_qid": None, "country": "IN"}


def test_item_that_is_a_region_and_a_foreign_volcano():
    assert resolve(ITEMS["Q1498"], get, CITY)["region_code"] == "IN-UP"
    r = resolve(ITEMS["Q900000004"], get, CITY)
    assert (r["region_code"], r["country"]) == ("PH-NEC", "PH")


def test_politician_region_from_seat_but_not_from_birthplace():
    born = {**ITEMS["Q213854"], "birthplace": ["Q900000104"]}                # born in Lucknow: not used
    assert resolve(born, get, CITY)["region_code"] is None
    mla = {**ITEMS["Q213854"], "electoral_district": ["Q900000002"]}        # holds a seat in UP
    r = resolve(mla, get, CITY)
    assert (r["region_code"], r["country"], r["city_qid"]) == ("IN-UP", "IN", None)
