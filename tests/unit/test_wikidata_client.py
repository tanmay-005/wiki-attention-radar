"""Step 4.6: the client's parsing, batching and caching, with the network replaced by a fake."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from enrichment.wikidata_client import WikidataClient, slim  # noqa: E402

ENTITY = {
    "id": "Q213854", "labels": {"en": {"value": "Virat Kohli"}},
    "claims": {
        "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
        "P106": [{"mainsnak": {"datavalue": {"value": {"id": "Q12299841"}}}}],
        "P27": [{"mainsnak": {"datavalue": {"value": {"id": "Q668"}}}}],
        "P54": [{"mainsnak": {"datavalue": {"value": {"id": "Q1156897"}}}}],
        "P300": [{"mainsnak": {"datavalue": {"value": "IN-XX"}}}],
        "P999": [{"mainsnak": {"snaktype": "novalue"}}],
    },
}


def test_slim_keeps_only_what_we_use():
    s = slim(ENTITY)
    assert s["_id"] == "Q213854" and s["label"] == "Virat Kohli"
    assert s["instance_of"] == ["Q5"] and s["occupation"] == ["Q12299841"] and s["citizenship"] == ["Q668"]
    assert s["iso3166_2"] == "IN-XX" and s["links"] == ["Q1156897"]


class FakeClient(WikidataClient):
    def __init__(self, responses):
        super().__init__("test-agent", cache=None, min_interval=0)
        self.responses, self.requests = responses, []

    def _get(self, params, url=None):
        self.requests.append(params)
        return self.responses.pop(0)


def test_titles_follow_normalisation_and_redirects():
    c = FakeClient([{"query": {
        "normalized": [{"from": "virat Kohli", "to": "Virat Kohli"}],
        "redirects": [{"from": "Kohli", "to": "Virat Kohli"}],
        "pages": [{"title": "Virat Kohli", "pageprops": {"wikibase_item": "Q213854"}},
                  {"title": "No Such Page", "missing": True}]}}])
    got = c.titles_to_qids("en", ["virat Kohli", "Kohli", "No Such Page"])
    assert got == {"virat Kohli": "Q213854", "Kohli": "Q213854", "No Such Page": None}


def test_entities_batches_of_50_and_memory_cache():
    qids = [f"Q{i}" for i in range(1, 121)]
    batches = [{"entities": {q: {"id": q, "labels": {}, "claims": {}} for q in qids[i:i + 50]}} for i in (0, 50, 100)]
    c = FakeClient(batches)
    got = c.entities(qids)
    assert len(got) == 120 and len(c.requests) == 3
    assert [len(r["ids"].split("|")) for r in c.requests] == [50, 50, 20]
    c.entities(qids[:10])                        # second time: no new request
    assert len(c.requests) == 3


def test_old_cached_records_are_fetched_again_when_a_field_is_needed():
    c = FakeClient([{"entities": {"Q1": {"id": "Q1", "labels": {}, "claims": {
        "P768": [{"mainsnak": {"datavalue": {"value": {"id": "Q1156"}}}}]}}}}])
    c.mem["Q1"] = {"_id": "Q1", "label": None, "located_in": []}      # written before P768 was kept
    assert c.entities(["Q1"])["Q1"].get("electoral_district") is None and not c.requests
    assert c.entities(["Q1"], need=("electoral_district",))["Q1"]["electoral_district"] == ["Q1156"]
    assert len(c.requests) == 1
