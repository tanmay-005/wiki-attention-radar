"""T-U7 (grouping), T-U8 (score), T-U9 (scopes)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.common.events import build_event, event_score, group_events, n_items, scope_keys  # noqa: E402

T0 = dt.datetime(2024, 6, 4, 6)


def node(qid, hours=0, kind="global", links=(), **kw):
    base = {"kind": kind, "qid": qid, "peak": T0 + dt.timedelta(hours=hours), "links": list(links),
            "lang": "en", "page_title": qid, "start": T0 + dt.timedelta(hours=hours - 1),
            "peak_views": 1000, "baseline": 100, "ratio": 10.0, "suspicious": False}
    base.update(kw)
    return base


def groups(nodes, **kw):
    return sorted(sorted(g) for g in group_events(nodes, window_hours=6, same_entity_hours=24, **kw))


def test_same_qid_merges_within_24h():
    assert groups([node("Q1", 0), node("Q1", 20, lang="hi"), node("Q1", 60)]) == [[0, 1], [2]]


def test_linked_items_merge_within_6h_only():
    assert groups([node("Q1", 0, links=["Q2"]), node("Q2", 4)]) == [[0, 1]]
    assert groups([node("Q1", 0, links=["Q2"]), node("Q2", 48)]) == [[0], [1]]


def test_generic_links_do_not_merge():
    nodes = [node("Q1", 0, links=["Q5", "Q668"]), node("Q5", 1), node("Q668", 1)]
    assert groups(nodes, generic={"Q668"}) == [[0], [1], [2]]


def test_hub_targets_are_ignored():
    nodes = [node(f"Q{i}", 0, links=["Q999"]) for i in range(1, 31)] + [node("Q999", 0)]
    assert len(groups(nodes, hub_limit=25)) == 31          # 30 items all linking to one hub stay apart


def test_country_spike_joins_global_same_item_next_day():
    g = groups([node("Q1", 0), node("Q1", 20, kind="country", reader_country="IN")])
    assert g == [[0, 1]]


def test_country_spikes_do_not_merge_through_links():
    nodes = [node("Q1", 0, kind="country", links=["Q2"], reader_country="IN"),
             node("Q2", 0, kind="country", reader_country="IN")]
    assert groups(nodes) == [[0], [1]]


def test_oversized_event_is_split():
    chain = [node(f"Q10{i}", 0, links=[f"Q10{i + 1}"]) for i in range(1, 6)] + [node("Q106", 0), node("Q101", 2, lang="hi")]
    assert len(groups(chain)) == 1                              # Q101 - Q102 - ... - Q106 chained by links
    stats = {}
    parts = sorted(sorted(g) for g in group_events(chain, 6, 24, max_members=4, stats=stats))
    assert stats["split"] == 1 and parts == [[0, 6], [1], [2], [3], [4], [5]]
    many = [node("Q101", 0, kind="country", reader_country=f"C{i}") for i in range(10)] + [node("Q101", 1)]
    assert len(group_events(many, 6, 24, max_members=4)) == 1  # one item in 10 countries is one story


def test_n_items():
    assert n_items([node("Q1"), node("Q1", kind="country"), node(None), node(None)], [0, 1, 2, 3]) == 3


def test_score_monotonic_and_capped():
    assert event_score(10, 10000, 100) > event_score(5, 10000, 100)
    assert event_score(10, 100000, 100) > event_score(10, 10000, 100)
    assert event_score(500, 10000, 100) == event_score(100, 10000, 100)


def test_scope_keys():
    assert scope_keys(True, ["IN"], "IN", "IN-RJ") == ["G", "R:IN", "A:IN", "A:IN:IN-RJ"]
    assert scope_keys(False, ["US", "IN", "IN"], None, None) == ["R:IN", "R:US"]


def test_build_event_lead_category_location():
    members = [
        node("Q1", 0, peak_views=50000, category="Politics", country="IN", region_code="IN-UP", label="Election"),
        node("Q2", 1, lang="hi", peak_views=4000, category="Politics"),
        node("Q1", 10, kind="country", reader_country="IN", peak_views=90000, category="Politics"),
    ]
    e = build_event(members)
    assert e["event_id"] == "evt_20240604_Q1" and e["title"] == "Election"
    assert e["category"] == "Politics" and e["loc_region"] == "IN-UP"
    hindi = ["IN-BR", "IN-CG", "IN-DL", "IN-HP", "IN-HR", "IN-JH", "IN-MP", "IN-RJ", "IN-UK", "IN-UP"]
    assert e["scopes"] == ["G", "R:IN"] + [f"R:IN:{s}" for s in hindi] + ["A:IN", "A:IN:IN-UP"]
    assert {s["lang"] for s in e["states"]} == {"hi"} and e["states"][0]["peak_views"] == 4000
    assert [a["role"] for a in e["articles"]] == ["lead", "member"] and e["langs"] == ["en", "hi"]
    assert e["countries"][0]["code"] == "IN" and not e["suspicious"]


def test_state_readers_from_language_spikes():
    e = build_event([node("Q7", 0, peak_views=9000), node("Q7", 1, lang="te", peak_views=300, ratio=12.0),
                     node("Q7", 2, lang="ta", peak_views=50, suspicious=True)])     # bot-like: not listed
    assert [(s["code"], s["lang"], s["ratio"]) for s in e["states"]] == [("IN-AP", "te", 12.0), ("IN-TG", "te", 12.0)]
    assert "R:IN:IN-AP" in e["scopes"] and "R:IN:IN-TN" not in e["scopes"]
    assert scope_keys(True, [], None, None, ["IN-TN"]) == ["G", "R:IN:IN-TN"]
