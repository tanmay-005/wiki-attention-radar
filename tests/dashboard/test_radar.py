"""T-UI: the five core scenarios and the empty states of docs/04_UI_UX_DESIGN.md section 4,
run against a fake data layer with Streamlit's AppTest."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

HARNESS = str(Path(__file__).with_name("radar_harness.py"))


def run(**state):
    at = AppTest.from_file(HARNESS, default_timeout=30)
    for k, v in state.items():
        at.session_state[k] = v
    at.run()
    assert not at.exception, at.exception
    return at


def text(at):
    parts = [m.value for m in at.markdown] + [c.value for c in at.caption] + [i.value for i in at.info]
    return "\n".join(str(p) for p in parts)


def titles(at):
    """Story titles in table order."""
    return [r["Story"] for r in at.dataframe[0].value.to_dict("records")] if at.dataframe else []


import datetime as dt  # noqa: E402

D25 = dt.date(2026, 10, 25)


def test_1_opens_on_top_global_stories():
    at = run(as_of=D25, window="24 hours")
    assert "Top stories worldwide" in text(at)
    assert titles(at) == ["Sample Player"]
    assert "Sample Player wins the Example Open final" in text(at)        # the news line is shown


def test_1b_window_ranking_and_cards():
    at = run(window="7 days")
    t = titles(at)
    assert t[:2] == ["Sample Player", "2026 Exampleland general election"]
    assert len(t) == 11
    assert t[2] == "⚠ Bot page"
    at = run(window="30 days")                                 # clipped to the detection start
    assert "(detection starts 13 Oct)" in text(at)


def test_2_country_india_means_readers_in_india():
    at = run(window="30 days", country="IN")
    assert "read by people in India" in text(at)
    assert "Sample Player" in titles(at) and "⚠ Bot page" not in titles(at)


def test_3_global_politics():
    at = run(window="30 days", category="Politics")
    assert titles(at) == ["2026 Exampleland general election", "Jodhpur Example constituency"]
    assert "Top Politics stories worldwide" in text(at)


def test_4_india_politics():
    at = run(window="30 days", country="IN", category="Politics")
    assert "Top Politics stories read by people in India" in text(at)
    assert "Sample Player" not in titles(at)


def test_5_events_about_rajasthan():
    at = run(window="30 days", country="IN", mode="Events about")
    at.selectbox(key="region").set_value("IN-RJ").run()
    assert at.session_state["mode"] == "Events about"
    assert titles(at) == ["Jodhpur Example constituency"]
    assert "about Rajasthan" in text(at)
    assert "Constituency in Rajasthan, India" in text(at)            # what the story is about


def test_5b_readers_in_tamil_nadu_uses_tamil_wikipedia():
    at = run(window="30 days", country="IN")
    at.selectbox(key="region").set_value("IN-TN").run()
    assert at.session_state["mode"] == "Readers in"
    assert titles(at) == ["Example Tamil film"]
    assert "read on Tamil Wikipedia (Tamil Nadu)" in text(at)
    assert "Spikes on Tamil Wikipedia" in text(at)


def test_switching_mode_keeps_the_state():
    at = run(window="30 days", country="IN", mode="Events about")
    at.selectbox(key="region").set_value("IN-RJ").run()
    at.segmented_control(key="mode").set_value("Readers in").run()
    assert at.session_state["region"] == "IN-RJ"                     # Rajasthan: data in both modes
    assert "read on Hindi Wikipedia (Rajasthan)" in text(at)
    at.selectbox(key="region").set_value("IN-TN").run()
    at.segmented_control(key="mode").set_value("Events about").run()
    assert at.session_state["region"] == "IN-TN"                     # kept, even with nothing to show
    assert "No stories located in **Tamil Nadu**" in text(at)
    at.segmented_control(key="mode").set_value("Readers in").run()
    assert at.session_state["region"] == "IN-TN" and titles(at) == ["Example Tamil film"]


def test_changing_country_resets_region_and_mode():
    at = run(window="30 days", country="IN", mode="Events about")
    at.selectbox(key="region").set_value("IN-RJ").run()
    at.selectbox(key="country").set_value("US").run()
    assert at.session_state["region"] == "" and at.session_state["mode"] == "Readers in"


def test_empty_states():
    at = run(window="24 hours", country="IN", category="Disasters")
    assert "No spikes match these filters" in text(at)
    at = run(window="30 days", country="FR")
    assert "does not publish per-country data" in text(at)


def test_language_filter_and_buttons():
    at = run(window="30 days", lang="ja")
    assert titles(at) == ["Sample Player"]
    assert "Japanese Wikipedia" in text(at)
    labels = [b.label for b in at.button]
    assert "Reset filters" in labels and "Open full story" in labels


def test_full_story_dialog():
    at = run(window="30 days", open_event="evt_20261025_Q3")   # what a click on "Full story" sets
    assert "Why people were reading" in text(at)
    assert "Example tennis player (born 2003)" in text(at)
