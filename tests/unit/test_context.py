"""T-U11: parsing Wikipedia's daily news and deaths pages, and matching them to events."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from enrichment.context import (about_from_page, deaths_title, links_in, match_news,  # noqa: E402
                                parse_current_events, parse_deaths, plain, portal_title)

D = dt.date(2026, 10, 13)

PORTAL = """{{Current events|year=2026|month=10|day=13|content=
<!-- All news items below this line -->
'''Armed conflicts and attacks'''
* [[Russian invasion of Ukraine]]
** [[Russia]] launches a drone attack on [[Kyiv]], killing four people. ([https://example.com/a Reuters])
'''Politics and elections'''
* [[2026 Exampleland general election]]
** Voters in [[Exampleland]] go to the polls; [[Jane Doe (politician)|Jane Doe]] leads early counts. ([https://example.com/b BBC News])
'''Sports'''
*[[Carlos Alcaraz]] defeats [[Jannik Sinner]] in the final of the [[2026 Example Open]]. ([https://example.com/c ESPN])
* Short.
<!-- All news items above this line -->}}"""

DEATHS = """==October 2026==
===13===
*[[Ann Example]], 88, British actress (''[[Some Film]]''), [[heart failure]].<ref>{{cite news|title=x}}</ref>
*[[Bob Sample|Robert Sample]], 70, Indian cricketer.<ref name="a" />
===14===
*[[Carol Test]], 64, American singer.
"""


def test_titles_and_plain_text():
    assert portal_title(D) == "Portal:Current events/2026 October 13"
    assert deaths_title(2026, 10) == "Deaths in October 2026"
    assert links_in("[[a_b|x]] and [[File:y.jpg]] and [[A b#Sec]]") == ["A b"]
    assert plain("[[Jane Doe (politician)|Jane Doe]] wins. ([https://x.org BBC])") == "Jane Doe wins."
    assert plain("'''Bold''' text<ref>cite</ref> {{tpl|x}}") == "Bold text"


def test_current_events_items_keep_topic_and_links():
    items = parse_current_events(PORTAL, D)
    texts = [i["text"] for i in items]
    assert len(items) == 3                                # the two topic bullets and "Short." are not items
    assert texts[0] == "Russia launches a drone attack on Kyiv, killing four people."
    war = items[0]
    assert war["section"] == "Armed conflicts and attacks" and war["topic"] == "Russian invasion of Ukraine"
    assert war["links"] == ["Russia", "Kyiv"] and war["topic_links"] == ["Russian invasion of Ukraine"]
    assert items[1]["links"] == ["Exampleland", "Jane Doe (politician)"]
    assert items[2]["topic"] is None and items[2]["links"][0] == "Carlos Alcaraz"


def test_deaths():
    rows = parse_deaths(DEATHS, 2026, 10)
    assert [(r["date"].day, r["person"]) for r in rows] == [(13, "Ann Example"), (13, "Bob Sample"), (14, "Carol Test")]
    assert rows[0]["text"] == "Ann Example, 88, British actress (Some Film), heart failure."
    assert rows[1]["text"] == "Robert Sample, 70, Indian cricketer."


YEARLY = """{{short description|none}}
==January==
===13===
*[[Wrong Person]], 50, January death.
==October==
===[[October 13]]===
*[[Ann Example]], 88, British actress.
=== 14 ===
*[[Carol Test]], 64, American singer.
==November==
===13===
*[[Later Person]], 70, November death.
"""


def test_deaths_on_a_yearly_page():
    rows = parse_deaths(YEARLY, 2026, 10)
    assert [(r["date"].day, r["person"]) for r in rows] == [(13, "Ann Example"), (14, "Carol Test")]


def test_matching_by_wikidata_item_and_date():
    items = parse_current_events(PORTAL, D)
    qid_of = {"Carlos Alcaraz": "Q1", "Jannik Sinner": "Q2", "Russian invasion of Ukraine": "Q3",
              "Russia": "Q4", "Kyiv": "Q5", "2026 Example Open": "Q6"}
    got = match_news({"Q1"}, D, D, items, qid_of)
    assert [g["text"][:15] for g in got] == ["Carlos Alcaraz "]
    assert match_news({"Q1"}, D + dt.timedelta(days=3), D + dt.timedelta(days=3), items, qid_of) == []
    war = match_news({"Q3"}, D, D, items, qid_of)          # only the topic links it: still found
    assert len(war) == 1 and war[0]["topic"] == "Russian invasion of Ukraine"
    both = match_news({"Q3", "Q5"}, D, D, items, qid_of)
    assert len(both) == 1                                  # the same line is not repeated


def test_about_block():
    page = {"title": "Carlos Alcaraz", "extract": "Carlos Alcaraz is a Spanish tennis player. ",
            "pageprops": {"wikibase-shortdesc": "Spanish tennis player", "wikibase_item": "Q85518537"},
            "thumbnail": {"source": "https://upload.wikimedia.org/x.jpg"}}
    a = about_from_page(page, "en")
    assert a == {"lang": "en", "title": "Carlos Alcaraz", "description": "Spanish tennis player", "qid": "Q85518537",
                 "extract": "Carlos Alcaraz is a Spanish tennis player.", "thumbnail": "https://upload.wikimedia.org/x.jpg"}
