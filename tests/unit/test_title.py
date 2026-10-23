"""Unit tests for mapreduce/common_title.py (the rules Hive UDFs must match too)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "mapreduce"))
from common_title import is_content_page, normalize_title, parse_domain  # noqa: E402

LANGS = {"en", "hi", "ta", "ja", "pt"}


def test_parse_domain():
    assert parse_domain("en", LANGS) == ("en", "desktop")
    assert parse_domain("hi.m", LANGS) == ("hi", "mobile")
    for d in ["de", "en.wikt", "en.m.m", "commons.m", "", "en.b"]:
        assert parse_domain(d, LANGS) is None


def test_namespaces_dropped():
    assert not is_content_page("en", "Category:1990s_births")
    assert not is_content_page("en", "special:Search")          # case-insensitive
    assert not is_content_page("en", "User_talk:Someone")
    assert not is_content_page("hi", "श्रेणी:भारत")
    assert not is_content_page("ja", "特別:検索")
    assert not is_content_page("ja", "Category:日本")             # English name works everywhere
    assert not is_content_page("pt", "Predefinição:Info")
    assert not is_content_page("ta", "பகுப்பு:நாடுகள்")


def test_articles_with_colons_kept():
    assert is_content_page("en", "Furiosa:_A_Mad_Max_Saga")
    assert is_content_page("ja", "マッドマックス:フュリオサ")
    assert is_content_page("en", "Star_Wars:_Episode_IV")


def test_main_pages_and_dash():
    assert not is_content_page("en", "Main_Page")
    assert not is_content_page("hi", "मुखपृष्ठ")
    assert not is_content_page("en", "-")
    assert not is_content_page("en", "")


def test_normalize_title():
    assert normalize_title("Virat_Kohli") == "Virat Kohli"
    assert normalize_title("Rock_%26_Roll") == "Rock & Roll"
    assert normalize_title("Tab%09Title") == "Tab Title"
    assert normalize_title("__a___b__") == "a b"
    assert normalize_title("%E0%A4%AD%E0%A4%BE%E0%A4%B0%E0%A4%A4") == "भारत"
    assert len(normalize_title("x" * 600)) == 255
