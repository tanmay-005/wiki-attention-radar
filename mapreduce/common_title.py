"""Title and domain rules shared by the MapReduce jobs (and mirrored by the Hive UDFs).

Kept dependency-free on purpose: this file is shipped to every YARN container
with `-files`, so it must not import anything outside the standard library.

The namespace lists come from step 1.4 (inspect_raw.py on real dumps): English
canonical names work on every wiki, and each wiki also has its own localised
names (Hindi श्रेणी = Category, Japanese 特別 = Special, ...). Only titles whose
prefix before the first ':' is a known namespace are dropped, because real
articles also contain colons (e.g. "Furiosa:_A_Mad_Max_Saga").
"""
from __future__ import annotations

from urllib.parse import unquote

MAX_TITLE_LEN = 255

# English canonical namespace names and aliases (valid on all Wikipedias).
_CANONICAL = {
    "media", "special", "talk", "user", "user talk", "wikipedia", "wikipedia talk",
    "project", "project talk", "wp", "wt", "file", "file talk", "image", "image talk",
    "mediawiki", "mediawiki talk", "template", "template talk", "help", "help talk",
    "category", "category talk", "portal", "portal talk", "draft", "draft talk",
    "module", "module talk", "timedtext", "timedtext talk", "gadget", "gadget talk",
    "gadget definition", "gadget definition talk", "topic", "book", "book talk",
    "education program", "education program talk", "event", "event talk",
}

# Localised namespace names per language (observed in the dumps + standard names).
_LOCAL = {
    "hi": {
        "विशेष", "मीडिया", "वार्ता", "सदस्य", "सदस्य वार्ता", "विकिपीडिया", "विकिपीडिया वार्ता",
        "चित्र", "चित्र वार्ता", "फ़ाइल", "फ़ाइल वार्ता", "मीडियाविकि", "मीडियाविकि वार्ता",
        "साँचा", "साँचा वार्ता", "सहायता", "सहायता वार्ता", "श्रेणी", "श्रेणी वार्ता",
        "प्रवेशद्वार", "प्रवेशद्वार वार्ता", "मसौदा", "मसौदा वार्ता", "मॉड्यूल", "मॉड्यूल वार्ता",
    },
    "ta": {
        "சிறப்பு", "ஊடகம்", "பேச்சு", "பயனர்", "பயனர் பேச்சு", "விக்கிப்பீடியா",
        "விக்கிப்பீடியா பேச்சு", "படிமம்", "படிமப் பேச்சு", "மீடியாவிக்கி", "மீடியாவிக்கி பேச்சு",
        "வார்ப்புரு", "வார்ப்புரு பேச்சு", "உதவி", "உதவி பேச்சு", "பகுப்பு", "பகுப்பு பேச்சு",
        "வலைவாசல்", "வலைவாசல் பேச்சு", "வரைவு", "வரைவு பேச்சு", "தொகுதி", "தொகுதி பேச்சு",
    },
    "ja": {
        "特別", "メディア", "ノート", "利用者", "利用者‐会話", "利用者-会話", "wikipedia‐ノート",
        "wikipedia-ノート", "ファイル", "ファイル‐ノート", "ファイル-ノート", "画像", "画像‐ノート",
        "mediawiki‐ノート", "template‐ノート", "ヘルプ", "ヘルプ‐ノート", "category‐ノート",
        "portal‐ノート", "プロジェクト", "プロジェクト‐ノート", "モジュール", "モジュール‐ノート",
        "ポータル", "ポータル‐ノート", "カテゴリ", "テンプレート",
    },
    "pt": {
        "especial", "discussão", "usuário(a)", "usuário(a) discussão", "usuário", "usuário discussão",
        "utilizador", "utilizador discussão", "wikipédia", "wikipédia discussão", "ficheiro",
        "ficheiro discussão", "arquivo", "arquivo discussão", "imagem", "imagem discussão",
        "multimédia", "mediawiki discussão", "predefinição", "predefinição discussão", "ajuda",
        "ajuda discussão", "categoria", "categoria discussão", "portal discussão", "livro",
        "livro discussão", "anexo", "anexo discussão", "módulo", "módulo discussão", "rascunho",
        "rascunho discussão",
    },
}

# Main pages: huge, always-on traffic that is never an "event".
_MAIN_PAGES = {
    "en": {"Main_Page"},
    "hi": {"मुखपृष्ठ"},
    "ta": {"முதற்_பக்கம்"},
    "ja": {"メインページ"},
    "pt": {"Wikipédia:Página_principal"},
}


# Namespace and main-page names of the other configured wikis (Telugu, Marathi, ...), written
# by scripts/fetch_namespaces.py from each wiki's own siteinfo and shipped next to this file.
try:
    from namespaces_extra import EXTRA_LOCAL, EXTRA_MAIN
except ImportError:
    EXTRA_LOCAL, EXTRA_MAIN = {}, {}


def parse_domain(domain: str, allowed: set[str]) -> tuple[str, str] | None:
    """'en' -> ('en','desktop'), 'en.m' -> ('en','mobile'); anything else -> None."""
    lang, _, rest = domain.partition(".")
    if lang not in allowed:
        return None
    if rest == "":
        return lang, "desktop"
    if rest == "m":
        return lang, "mobile"
    return None  # en.wikt, en.b, commons.m.m, ...


def _prefix_key(title: str) -> str:
    return title.split(":", 1)[0].replace("_", " ").strip().casefold()


def is_content_page(lang: str, raw_title: str) -> bool:
    """False for '-', empty titles, main pages and namespace pages."""
    if not raw_title or raw_title == "-":
        return False
    if raw_title in _MAIN_PAGES.get(lang, ()) or raw_title in EXTRA_MAIN.get(lang, ()):
        return False
    if ":" in raw_title:
        p = _prefix_key(raw_title)
        if p in _CANONICAL or p in _LOCAL.get(lang, ()) or p in EXTRA_LOCAL.get(lang, ()):
            return False
    return True


def normalize_title(raw_title: str) -> str:
    """URL-decode, underscores -> spaces, no tabs/newlines, collapse spaces, max 255 chars."""
    t = unquote(raw_title, errors="replace")
    t = t.replace("_", " ").replace("\t", " ").replace("\r", " ").replace("\n", " ")
    t = " ".join(t.split())
    return t[:MAX_TITLE_LEN]
