"""Java UDF rules (hive/udf/TitleRules.java) must match the Python rules MR1 uses.

Compiles TitleRules + ParityCheck with plain javac (no Hive jars needed), feeds
both implementations the same (lang, raw_title) pairs and compares the results.
Skipped when javac is not available (run it inside edge).
"""
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "mapreduce"))
import common_title as ct  # noqa: E402

UDF = ROOT / "hive" / "udf"
pytestmark = pytest.mark.skipif(shutil.which("javac") is None, reason="javac not installed")


def cases():
    out = []
    langs = ["en", "hi", "ta", "ja", "pt"]
    # every namespace name in several spellings, on every wiki
    names = set(ct._CANONICAL)
    for s in ct._LOCAL.values():
        names |= s
    for name in sorted(names):
        for variant in {name, name.title(), name.upper(), name.replace(" ", "_"), " " + name.replace(" ", "_")}:
            for lang in langs:
                out.append((lang, f"{variant}:Something"))
    for lang, s in ct._MAIN_PAGES.items():
        for m in s:
            out.append((lang, m))
    fixed = [
        "Virat_Kohli", "Rock_%26_Roll", "Tab%09Title", "__a___b__", "-", "", "Furiosa:_A_Mad_Max_Saga",
        "マッドマックス:フュリオサ", "%E0%A4%AD%E0%A4%BE%E0%A4%B0%E0%A4%A4", "100%", "%zz%4", "%E0%A4", "a%E0b",
        "%ED%A0%80", "%C3%28", "%F0%9F%98%80", "C%2B%2B", "a+b", "%25", "x" * 600, "é" * 300,
        "Stra%C3%9Fe:Weg", "ﬁle:X", "Ｆｉｌｅ:X", "a\u00a0\u00a0b", "a\u3000b", "a\u2028b", "a\u0085b",
        "\u200bzero", "%E2%80%83em", "Bad%0Aline", "_%20_", "%C3%A9%E0", "é%A9", "भ%E0%A4", "%",
        "Wikipédia:Página_principal", "WIKIPÉDIA:x", "STRASSE:x", "talk_:x", " talk :x", "Talk%3AX",
    ]
    for t in fixed:
        for lang in langs:
            out.append((lang, t))
    # random fuzz: percent escapes, unicode, spaces, colons
    rnd = random.Random(42)
    alphabet = list("ab_:%%%-+ ") + ["%E0", "%A4", "%C3", "%A9", "%2", "%G1", "é", "भ", "日", "\u00a0", "\u3000", "ß", "ﬁ"]
    for _ in range(3000):
        t = "".join(rnd.choice(alphabet) for _ in range(rnd.randint(1, 14)))
        out.append((rnd.choice(langs), t.replace("\t", " ").replace("\n", " ")))
    # real raw lines from the MR fixture
    for line in (ROOT / "tests/fixtures/mr/pageviews-20240603-120000").read_text(encoding="utf-8").splitlines():
        parts = line.split(" ")
        if len(parts) == 4:
            out.append((parts[0].split(".")[0], parts[1]))
    return out


def python_result(lang, title):
    norm = ct.normalize_title(title)
    norm = norm.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return ("1" if ct.is_content_page(lang, title) else "0") + "\t" + norm


def test_java_matches_python():
    pairs = [(l, t) for l, t in cases() if "\n" not in t and "\r" not in t]
    with tempfile.TemporaryDirectory() as d:
        subprocess.run(["javac", "-nowarn", "-encoding", "UTF-8", "-d", d,
                        str(UDF / "TitleRules.java"), str(UDF / "ParityCheck.java")],
                       check=True, capture_output=True)
        data = "".join(f"{l}\t{t}\n" for l, t in pairs).encode("utf-8")
        p = subprocess.run(["java", "-cp", d, "radar.udf.ParityCheck"], input=data,
                           capture_output=True, check=True)
    java = p.stdout.decode("utf-8").split("\n")[:-1]
    assert len(java) == len(pairs)
    bad = [(l, t, python_result(l, t), j) for (l, t), j in zip(pairs, java) if python_result(l, t) != j]
    assert not bad, f"{len(bad)} mismatches, first few: {bad[:5]}"
