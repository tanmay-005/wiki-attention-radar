"""T-M1..T-M5: run the streaming scripts with plain pipes, the way Hadoop would.

map -> sort by key -> (combiner) -> sort -> reduce. No cluster needed, so these
run in a second on any machine:  python3 -m pytest tests/mr -q
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MR = ROOT / "mapreduce"
FIX = ROOT / "tests" / "fixtures" / "mr"
RAW = FIX / "pageviews-20240603-120000"


def run(script, text, **env):
    e = dict(os.environ, PYTHONPATH=str(MR), RADAR_LANGS="en,hi,ta,ja,pt", **env)
    p = subprocess.run([sys.executable, str(MR / script)], input=text, capture_output=True,
                       text=True, env=e, check=True)
    return p.stdout, p.stderr


def key_sort(text, nkeys):
    """Sort lines by their first nkeys tab fields, byte order (like Hadoop's Text keys)."""
    lines = [l for l in text.splitlines() if l]
    return "".join(l + "\n" for l in sorted(lines, key=lambda l: "\t".join(l.split("\t")[:nkeys]).encode()))


def counters(stderr):
    out = {}
    for l in stderr.splitlines():
        if l.startswith("reporter:counter:Radar,"):
            name, n = l.split(",")[1:3]
            out[name] = out.get(name, 0) + int(n)
    return out


def mr1(combiner=True):
    mapped, err = run("mr1_mapper.py", RAW.read_text(encoding="utf-8"), MR_INPUT_FILE=str(RAW))
    data = key_sort(mapped, 4)
    if combiner:
        data, _ = run("sum_reducer.py", data)
        data = key_sort(data, 4)
    out, _ = run("sum_reducer.py", data)
    return out, counters(err)


def test_m1_filter_normalise_matches_expected():
    """T-M1: every filter rule in the fixture gives exactly the expected rows."""
    out, _ = mr1()
    assert key_sort(out, 4) == key_sort((FIX / "expected_mr1.tsv").read_text(encoding="utf-8"), 4)


def test_m2_combiner_does_not_change_result():
    """T-M2: reducer used as combiner + reducer == reducer alone."""
    with_c, _ = mr1(combiner=True)
    without_c, _ = mr1(combiner=False)
    assert key_sort(with_c, 4) == key_sort(without_c, 4)


def test_m3_daily_agg_merges_platforms_and_splits_midnight():
    """T-M3: desktop + mobile and all hours of a day are summed; 23:00 and 00:00 land on different days."""
    hourly, _ = mr1()
    hourly += ("en\tdesktop\tVirat Kohli\t2024-06-03T23\t100\n"
               "en\tmobile\tVirat Kohli\t2024-06-04T00\t40\n")
    mapped, _ = run("mr2_mapper.py", hourly)
    daily, _ = run("sum_reducer.py", key_sort(mapped, 3))
    rows = {tuple(l.split("\t")[:3]): int(l.split("\t")[3]) for l in daily.splitlines()}
    assert rows[("en", "Virat Kohli", "2024-06-03")] == 1834 + 5000 + 100
    assert rows[("en", "Virat Kohli", "2024-06-04")] == 40
    assert rows[("en", "Furiosa: A Mad Max Saga", "2024-06-03")] == 5044
    assert rows[("hi", "भारत", "2024-06-03")] == 120


def test_m4_top_n_keeps_100_in_descending_order():
    """T-M4: exactly N rows per (lang, day), most-viewed first (secondary sort)."""
    daily = "".join(f"{lang}\tT{i}\t{day}\t{100 + (i * 37) % 997}\n"
                    for lang in ("en", "hi") for day in ("2024-06-03", "2024-06-04") for i in range(150))
    daily += "en\tTiny\t2024-06-03\t50\n"            # below the 100-view floor
    mapped, _ = run("mr3_mapper.py", daily)
    # what KeyFieldBasedComparator "-k1,1 -k2,2 -k3,3nr" does
    lines = sorted(mapped.splitlines(), key=lambda l: (l.split("\t")[0], l.split("\t")[1], -int(l.split("\t")[2])))
    out, _ = run("mr3_reducer.py", "".join(l + "\n" for l in lines), TOPN="100")
    groups = {}
    for l in out.splitlines():
        lang, day, rank, views, title = l.split("\t")
        groups.setdefault((lang, day), []).append((int(rank), int(views), title))
    assert len(groups) == 4
    for rows in groups.values():
        assert len(rows) == 100
        assert [r[0] for r in rows] == list(range(1, 101))
        views = [r[1] for r in rows]
        assert views == sorted(views, reverse=True)
        assert "Tiny" not in [r[2] for r in rows]


def test_m5_malformed_lines_counted_not_crashing():
    """T-M5: 3-field lines and non-numeric views are counted as bad_lines; every input line is accounted for."""
    _, c = mr1()
    assert c["lines_in"] == 30
    assert c["bad_lines"] == 2
    assert c["kept"] == 13            # 13 mapper rows -> 12 after the two Rock & Roll rows merge
    assert c["lines_in"] == c["kept"] + c["other_domain"] + c["non_content"] + c["bad_lines"]


def test_mr4_search():
    """MR4: map-only search returns every row of one title."""
    hourly, _ = mr1()
    out, err = run("mr4_search.py", hourly, SEARCH_TITLE="Virat_Kohli")
    assert sorted(l.split("\t")[1] for l in out.splitlines()) == ["desktop", "mobile"]
    assert counters(err)["search_hits"] == 2
    out, _ = run("mr4_search.py", hourly, SEARCH_TITLE="भारत", SEARCH_LANG="hi")
    assert len(out.splitlines()) == 2
