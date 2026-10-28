"""Step 6.7: run the dashboard acceptance tests A1-A14 (docs/09_TESTING_QA.md section 6) against the
real Cassandra and MongoDB, and write the record to tests/acceptance_dashboard.md.

The Overview page is driven with Streamlit's AppTest, the same way a browser session drives it:
filters are set, buttons and the mode control are clicked, and the page is checked after each step.
Two clicks cannot be made by AppTest and are done through the state they set: a click on a category
bar (A10) sets `pending_category`, and "Open full story" (A9) sets `open_event`.

Timing (A14): each step is run twice. "cold" clears Streamlit's cache first, so every query goes to
the databases (a filter combination nobody has opened yet); "warm" is the same step again (a
combination opened in the last 5 minutes).

Run on Windows from the repo root, with the nosql containers up:
  .venv\\Scripts\\python scripts\\run_acceptance.py
"""
from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HARNESS = str(ROOT / "tests" / "acceptance" / "harness.py")
OUT = ROOT / "tests" / "acceptance_dashboard.md"
FAKE = "--fake" in sys.argv
if FAKE:
    os.environ["ACCEPT_FAKE"] = "1"
os.environ.setdefault("RADAR_ENV", "on_host")
sys.path[:0] = [str(ROOT / "dashboard"), str(ROOT / "tests" / "dashboard"), str(ROOT)]

import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

if FAKE:
    import fake_data as da  # noqa: E402
else:
    import data_access as da  # noqa: E402

TIMES: dict[str, list[tuple[float, float]]] = {}
RESULTS: list[tuple[str, str, str, str]] = []


def text(at) -> str:
    parts = [m.value for m in at.markdown] + [c.value for c in at.caption] + [i.value for i in at.info] + \
            [e.value for e in at.error]
    return "\n".join(str(p) for p in parts)


def rows(at) -> list[dict]:
    return at.dataframe[0].value.to_dict("records") if at.dataframe else []


class Step:
    """One page state: run it cold and warm, keep the warm AppTest for the checks."""

    def __init__(self, test_id: str, **state):
        self.id, self.state = test_id, state

    def run(self, actions=()) -> AppTest:
        cold = self._once(actions, clear=True)
        warm = self._once(actions, clear=False)
        TIMES.setdefault(self.id, []).append((cold[1], warm[1]))
        return warm[0]

    def _once(self, actions, clear: bool):
        if clear and not FAKE:
            st.cache_data.clear()
        at = AppTest.from_file(HARNESS, default_timeout=120)
        for k, v in self.state.items():
            at.session_state[k] = v
        at.run()
        ms = [at.session_state["_ms"]] if "_ms" in at.session_state else []
        for act in actions:
            at = act(at)
            if "_ms" in at.session_state:
                ms.append(at.session_state["_ms"])
        if at.exception:
            raise AssertionError(f"page raised: {at.exception[0].message}")
        return at, max(ms) if ms else 0.0


def check(test_id: str, what: str, fn) -> None:
    try:
        note = fn() or ""
        RESULTS.append((test_id, what, "Pass", note))
        print(f"  {test_id}  Pass  {note}")
    except AssertionError as e:
        RESULTS.append((test_id, what, "FAIL", str(e)))
        print(f"  {test_id}  FAIL  {e}")
    except Exception as e:  # noqa: BLE001
        RESULTS.append((test_id, what, "FAIL", f"{type(e).__name__}: {e}"))
        print(f"  {test_id}  FAIL  {type(e).__name__}: {e}")


def scope_days(as_of: dt.date, n: int, first: dt.date) -> list[dt.date]:
    return [d for d in (as_of - dt.timedelta(days=i) for i in range(n)) if d >= first]


# ---- the tests -------------------------------------------------------------------
def a1():
    at = Step("A1").run()
    s = at.session_state
    assert (s["country"], s["category"], s["lang"], s["window"]) == ("", "All", "All", "7 days"), "defaults differ"
    assert rows(at), "the stories table is empty"
    assert "Top stories worldwide" in text(at), "summary sentence missing"
    return f"{len(rows(at))} stories listed; top: {rows(at)[0]['Story']}"


def a2():
    at = Step("A2", country="IN").run()
    assert at.session_state["mode"] == "Readers in", "mode is not Readers in"
    assert not at.selectbox(key="region").disabled, "state dropdown disabled"
    assert "read by people in India" in text(at)
    return f"{len(rows(at))} stories read in India"


def a3():
    at = Step("A3", category="Politics").run()
    cats = {r["Category"] for r in rows(at)}
    assert rows(at) and cats == {"Politics"}, f"categories in list: {cats}"
    return f"{len(rows(at))} stories, all Politics"


def a4():
    at = Step("A4", country="IN", category="Politics").run()
    cats = {r["Category"] for r in rows(at)}
    assert rows(at) and cats == {"Politics"}, f"categories in list: {cats}"
    assert "Top Politics stories read by people in India" in text(at)
    return f"{len(rows(at))} stories, all Politics"


def a5():
    # the Indian state with the most located stories (Rajasthan in the original plan has none in Oct 2026)
    best = max(da.list_regions("IN"), key=lambda r: r["events"])
    code, name = best["code"], best["name"]
    ui_code = {"IN-TS": "IN-TG", "IN-OR": "IN-OD", "IN-CT": "IN-CG", "IN-UT": "IN-UK"}.get(code, code)
    at = Step("A5", country="IN", mode="Events about", region=ui_code, window="30 days").run()
    assert at.session_state["region"] == ui_code, f"{name} not selectable"
    meta = da.get_meta()
    evs = da.get_top_events(f"A:IN:{code}", scope_days(meta["as_of"], 30, meta["detect_start"]), limit=20)
    assert evs and rows(at), f"no stories about {name}"
    bad = [e["title"] for e in evs if ((da.get_event(e["event_id"]) or {}).get("location") or {}).get("region")
           != code]
    assert not bad, f"not located in {name}: {bad[:3]}"
    return f"{name}: {len(rows(at))} stories (30 days), each located there"


def a6():
    def to_about(at):
        at.segmented_control(key="mode").set_value("Events about").run()
        return at

    def to_readers(at):
        at.segmented_control(key="mode").set_value("Readers in").run()
        return at

    at = Step("A6", country="IN", region="IN-TN", window="30 days").run()
    assert "Spikes on Tamil Wikipedia" in text(at), "Tamil Wikipedia explanation missing"
    n_readers = len(rows(at))
    at = Step("A6", country="IN", region="IN-TN", window="30 days").run([to_about])
    assert at.session_state["region"] == "IN-TN", "state lost after switching to Events about"
    n_about = len(rows(at))
    at = Step("A6", country="IN", region="IN-TN", window="30 days").run([to_about, to_readers])
    assert at.session_state["region"] == "IN-TN", "state lost after switching back"
    return f"Tamil Nadu kept both ways; Readers in {n_readers} stories, Events about {n_about}"


def a7():
    at = Step("A7", lang="hi").run()
    meta = da.get_meta()
    evs = da.get_top_events("G", scope_days(meta["as_of"], 7, meta["detect_start"]), "ALL", ["hi"], limit=None)
    assert rows(at), "no Hindi stories"
    assert all("hi" in e["langs"] for e in evs), "a listed story has no Hindi article"
    return f"{len(evs)} stories, all with a Hindi Wikipedia article"


def a8():
    day = Step("A8", window="24 hours").run()
    week = Step("A8", window="7 days").run()
    meta = da.get_meta()
    n1 = len(da.get_top_events("G", scope_days(meta["as_of"], 1, meta["detect_start"]), limit=None))
    n7 = len(da.get_top_events("G", scope_days(meta["as_of"], 7, meta["detect_start"]), limit=None))
    assert n7 >= n1, f"7 days has fewer stories ({n7}) than 24 hours ({n1})"
    titles = [r["Story"] for r in rows(week)]
    assert len(titles) == len(set(titles)), "a story is listed twice"
    assert rows(day), "24 hours list is empty"
    return f"24 h: {n1} stories, 7 days: {n7}; no duplicates in the table"


def a9():
    at = Step("A9").run()
    import html
    top = rows(at)[0]["Story"].lstrip("⚠ ")
    assert html.escape(top) in text(at) or top in text(at), "detail panel does not show the selected story"
    meta = da.get_meta()
    evs = da.get_top_events("G", scope_days(meta["as_of"], 7, meta["detect_start"]), limit=2)
    at = Step("A9", open_event=evs[1]["event_id"]).run()
    t = text(at)
    assert "Why people were reading" in t, "full story did not open"
    assert "Hourly views of" in t and "dashed line = its normal level" in t, "hourly chart with normal line missing"
    return f"detail panel shows '{top}'; full story of '{evs[1]['title']}' has the hourly chart"


def a10():
    at = Step("A10", pending_category="Sports").run()
    assert at.session_state["category"] == "Sports", "category not set by the bar click"
    assert {r["Category"] for r in rows(at)} == {"Sports"}
    at = Step("A10", category="Sports", pending_category="All").run()
    assert at.session_state["category"] == "All", "second click did not clear the filter"
    return "Sports set by the click, cleared by the second click"


def a11():
    def reset(at):
        next(b for b in at.button if b.label == "Reset filters").click().run()
        return at

    # a state is chosen too: AppTest cannot click while a dropdown shows its empty ("All") choice
    at = Step("A11", country="IN", mode="Events about", region="IN-RJ", category="Sports", lang="hi",
              window="30 days").run([reset])
    s = at.session_state
    got = (s["country"], s["mode"], s["category"], s["lang"], s["window"], s["as_of"])
    want = ("", "Readers in", "All", "All", "7 days", da.get_meta()["as_of"])
    assert s["region"] == "", "state not cleared"
    assert got == want, f"after reset: {got}"
    return "all filters back to the defaults"


def a12():
    if FAKE:
        return "skipped with --fake"
    code = ("import os,sys; os.environ['ACCEPT_DEAD_CASSANDRA']='1'; "
            "from streamlit.testing.v1 import AppTest; "
            f"at=AppTest.from_file(r'{HARNESS}', default_timeout=60); at.run(); "
            "t=' '.join(str(e.value) for e in at.error); "
            "print('EXC' if at.exception else 'NOEXC'); print(t)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120).stdout
    assert "NOEXC" in out, "the page raised an exception"
    assert "Cannot reach Cassandra" in out, f"no error banner: {out[-200:]}"
    return "red 'Cannot reach Cassandra' banner, no stack trace (Cassandra pointed at a closed port)"


def a13():
    c = next((c for c in da.list_countries() if not c["readers"]), None)
    assert c, "every listed country has per-country data"
    at = Step("A13", country=c["code"]).run()
    assert "does not publish per-country data" in text(at), "empty-state message missing"
    return f"{c['name']}: privacy message shown"


TESTS = [
    ("A1", "Open the app", a1), ("A2", "Country = India", a2), ("A3", "Global + Politics", a3),
    ("A4", "India + Politics", a4), ("A5", "India, Events about, a state", a5),
    ("A6", "India, Readers in, Tamil Nadu; switch modes both ways", a6), ("A7", "Language = Hindi", a7),
    ("A8", "24 hours vs 7 days", a8), ("A9", "Detail panel and full story", a9),
    ("A10", "Category bar click and second click", a10), ("A11", "Reset filters", a11),
    ("A12", "Cassandra unreachable", a12), ("A13", "Country without per-country data", a13),
]


def main() -> None:
    print(f"acceptance tests against {'the fake data' if FAKE else 'Cassandra + MongoDB'}")
    for tid, what, fn in TESTS:
        check(tid, what, fn)
    cold = max((c for v in TIMES.values() for c, _ in v), default=0)
    warm = max((w for v in TIMES.values() for _, w in v), default=0)
    slow = sorted(((c, k) for k, v in TIMES.items() for c, _ in v), reverse=True)[:3]
    ok = cold < 1000
    RESULTS.append(("A14", "Every filter change under 1 s", "Pass" if ok else "FAIL",
                    f"slowest cold {cold:.0f} ms, slowest warm {warm:.0f} ms"))
    print(f"  A14  {'Pass' if ok else 'FAIL'}  slowest cold {cold:.0f} ms ({', '.join(k for _, k in slow)}), "
          f"slowest warm {warm:.0f} ms")

    meta = da.get_meta()
    n_pass = sum(1 for r in RESULTS if r[2] == "Pass")
    lines = [
        "# Dashboard acceptance tests (step 6.7)", "",
        "A1–A14 from `docs/09_TESTING_QA.md` section 6, run with `scripts/run_acceptance.py`.", "",
        f"- Run: {dt.datetime.now():%Y-%m-%d %H:%M}, against "
        + ("the fake data layer" if FAKE else "the real Cassandra and MongoDB (nosql containers)"),
        f"- Data: {meta['data_start']:%d %b} – {meta['as_of']:%d %b %Y}, detection from {meta['detect_start']:%d %b}, "
        f"{meta.get('events') or '?'} events",
        "- Method: the Overview page is driven with Streamlit's AppTest (filters set, buttons and the mode "
        "control clicked, the page checked after each step). A category-bar click (A10) and "
        "\"Open full story\" (A9) are made through the session state those clicks set.",
        "- Time: render time of the page. Cold = Streamlit's cache cleared first, so every query goes to the "
        "databases; warm = the same view again within the 5-minute cache.", "",
        "| ID | Test | Result | Cold (ms) | Warm (ms) | Notes |", "|---|---|---|---|---|---|",
    ]
    for tid, what, res, note in RESULTS:
        t = TIMES.get(tid)
        c = f"{max(x for x, _ in t):.0f}" if t else "–"
        w = f"{max(y for _, y in t):.0f}" if t else "–"
        lines.append(f"| {tid} | {what} | {res} | {c} | {w} | {note.replace('|', '/')} |")
    lines += ["", f"Result: {n_pass} of {len(RESULTS)} passed.", ""]
    if not FAKE:
        OUT.write_text("\n".join(lines), encoding="utf-8")
        print(f"\n{n_pass} of {len(RESULTS)} passed. Written to {OUT.relative_to(ROOT)}")
    else:
        print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
