"""Attention Radar dashboard.

Run on Windows from the repo root (Docker's nosql services must be running):
    streamlit run dashboard/app.py
"""
import time

import streamlit as st

st.set_page_config(page_title="Attention Radar", page_icon="◎", layout="wide")

import data_access as da  # noqa: E402
import radar  # noqa: E402
from ui import apply_style, require_databases  # noqa: E402

apply_style()


def radar_page() -> None:
    require_databases(da)
    t0 = time.perf_counter()
    try:
        radar.render()
    finally:                                     # acceptance test A14: every filter change < 1 s
        s = st.session_state
        print(f"[timing] Overview {1000 * (time.perf_counter() - t0):6.0f} ms   country={s.get('country') or 'Global'} "
              f"mode={s.get('mode')} state={s.get('region') or 'All'} category={s.get('category')} "
              f"lang={s.get('lang')} window={s.get('window')}", flush=True)


nav = st.navigation([
    st.Page(radar_page, title="Overview", icon=":material/monitoring:", default=True),
    st.Page("pages/2_Validation.py", title="Validation", icon=":material/fact_check:"),
    st.Page("pages/3_Benchmarks.py", title="Benchmarks", icon=":material/speed:"),
    st.Page("pages/4_About.py", title="About", icon=":material/info:"),
])
nav.run()
