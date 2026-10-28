"""Attention Radar dashboard.

Run on Windows from the repo root (Docker's nosql services must be running):
    streamlit run dashboard/app.py
"""
import streamlit as st

st.set_page_config(page_title="Attention Radar", page_icon="◎", layout="wide")

import data_access as da  # noqa: E402
import radar  # noqa: E402
from ui import apply_style, require_databases  # noqa: E402

apply_style()


def radar_page() -> None:
    require_databases(da)
    radar.render()


nav = st.navigation([
    st.Page(radar_page, title="Overview", icon=":material/monitoring:", default=True),
    st.Page("pages/2_Validation.py", title="Validation", icon=":material/fact_check:"),
    st.Page("pages/3_Benchmarks.py", title="Benchmarks", icon=":material/speed:"),
    st.Page("pages/4_About.py", title="About", icon=":material/info:"),
])
nav.run()
