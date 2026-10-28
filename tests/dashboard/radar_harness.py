"""Runs the Radar page with the fake data layer (used by test_radar.py through Streamlit's AppTest)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "dashboard"), str(Path(__file__).parent)]
import streamlit as st  # noqa: E402

st.set_page_config(layout="wide")
import fake_data  # noqa: E402

sys.modules["data_access"] = fake_data
import radar  # noqa: E402
from ui import apply_style  # noqa: E402

apply_style()
radar.render()
