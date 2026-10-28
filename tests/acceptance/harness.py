"""The Overview page as the browser runs it, for scripts/run_acceptance.py (Streamlit AppTest).
Uses the real data layer unless ACCEPT_FAKE=1. ACCEPT_DEAD_CASSANDRA=1 points Cassandra at a closed
port (test A12). The render time of each run is left in session_state["_ms"]."""
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "dashboard"), str(ROOT / "tests" / "dashboard"), str(ROOT)]
os.environ.setdefault("RADAR_ENV", "on_host")
import streamlit as st  # noqa: E402

st.set_page_config(layout="wide")
if os.environ.get("ACCEPT_FAKE"):
    import fake_data
    sys.modules["data_access"] = fake_data
import data_access as da  # noqa: E402

if os.environ.get("ACCEPT_DEAD_CASSANDRA"):
    from spark.common.config import env_name
    da.CFG["connections"][env_name()]["cassandra_port"] = 1
import radar  # noqa: E402
from ui import apply_style, require_databases  # noqa: E402

apply_style()
if not os.environ.get("ACCEPT_FAKE"):
    require_databases(da)
t0 = time.perf_counter()
radar.render()
st.session_state["_ms"] = 1000 * (time.perf_counter() - t0)
