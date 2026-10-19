"""Attention Radar dashboard (Phase 0 placeholder).

Run from the repo root:  streamlit run dashboard/app.py
"""
import os
import socket

import streamlit as st

os.environ.setdefault("RADAR_ENV", "on_host")

st.set_page_config(page_title="Attention Radar", layout="wide")
st.title("Attention Radar")
st.caption("What the world is suddenly reading about on Wikipedia")


def port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


cass = port_open("127.0.0.1", 9042)
mongo = port_open("127.0.0.1", 27017)
c1, c2 = st.columns(2)
c1.metric("Cassandra (9042)", "reachable" if cass else "not running")
c2.metric("MongoDB (27017)", "reachable" if mongo else "not running")

st.info("No data yet. The dashboard is built in Phase 6; this page only checks the setup.")
