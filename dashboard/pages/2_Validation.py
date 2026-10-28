"""Validation page: known events against detected events (docs/04_UI_UX_DESIGN.md section 8)."""
import statistics

import pandas as pd
import streamlit as st

import data_access as da
from ui import require_databases

st.title("Validation")
st.caption("Does the radar find events we know happened? Each known event (from reliable sources, with its date) "
           "is matched to a detected event that contains the expected article and was first detected within 48 hours.")
require_databases(da)

rows = da.get_validation()
if not rows:
    st.info("No validation results yet. They are produced in Phase 7 by `spark/jobs/s7_validate.py` "
            "from `data/validation/known_events_2024-06.csv`.")
    st.stop()

df = pd.DataFrame(rows)
found = df[df.get("detected", False) == True]  # noqa: E712
c1, c2, c3 = st.columns(3)
c1.metric("Recall", f"{len(found)} / {len(df)}", f"{100 * len(found) / len(df):.0f}%")
delays = [d for d in found.get("delay_hours", []) if d is not None]
c2.metric("Median detection delay", f"{statistics.median(delays):.0f} h" if delays else "–")
ranks = [r for r in found.get("rank", []) if r is not None]
c3.metric("Median rank in its scope", f"{statistics.median(ranks):.0f}" if ranks else "–")

show = pd.DataFrame({
    "Known event": df.get("name"), "Date": df.get("date"), "Scope": df.get("scope"),
    "Detected": df.get("detected").map({True: "✓", False: "✗"}) if "detected" in df else None,
    "Matched event": df.get("matched_title"), "Rank": df.get("rank"),
    "Hours after": df.get("delay_hours"), "Notes": df.get("notes")})
st.dataframe(show, hide_index=True, width="stretch")
