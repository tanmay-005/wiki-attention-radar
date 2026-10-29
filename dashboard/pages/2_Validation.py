"""Validation page: known events against detected events, and precision of the top 50
(docs/09_TESTING_QA.md section 5; written by spark/jobs/s7_validate.py)."""
import pandas as pd
import streamlit as st

import data_access as da
from ui import require_databases

st.title("Validation")
st.caption("Does the radar find events we know happened? The known events were picked from Wikipedia's "
           "Current events pages and deaths list for the period, before looking at the radar's output. A known "
           "event counts as found when a detected story contains its Wikipedia article and was first detected "
           "between the day before and 48 hours after the listed day (UTC).")
require_databases(da)

rows = da.get_validation()
summ = da.get_validation_summary()
if not rows:
    st.info("No validation results yet. They are produced by `spark/jobs/s7_validate.py` "
            "from `data/validation/known_events_2026-10.csv`.")
    st.stop()

p50 = (summ.get("precision50") or {})
c1, c2, c3, c4 = st.columns(4)
c1.metric("Recall", f"{summ.get('found', 0)} / {summ.get('known', len(rows))}",
          f"{100 * (summ.get('recall') or 0):.0f}% (target 75%)", delta_color="off")
c2.metric("Precision@50", f"{100 * p50['precision']:.0f}%" if p50.get("precision") is not None else "–",
          f"{p50.get('real', 0)} of {p50.get('labelled', 0)} real (target 80%)" if p50.get("labelled") else
          "not labelled yet", delta_color="off")
md = summ.get("median_delay_hours")
c3.metric("Median detection delay", f"{md:.0f} h" if md is not None else "–",
          "after the listed day starts (UTC)", delta_color="off")
c4.metric("Median rank in its list", f"{summ['median_rank']:.0f}" if summ.get("median_rank") else "–",
          f"{summ.get('top10', 0)} in the top 10 of their day", delta_color="off")
groups = summ.get("by_group") or {}
if len(groups) > 1:
    st.caption(" · ".join(f"{g}: {v['found']} of {v['known']}" for g, v in groups.items()))
if summ.get("india_in_r_in"):
    st.caption(f"Indian events: {summ['india_higher_in_r_in']} of {summ['india_in_r_in']} ranked higher among "
               "readers in India (R:IN) than worldwide (G), as they should be.")

df = pd.DataFrame(rows)
show = pd.DataFrame({
    "Known event": df["name"], "Date": pd.to_datetime(df["date"]).dt.strftime("%d %b"), "List": df["scope"],
    "Group": df.get("group"),
    "Found": df["detected"].map({True: "✓", False: "✗"}), "Matched story": df.get("matched_title"),
    "Rank": df.get("rank"), "Rank worldwide": df.get("rank_g"), "Rank in India": df.get("rank_r_in"),
    "Hours after": df.get("delay_hours"), "Why missed": df.get("why_missed")})
st.dataframe(show, hide_index=True, width="stretch",
             column_config={"Rank": st.column_config.NumberColumn(format="%d", help="Best position in its list "
                                                                  "on the days the story was listed"),
                            "Rank worldwide": st.column_config.NumberColumn(format="%d"),
                            "Rank in India": st.column_config.NumberColumn(format="%d"),
                            "Hours after": st.column_config.NumberColumn(
                                format="%.0f", help="From the start of the listed day (UTC) to the first "
                                "detection; negative when the spike began the day before")})
