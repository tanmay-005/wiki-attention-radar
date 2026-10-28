"""Shared look for every dashboard page: one dark analytics theme, chart styling and formatting.

Colour choices (checked with a colour-vision validator against the card surface #161C24):
  * one primary hue, blue, for interface accents and "above normal" marks; everything at or
    below normal is a neutral grey, so the eye goes straight to the spike;
  * categories take six hues in a fixed order that keeps every neighbouring pair apart for
    red-green and blue-yellow colour blindness, plus grey for "Other";
  * a single-hue blue ramp for magnitude (the map), dark = few, bright = many;
  * red is kept for warnings only (possible bot traffic) and always comes with text.
Text is never coloured by series: values stay in the primary or secondary ink.
"""
from __future__ import annotations

import datetime as dt

import streamlit as st

# surfaces and ink
PAGE, SURFACE, RAISED = "#0E1116", "#161C24", "#1E2631"
BORDER, GRID, AXIS = "rgba(255,255,255,0.08)", "#262E39", "#39424F"
TEXT, TEXT_2, MUTED = "#E8ECF1", "#A9B3C1", "#7C8796"
# roles
PRIMARY = "#3987E5"          # interface accent and "above normal"
NEUTRAL = "#3A4452"          # at or below normal
BASELINE = "#C3CAD4"         # the normal-level line
CRITICAL = "#D03B3B"         # warnings only, always with a label
CAT_COLORS = {"Politics": "#3987E5", "Disasters": "#D95926", "Science & Tech": "#199E70", "Business": "#C98500",
              "Entertainment": "#D55181", "Sports": "#008300", "Other": "#898781"}
CAT_ORDER = list(CAT_COLORS)
SEQ_BLUE = ["#104281", "#1C5CAB", "#2A78D6", "#5598E7", "#86B6EF", "#B7D3F6"]   # few -> many on dark
IST = dt.timedelta(hours=5, minutes=30)

CSS = f"""
<style>
:root {{ --surface:{SURFACE}; --raised:{RAISED}; --border:{BORDER}; --text:{TEXT}; --text2:{TEXT_2}; --muted:{MUTED}; }}
html, body, [data-testid="stAppViewContainer"] {{ font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }}
.block-container {{ padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1480px; }}
[data-testid="stSidebar"] {{ background: {SURFACE}; border-right: 1px solid {BORDER}; }}
[data-testid="stVerticalBlockBorderWrapper"] {{ background: {SURFACE}; border-color: {BORDER} !important; border-radius: 10px; }}
h1, h2, h3 {{ letter-spacing: -0.01em; }}
.brand {{ font-weight: 700; font-size: 1.15rem; color: {TEXT}; display: flex; align-items: center; gap: 8px; }}
.brand .dot {{ width: 10px; height: 10px; border-radius: 50%; background: {PRIMARY}; box-shadow: 0 0 0 4px rgba(57,135,229,.2); }}
.brand-sub {{ color: {MUTED}; font-size: 0.78rem; margin: 2px 0 6px; }}
.pagehead {{ display: flex; justify-content: space-between; align-items: flex-end; flex-wrap: wrap; gap: 8px; margin-bottom: 10px; }}
.pagehead h1 {{ font-size: 1.6rem; font-weight: 650; color: {TEXT}; margin: 0; padding: 0; }}
.pagehead .sub {{ color: {TEXT_2}; font-size: 0.92rem; margin-top: 2px; }}
.pagehead .fresh {{ color: {MUTED}; font-size: 0.8rem; text-align: right; }}
.kpis {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 4px 0 14px; }}
@media (max-width: 900px) {{ .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
.kpi {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 10px; padding: 12px 14px; }}
.kpi .label {{ color: {TEXT_2}; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.05em; }}
.kpi .value {{ color: {TEXT}; font-size: 1.65rem; font-weight: 650; margin-top: 2px; line-height: 1.2;
               white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.kpi .note {{ color: {MUTED}; font-size: 0.8rem; margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.panel-title {{ color: {TEXT}; font-weight: 600; font-size: 0.98rem; }}
.panel-sub {{ color: {MUTED}; font-size: 0.8rem; margin-bottom: 4px; }}
.badge {{ display: inline-flex; align-items: center; gap: 6px; font-size: 0.78rem; color: {TEXT_2};
          border: 1px solid {BORDER}; border-radius: 999px; padding: 1px 9px; }}
.badge i {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
.warn {{ display: inline-flex; gap: 5px; font-size: 0.78rem; color: #F3B4B4; background: rgba(208,59,59,.15);
         border: 1px solid rgba(208,59,59,.45); border-radius: 999px; padding: 1px 9px; }}
.d-title {{ color: {TEXT}; font-size: 1.25rem; font-weight: 650; line-height: 1.25; margin: 8px 0 2px; }}
.d-desc {{ color: {TEXT_2}; font-size: 0.9rem; margin-bottom: 8px; }}
.news {{ color: {TEXT}; font-size: 0.86rem; line-height: 1.45; background: {RAISED}; border-left: 3px solid {PRIMARY};
         border-radius: 0 6px 6px 0; padding: 7px 10px; margin: 6px 0; }}
.news b {{ color: {TEXT_2}; font-weight: 600; }}
.nonews {{ color: {MUTED}; font-size: 0.82rem; margin: 6px 0; }}
.extract {{ color: {TEXT_2}; font-size: 0.88rem; line-height: 1.5; }}
.facts {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; margin: 8px 0; }}
.fact .k {{ color: {MUTED}; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; }}
.fact .v {{ color: {TEXT}; font-size: 1.05rem; font-weight: 600; font-variant-numeric: tabular-nums; }}
.thumb {{ width: 100%; object-fit: cover; border-radius: 8px; display: block; background: {RAISED}; }}
.noimg {{ display: flex; align-items: center; justify-content: center; color: {TEXT_2}; font-size: 0.8rem;
          border: 1px dashed {BORDER}; }}
.artchip {{ display: inline-block; margin: 0 6px 6px 0; padding: 2px 9px; border: 1px solid {BORDER}; border-radius: 6px;
            color: {TEXT} !important; text-decoration: none; font-size: 0.82rem; background: {RAISED}; }}
.artchip b {{ color: {TEXT_2}; margin-right: 4px; font-weight: 600; }}
.footer {{ color: {MUTED}; font-size: 0.8rem; border-top: 1px solid {BORDER}; padding-top: 12px; margin-top: 18px; line-height: 1.5; }}
.muted {{ color: {MUTED}; font-size: 0.82rem; }}
</style>
"""


def apply_style() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def plotly_layout(height: int = 260, legend: bool = False, bargap: float = 0.25) -> dict:
    return dict(
        height=height, margin=dict(l=8, r=12, t=8, b=8), plot_bgcolor=SURFACE, paper_bgcolor=SURFACE,
        font=dict(family="system-ui, -apple-system, Segoe UI, Roboto, sans-serif", color=TEXT_2, size=12),
        xaxis=dict(showgrid=False, linecolor=AXIS, tickcolor=AXIS, zeroline=False),
        yaxis=dict(showgrid=True, gridcolor=GRID, gridwidth=1, zeroline=False, linecolor=AXIS),
        hoverlabel=dict(bgcolor=RAISED, bordercolor=AXIS, font=dict(color=TEXT, size=12)),
        bargap=bargap, showlegend=legend,
        legend=dict(orientation="h", y=1.08, x=0, font=dict(color=TEXT_2, size=11), bgcolor="rgba(0,0,0,0)"),
    )


def badge(category: str) -> str:
    return f"<span class='badge'><i style='background:{CAT_COLORS.get(category, CAT_COLORS['Other'])}'></i>{category}</span>"


def fmt_int(x) -> str:
    return f"{int(round(x)):,}" if x is not None else "–"


def fmt_short(x) -> str:
    x = float(x or 0)
    return f"{x / 1e6:.1f}M" if x >= 1e6 else f"{x / 1e3:.1f}k" if x >= 1e4 else f"{x:,.0f}"


def fmt_time(t: dt.datetime) -> str:
    """'4 Jun 08:00 UTC (13:30 IST)'"""
    return f"{t.day} {t:%b %H:%M} UTC ({(t + IST):%H:%M} IST)"


def require_databases(da) -> None:
    """Error banner and a retry button when Cassandra or MongoDB cannot be reached; stops the page."""
    h = da.health()
    if h["cassandra"] and h["mongo"] and h["has_data"]:
        return
    if not (h["cassandra"] and h["mongo"]):
        missing = " and ".join(n for n, ok in (("Cassandra", h["cassandra"]), ("MongoDB", h["mongo"])) if not ok)
        st.error(f"Cannot reach {missing}. Is Docker running? Start it with "
                 "`docker compose --profile nosql start` in the docker folder.")
        if h["error"]:
            st.caption(h["error"][:300])
    else:
        st.warning("The databases are up but empty. Load them with `bash scripts/run_s6.sh` inside edge.")
    if st.button("Retry"):
        da.reset_connections()
        st.rerun()
    st.stop()
