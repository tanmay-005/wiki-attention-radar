"""Benchmarks page: the measured results in benchmarks/results/*.csv (docs/04_UI_UX_DESIGN.md section 9)."""
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ui import ACCENT, NEUTRAL, plotly_layout

RES = Path(__file__).resolve().parents[2] / "benchmarks" / "results"

st.title("Benchmarks")
st.caption("Each setting was run once on one laptop (Docker, single node), so differences of a few seconds are noise. "
           "Record and byte counts are exact, and in every benchmark both variants produced the same result.")


def load(name: str) -> pd.DataFrame | None:
    p = RES / name
    return pd.read_csv(p) if p.exists() else None


def bars(labels, values, title, unit, best="min", fmt=",.0f"):
    vals = list(values)
    pick = min(vals) if best == "min" else max(vals)
    fig = go.Figure(go.Bar(x=list(labels), y=vals, marker_color=[ACCENT if v == pick else NEUTRAL for v in vals],
                           text=[f"{v:{fmt}}" for v in vals], textposition="outside",
                           hovertemplate="%{x}: %{y:" + fmt + "} " + unit + "<extra></extra>"))
    fig.update_layout(**plotly_layout(height=260), yaxis_title=unit, title=dict(text=title, font=dict(size=14)))
    fig.update_layout(margin=dict(l=8, r=8, t=36, b=8))
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def missing(name: str, how: str) -> None:
    st.info(f"`benchmarks/results/{name}` not found. Run `{how}` inside edge.")


st.subheader("B1 · MapReduce combiner (MR2, one day)")
b1 = load("b1.csv")
if b1 is None:
    missing("b1.csv", "bash benchmarks/b1_combiner.sh")
else:
    lab = b1["run"].str.replace("combiner=", "combiner ")
    c1, c2 = st.columns(2)
    with c1:
        bars(lab, b1["reduce_shuffle_bytes"] / 1e6, "Shuffle size", "MB")
    with c2:
        bars(lab, b1["wall_seconds"], "Run time", "s")
    st.caption("The combiner adds up an article's hours inside each mapper, so far fewer records cross the network. "
               "Run time improves less, because one large gzip file per language is read by a single mapper.")

st.subheader("B2 · Map-output compression (MR1)")
b2 = load("b2.csv")
if b2 is None:
    missing("b2.csv", "bash benchmarks/b2_compression.sh")
else:
    lab = b2["run"].str.replace("map_compress=", "")
    c1, c2, c3 = st.columns(3)
    with c1:
        bars(lab, b2["reduce_shuffle_bytes"] / 1e6, "Shuffle size", "MB")
    with c2:
        bars(lab, b2["cpu_ms"] / 1000, "CPU time", "s")
    with c3:
        bars(lab, b2["wall_seconds"], "Run time", "s")
    st.caption("Gzip gives the smallest shuffle but costs the most CPU; Snappy is cheap enough to win on time. "
               "The jobs use Snappy for the shuffle and Gzip for the stored output.")

st.subheader("B3 · Storage formats in Hive (hourly_views, one day)")
b3 = load("b3.csv")
if b3 is None:
    missing("b3.csv", "bash benchmarks/b3_formats.sh")
else:
    c1, c2 = st.columns(2)
    with c1:
        bars(b3["format"], b3["size_bytes"] / 1e6, "Size on HDFS", "MB")
    with c2:
        long = b3.melt(id_vars="format", value_vars=["q1_count_sum_s", "q2_one_language_top10_s", "q3_hourly_curve_s"],
                       var_name="query", value_name="s")
        names = {"q1_count_sum_s": "Q1 count + sum", "q2_one_language_top10_s": "Q2 top 10 (one language)",
                 "q3_hourly_curve_s": "Q3 views per hour"}
        fig = go.Figure()
        for fmt, colour in zip(b3["format"], [NEUTRAL, ACCENT, "#7A7466"]):
            part = long[long["format"] == fmt]
            fig.add_bar(name=fmt, x=[names[q] for q in part["query"]], y=part["s"], marker_color=colour)
        fig.update_layout(**plotly_layout(height=260), barmode="group", yaxis_title="s",
                          title=dict(text="Query time", font=dict(size=14)))
        fig.update_layout(margin=dict(l=8, r=8, t=36, b=8), showlegend=True)
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    st.caption("Columnar formats read only the columns a query needs. ORC is the smallest and fastest here; "
               "Parquet's slow Q3 is most likely Hive converting Spark's INT96 timestamps.")

st.subheader("B4 · Same daily aggregation: MapReduce vs Spark")
b4 = load("b4.csv")
if b4 is None:
    missing("b4.csv", "bash benchmarks/b4_mr_vs_spark.sh")
else:
    bars(b4["engine"].map({"mapreduce_mr2": "MapReduce (MR2)", "spark_dataframe": "Spark DataFrame"}).fillna(b4["engine"]),
         b4["wall_seconds"], "Run time", "s", fmt=",.1f")
    st.caption("Same input and identical output. Spark keeps the aggregation in the JVM and needs no per-task "
               "Python processes or job setup on YARN.")

st.subheader("B5 · RDD vs DataFrame (same aggregation)")
b5 = load("b5.csv")
if b5 is None:
    missing("b5.csv", "spark-submit benchmarks/b5_rdd_vs_df.py")
else:
    lab = b5["api"].map({"rdd_reduceByKey": "RDD reduceByKey", "dataframe_groupBy": "DataFrame groupBy"}).fillna(b5["api"])
    c1, c2 = st.columns(2)
    with c1:
        bars(lab, b5["seconds"], "Run time", "s", fmt=",.1f")
    with c2:
        bars(lab, b5["shuffle_write_mb"], "Shuffle write", "MB", fmt=",.1f")
    st.caption("The DataFrame version shuffled more data yet ran 3.5 times faster: the RDD version sends every row "
               "to Python lambdas, the DataFrame version stays inside the JVM with generated code.")
