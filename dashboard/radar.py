"""Overview page: an analytics dashboard of what people suddenly read on Wikipedia.

Layout: filters in the sidebar; a header and four KPIs; new stories per day by category next to
the category breakdown; the ranked stories table next to the selected story; the reader map next
to the language breakdown. Filter rules and scopes follow docs/04_UI_UX_DESIGN.md section 4.
"""
from __future__ import annotations

import datetime as dt
import html
from urllib.parse import quote

import plotly.graph_objects as go
import streamlit as st

import data_access as da
from enrichment.regions import LANG_STATES
from ui import (BASELINE, CAT_COLORS, CAT_ORDER, NEUTRAL, PRIMARY, SEQ_BLUE, SURFACE, TEXT, badge,
                fmt_short, fmt_time, plotly_layout)

WINDOWS = {"24 hours": 1, "7 days": 7, "30 days": 30}
LANG_NAMES = {"en": "English", "hi": "Hindi", "ta": "Tamil", "ja": "Japanese", "pt": "Portuguese", "te": "Telugu",
              "mr": "Marathi", "kn": "Kannada", "ml": "Malayalam", "bn": "Bengali", "gu": "Gujarati",
              "pa": "Punjabi", "or": "Odia", "as": "Assamese"}
READERS, ABOUT = "Readers in", "Events about"
TABLE_ROWS = 100
S = st.session_state
CHART = {"displayModeBar": False}


# ---- filter state ----------------------------------------------------------------
def defaults(as_of: dt.date) -> dict:
    return {"country": "", "region": "", "mode": READERS, "lang": "All", "window": "7 days",
            "as_of": as_of, "category": "All"}


def init_state(as_of: dt.date) -> None:
    for k, v in defaults(as_of).items():
        S.setdefault(k, v)
    S.setdefault("chart_key", 0)
    if "pending_category" in S:                      # set by a click on the category chart
        S.category = S.pop("pending_category")


def on_country() -> None:
    S.region, S.mode = "", READERS


def on_mode() -> None:
    if not S.mode:
        S.mode = READERS
    # the selected state is kept: both modes use the same state list (state_options)


# Wikidata still uses some pre-2023 ISO 3166-2:IN codes; the list shows the current ones
OLD_CODES = {"IN-TS": "IN-TG", "IN-OR": "IN-OD", "IN-CT": "IN-CG", "IN-UT": "IN-UK"}


def state_options(country: str) -> list[dict]:
    """One list of states for both modes, so the chosen state never changes when the mode does.
    Each state has `langs` (its language Wikipedias, for "Readers in") and `about_code` (its code
    in the "Events about" index, None when no story is located there)."""
    out = {}
    for r in da.reader_regions(country):
        out[r["code"]] = {"code": r["code"], "name": r["name"], "langs": r["langs"], "about_code": None}
    for r in da.list_regions(country):
        code = OLD_CODES.get(r["code"], r["code"])
        if code not in out:
            out[code] = {"code": code, "name": r["name"], "langs": [], "about_code": None}
        out[code]["about_code"] = r["code"]
    for r in out.values():
        r["name"] = r["name"].replace("National Capital Territory of ", "")
    return sorted(out.values(), key=lambda r: r["name"])


def on_window() -> None:
    if not S.window:
        S.window = "7 days"


def on_reset() -> None:
    for k, v in defaults(da.get_meta()["as_of"]).items():
        S[k] = v


# ---- helpers ---------------------------------------------------------------------
def scope_of(country: str | None, mode: str, region: dict | None) -> str | None:
    """The ranked list to show; None when the chosen state has no data in this mode."""
    if not country:
        return "G"
    if mode == READERS:
        if not region:
            return f"R:{country}"
        return f"R:{country}:{region['code']}" if region["langs"] else None
    if not region:
        return f"A:{country}"
    return f"A:{country}:{region['about_code']}" if region["about_code"] else None


def country_readers(scope: str) -> bool:
    """R:<CC>: Wikimedia's daily per-country data."""
    return scope.startswith("R:") and scope.count(":") == 1


def state_readers(scope: str) -> bool:
    """R:<CC>:<STATE>: hourly spikes on the state's language Wikipedia."""
    return scope.startswith("R:") and scope.count(":") == 2


def langs_text(langs: list[str]) -> str:
    return " and ".join(LANG_NAMES.get(l, l) for l in langs)


def window_days(as_of: dt.date, n: int, first: dt.date) -> tuple[list[dt.date], bool]:
    days = [as_of - dt.timedelta(days=i) for i in range(n)]
    kept = [d for d in days if d >= first]
    return kept, len(kept) < len(days)


def wiki_url(lang: str, title: str) -> str:
    return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"


def esc(s) -> str:
    return html.escape(str(s)) if s is not None else ""


def thumb_html(url: str | None, height: int) -> str:
    if url:
        return f"<img class='thumb' style='height:{height}px' src='{esc(url)}' loading='lazy' alt=''>"
    return ""


def panel(title: str, sub: str | None = None) -> None:
    st.markdown(f"<div class='panel-title'>{title}</div>" + (f"<div class='panel-sub'>{sub}</div>" if sub else ""),
                unsafe_allow_html=True)


# ---- sidebar ---------------------------------------------------------------------
def sidebar(meta: dict, countries: list[dict], cname: dict) -> list[dict]:
    with st.sidebar:
        st.markdown("<div class='brand'><span class='dot'></span>Attention Radar</div>"
                    "<div class='brand-sub'>Wikipedia reading spikes, worldwide and by country</div>",
                    unsafe_allow_html=True)
        st.selectbox("Country", [""] + [c["code"] for c in countries], key="country", on_change=on_country,
                     format_func=lambda c: cname[c] if c else "Global (all readers)")
        st.segmented_control("Measure by", [READERS, ABOUT], key="mode", on_change=on_mode, disabled=not S.country,
                             help="Readers in: what people in that country read more than usual. "
                                  "Events about: worldwide spikes whose subject is located there.")
        regions = state_options(S.country) if S.country else []
        names = {r["code"]: r["name"] for r in regions}
        if S.region and S.region not in names:
            S.region = ""
        st.selectbox("State / region", [""] + list(names), key="region",
                     disabled=not S.country or not regions,
                     help="Readers in: Wikimedia gives reader locations only per country, so a state is "
                          "approximated by its language Wikipedia (Tamil Nadu = Tamil Wikipedia). "
                          "Events about: stories whose subject Wikidata places in that state.",
                     format_func=lambda r: names[r] if r else
                     ("No states with data" if S.country and not regions else "All states / regions"))
        st.selectbox("Category", ["All"] + da.CATEGORIES, key="category")
        st.selectbox("Wikipedia language", ["All"] + da.LANGS, key="lang",
                     format_func=lambda l: "All languages" if l == "All" else LANG_NAMES.get(l, l))
        st.segmented_control("Time window", list(WINDOWS), key="window", on_change=on_window)
        detect_days = [meta["as_of"] - dt.timedelta(days=i)
                       for i in range((meta["as_of"] - meta["detect_start"]).days + 1)]
        st.selectbox("As of", detect_days, key="as_of", format_func=lambda d: d.strftime("%a %d %b %Y"))
        st.button("Reset filters", on_click=on_reset, width="stretch")
        run = meta.get("pipeline_run")
        st.caption(f"Data {meta['data_start']:%d %b} – {meta['as_of']:%d %b %Y} (UTC). Spikes detected from "
                   f"{meta['detect_start']:%d %b}." + (f" Loaded {run:%d %b %H:%M} UTC." if run else ""))
    return regions


# ---- page ------------------------------------------------------------------------
def render() -> None:
    meta = da.get_meta()
    init_state(meta["as_of"])
    countries = da.list_countries()
    cname = {c["code"]: c["name"] for c in countries}
    regions = sidebar(meta, countries, cname)

    region = next((r for r in regions if r["code"] == S.region), None)
    region_name = region["name"] if region else None
    scope = scope_of(S.country, S.mode, region)
    days, clipped = window_days(S.as_of, WINDOWS[S.window], meta["detect_start"])
    langs = None if S.lang == "All" else [S.lang]
    category = "ALL" if S.category == "All" else S.category
    if scope is None:                                # the state has no data in this mode
        st.markdown(f"<div class='pagehead'><div><h1>Attention overview</h1><div class='sub'>"
                    f"{esc(S.mode)} {esc(region_name)}</div></div></div>", unsafe_allow_html=True)
        if S.mode == READERS:
            st.info(f"No reader data for **{region_name}**. Wikimedia gives reader locations only per country, and "
                    f"{region_name} has no regional-language Wikipedia in this project. Switch to **Events about**.")
        else:
            st.info(f"No stories located in **{region_name}** in this period. Switch to **Readers in**.")
        return

    if not S.country:
        who = "worldwide"
    elif state_readers(scope):
        who = f"read on {langs_text(region['langs'])} Wikipedia ({region_name})"
    elif S.mode == READERS:
        who = f"read by people in {cname[S.country]}"
    else:
        who = f"about {region_name or cname[S.country]}"
    if state_readers(scope):
        shared = len(LANG_STATES.get(region["langs"][0], [])) > 1
        how = (f"Spikes on {esc(langs_text(region['langs']))} Wikipedia, on any topic: the closest public signal "
               f"for readers in {esc(region_name)}<br>(Wikimedia gives reader locations only per country"
               + ("; this list is shared by every state that mainly reads it)" if shared else ")"))
    elif country_readers(scope):
        how = (f"What people in {esc(cname[S.country])} read far more than they normally do,<br>"
               "on any topic in the world (Wikimedia's daily per-country data)")
    elif scope.startswith("A:"):
        how = (f"Worldwide spikes whose subject is located in {esc(region_name or cname[S.country])}<br>"
               "(from Wikidata), whoever was reading")
    else:
        how = "Ranked by the jump above each article's own normal level,<br>weighted by the extra readers"
    what = "Top stories" if category == "ALL" else f"Top {category} stories"
    lang_txt = "" if not langs else f" · {LANG_NAMES.get(S.lang, S.lang)} Wikipedia"
    span = f"{S.as_of:%d %b %Y}" if len(days) == 1 else f"{min(days):%d %b} – {S.as_of:%d %b %Y}"
    st.markdown(f"<div class='pagehead'><div><h1>Attention overview</h1>"
                f"<div class='sub'>{what} {esc(who)}{lang_txt} · {span}"
                f"{' (detection starts ' + format(meta['detect_start'], '%d %b') + ')' if clipped else ''}</div></div>"
                f"<div class='fresh'>{how}</div></div>", unsafe_allow_html=True)

    if country_readers(scope) and not next((c["readers"] for c in countries if c["code"] == S.country), False):
        st.info("Wikimedia does not publish per-country data for this country (privacy). Switch to **Events about**.")
        return
    events = da.get_top_events(scope, days, category, langs, limit=None)
    if not events:
        st.info("No spikes match these filters. Try a longer time window or a wider location.")
        return
    cats = da.get_categories(tuple(e["event_id"] for e in events))
    for e in events:
        e["cat"] = cats.get(e["event_id"], "Other")
    top = events[:TABLE_ROWS]
    cards = da.get_cards(tuple(e["event_id"] for e in top))

    unit = "day" if country_readers(scope) else "h"     # per-country "Readers in" numbers are daily
    kpis(events, top, cards, unit)

    left, right = st.columns([2.2, 1], gap="medium")
    with left, st.container(border=True):
        daily_chart(scope, days, meta, langs)
    with right, st.container(border=True):
        category_chart(events)

    left, right = st.columns([2.2, 1], gap="medium")
    with left, st.container(border=True):
        chosen = stories_table(top, cards, unit)
    with right, st.container(border=True):
        detail_panel(chosen, cards.get(chosen["event_id"], {}), unit)

    left, right = st.columns([2.2, 1], gap="medium")
    with left, st.container(border=True):
        reader_map(tuple(e["event_id"] for e in events[:2000]))
    with right, st.container(border=True):
        language_chart(events)

    with st.expander("Trending now · Cassandra TTL table"):
        trending(scope)
    st.markdown("<div class='footer'><b>How to read this.</b> Every article is compared with its own normal level: the "
                "median of its previous 7 days of hourly views (worldwide) or 14 days of daily views (per country). A "
                "spike needs at least ×10 that level, a clear margin above its usual ups and downs and enough readers "
                "to matter; jumps above ×100 are shown as ×100. Related articles are grouped into one story through "
                "Wikidata. <b>Readers in</b> a country uses Wikimedia's per-country data; <b>Events about</b> a place "
                "uses Wikidata to locate the subject. Descriptions, pictures and news lines come from Wikipedia.</div>",
                unsafe_allow_html=True)
    if S.get("open_event"):
        story_dialog(S.pop("open_event"), scope, S.country, cname)


def kpis(events: list[dict], top: list[dict], cards: dict, unit: str) -> None:
    lead = events[0]
    by_cat = {}
    for e in events:
        by_cat[e["cat"]] = by_cat.get(e["cat"], 0) + 1
    busiest = max(by_cat, key=by_cat.get)
    n20 = top[:20]
    in_news = sum(1 for e in n20 if (cards.get(e["event_id"]) or {}).get("news"))
    peak = max(events, key=lambda e: e["peak_views"])
    tiles = [
        ("Stories detected", f"{len(events):,}", f"{sum(1 for e in events if e['suspicious'])} flagged as possible bots"),
        ("Top story", esc(lead["title"]), f"×{lead['ratio']:.0f} above normal · {fmt_short(lead['peak_views'])} views/{unit}"),
        ("Busiest category", busiest, f"{by_cat[busiest]:,} stories ({100 * by_cat[busiest] / len(events):.0f}%)"),
        ("Top 20 found in the news", f"{in_news} of {len(n20)}",
         f"Most read at peak: {esc(peak['title'][:28])} ({fmt_short(peak['peak_views'])}/{unit})"),
    ]
    st.markdown("<div class='kpis'>" + "".join(
        f"<div class='kpi'><div class='label'>{a}</div><div class='value' title='{b}'>{b}</div>"
        f"<div class='note'>{c}</div></div>" for a, b, c in tiles) + "</div>", unsafe_allow_html=True)


def daily_chart(scope: str, days: list[dt.date], meta: dict, langs) -> None:
    span = max(14, len(days))
    shown = [S.as_of - dt.timedelta(days=i) for i in range(span)][::-1]
    shown = [d for d in shown if d >= meta["detect_start"]]
    panel("New stories per day, by category", "Counted on the day each story was first detected · the selected "
          "window is solid, earlier days are faded")
    per_day = da.get_daily_category_counts(scope, tuple(shown))
    fig = go.Figure()
    for cat in CAT_ORDER:
        ys = [per_day[d].get(cat, 0) for d in shown]
        if not any(ys):
            continue
        fig.add_bar(name=cat, x=shown, y=ys, marker_color=CAT_COLORS[cat],
                    marker_opacity=[1.0 if d in days else 0.35 for d in shown], marker_line_width=0,
                    hovertemplate=f"{cat}: %{{y:,}}<extra>%{{x|%a %d %b}}</extra>")
    fig.update_layout(**plotly_layout(height=300, legend=True, bargap=0.35), barmode="stack")
    fig.update_layout(margin=dict(l=8, r=12, t=40, b=8), legend=dict(y=1.18, x=0, orientation="h"))
    fig.update_xaxes(tickformat="%d %b", tickangle=0, dtick=86400000.0 * (2 if len(shown) <= 14 else 4))
    st.plotly_chart(fig, width="stretch", config=CHART)
    if langs:
        st.caption("This chart counts stories in all languages (the counter table has no language split).")


def category_chart(events: list[dict]) -> None:
    panel("Stories by category", "In the current view · click a bar to filter")
    counts = {}
    for e in events:
        counts[e["cat"]] = counts.get(e["cat"], 0) + 1
    items = sorted(counts.items(), key=lambda kv: kv[1])
    fig = go.Figure(go.Bar(x=[n for _, n in items], y=[c for c, _ in items], orientation="h",
                           marker_color=[CAT_COLORS.get(c, CAT_COLORS["Other"]) for c, _ in items], marker_line_width=0,
                           text=[f"{n:,}" for _, n in items], textposition="outside", textfont=dict(color=TEXT),
                           cliponaxis=False, hovertemplate="%{y}: %{x:,} stories<extra></extra>"))
    fig.update_layout(**plotly_layout(height=300, bargap=0.45))
    fig.update_xaxes(showgrid=False, showticklabels=False)
    fig.update_yaxes(showgrid=False)
    ev = st.plotly_chart(fig, width="stretch", on_select="rerun", selection_mode="points",
                         key=f"catchart{S.chart_key}", config=CHART)
    pts = ev.selection.points if ev and ev.selection else []
    if pts:
        picked = pts[0].get("y")
        S.pending_category = "All" if picked == S.category else picked
        S.chart_key += 1
        st.rerun()


def stories_table(top: list[dict], cards: dict, unit: str) -> dict:
    panel("Top stories", f"Top {len(top)} by score · select a row to see it on the right")
    rows = []
    for i, e in enumerate(top, 1):
        c = cards.get(e["event_id"]) or {}
        rows.append({"#": i, "Story": ("⚠ " if e["suspicious"] else "") + e["title"],
                     "What it is": c.get("description") or "", "Category": e["cat"], "Jump": f"×{e['ratio']:.0f}",
                     f"Peak/{unit}": e["peak_views"], "Trend": e["sparkline"], "News": "✓" if c.get("news") else ""})
    sel = st.dataframe(rows, hide_index=True, on_select="rerun", selection_mode="single-row", height=430,
                       key=f"table|{S.country}|{S.mode}|{S.region}|{S.category}|{S.lang}|{S.window}|{S.as_of}",
                       width="stretch", column_config={
                           "#": st.column_config.NumberColumn(width=32),
                           "Story": st.column_config.TextColumn(width=165, help="⚠ = possible bot traffic"),
                           "What it is": st.column_config.TextColumn(width=132),
                           "Category": st.column_config.TextColumn(width=86),
                           "Jump": st.column_config.TextColumn(width=48, help="Peak views ÷ normal views (capped at ×100)"),
                           f"Peak/{unit}": st.column_config.NumberColumn(
                               width=62, format="compact",
                               help="Views on the busiest day among readers in this country" if unit == "day"
                               else "Views in the busiest hour"),
                           "Trend": st.column_config.LineChartColumn(width=74, y_min=0,
                                                                     help="48 h before to 48 h after the peak"),
                           "News": st.column_config.TextColumn(width=40, help="Found in Wikipedia's news pages"),
                       })
    picked = sel.selection.rows if sel and sel.selection else []
    return top[picked[0]] if picked else top[0]


def detail_panel(e: dict, card: dict, unit: str) -> None:
    flag = " <span class='warn'>⚠ possible bot traffic</span>" if e["suspicious"] else ""
    st.markdown(f"{thumb_html(card.get('thumbnail'), 160)}"
                f"<div style='margin-top:{10 if card.get('thumbnail') else 0}px'>{badge(e['cat'])}{flag}</div>"
                f"<div class='d-title'>{esc(e['title'])}</div>"
                f"<div class='d-desc'>{esc(card.get('description') or '')}</div>", unsafe_allow_html=True)
    n = card.get("news")
    if n:
        label = "Died" if n["kind"] == "death" else "In the news"
        st.markdown(f"<div class='news'><b>{label} · {n['date']:%d %b}</b> {esc(n['text'])}</div>",
                    unsafe_allow_html=True)
    elif card.get("extract"):
        x = card["extract"]
        st.markdown(f"<div class='extract'>{esc(x[:260])}{'…' if len(x) > 260 else ''}</div>", unsafe_allow_html=True)
    st.markdown("<div class='facts'>"
                f"<div class='fact'><div class='k'>Jump</div><div class='v'>×{e['ratio']:.0f}</div></div>"
                f"<div class='fact'><div class='k'>Peak / {'day' if unit == 'day' else 'hour'}</div><div class='v'>{fmt_short(e['peak_views'])}</div></div>"
                f"<div class='fact'><div class='k'>First seen</div><div class='v'>{e['first_seen']:%d %b}</div></div>"
                "</div>", unsafe_allow_html=True)
    if st.button("Open full story", key=f"open_{e['event_id']}", type="primary", width="stretch"):
        S.open_event = e["event_id"]
        st.rerun()


def reader_map(ids: tuple[str, ...]) -> None:
    panel("Where readers spiked", "Stories in view that spiked among readers in each country "
          "(Wikimedia's per-country data)")
    rows = da.get_country_counts(ids)
    if not rows:
        st.caption("No per-country spikes for these stories.")
        return
    try:
        import pycountry
        iso3 = {r["code"]: getattr(pycountry.countries.get(alpha_2=r["code"]), "alpha_3", None) for r in rows}
    except ImportError:
        iso3 = {}
    if iso3:
        fig = go.Figure(go.Choropleth(
            locations=[iso3[r["code"]] for r in rows], z=[r["stories"] for r in rows],
            text=[r["name"] for r in rows], colorscale=[[i / (len(SEQ_BLUE) - 1), c] for i, c in enumerate(SEQ_BLUE)],
            marker_line_color=SURFACE, marker_line_width=0.5,
            colorbar=dict(thickness=10, len=0.7, outlinewidth=0, tickfont=dict(color=TEXT)),
            hovertemplate="%{text}: %{z:,} stories<extra></extra>"))
        fig.update_geos(bgcolor=SURFACE, showframe=False, showcoastlines=False, showcountries=True,
                        countrycolor="#2B3440", showland=True, landcolor="#1F2731", projection_type="natural earth",
                        lataxis_range=[-58, 85])
        fig.update_layout(**plotly_layout(height=330))
        st.plotly_chart(fig, width="stretch", config=CHART)
    else:
        st.caption("Install pycountry to draw the map.")
    st.caption("Most active: " + " · ".join(f"{r['name']} {r['stories']:,}" for r in rows[:6]))


def language_chart(events: list[dict]) -> None:
    panel("Language editions", "Stories that spiked in each Wikipedia")
    counts = {}
    for e in events:
        for l in e["langs"]:
            counts[l] = counts.get(l, 0) + 1
    items = sorted(counts.items(), key=lambda kv: kv[1])
    fig = go.Figure(go.Bar(x=[n for _, n in items], y=[LANG_NAMES.get(l, l) for l, _ in items], orientation="h",
                           marker_color=PRIMARY, marker_line_width=0, text=[f"{n:,}" for _, n in items],
                           textposition="outside", textfont=dict(color=TEXT), cliponaxis=False,
                           hovertemplate="%{y}: %{x:,} stories<extra></extra>"))
    fig.update_layout(**plotly_layout(height=330, bargap=0.5))
    fig.update_xaxes(showgrid=False, showticklabels=False)
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(fig, width="stretch", config=CHART)


def trending(scope: str) -> None:
    rows = da.get_trending_now(scope)
    if not rows:
        st.caption("Empty: rows in trending_now expire 48 hours after the serving load (TTL). "
                   "Run `bash scripts/run_s6.sh --skip-series` in edge to refill it.")
        return
    st.dataframe([{"Story": r["title"], "Jump": f"×{r['ratio']:.0f}", "Score": round(r["score"]),
                   "Expires in": f"{r['ttl'] // 3600} h {r['ttl'] % 3600 // 60} min"} for r in rows],
                 hide_index=True, width="stretch")


# ---- full story ------------------------------------------------------------------
@st.dialog("Story", width="large")
def story_dialog(event_id: str, scope: str, country: str | None, cname: dict) -> None:
    e = da.get_event(event_id)
    if not e:
        st.warning("This story is not in MongoDB. Re-run the serving load.")
        return
    about = e.get("about") or {}
    loc = e.get("location") or {}
    place = ", ".join(x for x in [loc.get("city_name"), loc.get("region_name"), loc.get("country_name")] if x)
    if about.get("thumbnail"):
        left, right = st.columns([1, 2], gap="medium")
        left.markdown(thumb_html(about["thumbnail"], 180), unsafe_allow_html=True)
    else:
        right = st.container()
    with right:
        flag = " <span class='warn'>⚠ possible bot traffic</span>" if e.get("suspicious") else ""
        st.markdown(f"{badge(e['category'])}{flag}<div class='d-title'>{esc(e['title'])}</div>"
                    f"<div class='d-desc'>{esc(about.get('description') or '')}</div>"
                    f"<div class='muted'>First detected {fmt_time(e['first_detected'])}"
                    f"{' · ' + esc(place) if place else ''}</div>", unsafe_allow_html=True)
        if about.get("extract"):
            st.markdown(f"<div class='extract' style='margin-top:8px'>{esc(about['extract'])}</div>",
                        unsafe_allow_html=True)

    news = e.get("news") or []
    panel("Why people were reading", "Matched to Wikipedia's own news pages for those days")
    if news:
        for n in news:
            label = "Died" if n["kind"] == "death" else (n.get("section") or "News")
            topic = f"{esc(n['topic'])}: " if n.get("topic") else ""
            st.markdown(f"<div class='news'><b>{n['date']:%d %b} · {esc(label)}</b> {topic}{esc(n['text'])}</div>",
                        unsafe_allow_html=True)
    else:
        st.markdown("<div class='nonews'>Not on Wikipedia's news pages for those days. Spikes like this often come "
                    "from TV, social media or search trends rather than news.</div>", unsafe_allow_html=True)

    reader = country_readers(scope)
    cinfo = next((c for c in e.get("countries") or [] if c["code"] == country), None) if reader else None
    sinfo = (next((s for s in e.get("states") or [] if s["code"] == scope.split(":")[2]), None)
             if state_readers(scope) else None)
    if reader and cinfo:
        facts = [("Jump", f"×{cinfo['ratio']:.0f}"), ("Peak / day", fmt_short(cinfo["peak_views"])),
                 ("Normal / day", fmt_short(cinfo["peak_views"] / max(cinfo["ratio"], 1e-9)))]
    elif sinfo:
        facts = [(f"Jump · {LANG_NAMES.get(sinfo['lang'], sinfo['lang'])}", f"×{sinfo['ratio']:.0f}"),
                 ("Peak / hour", fmt_short(sinfo["peak_views"])), ("Normal / hour", fmt_short(sinfo["baseline"]))]
    else:
        facts = [("Jump", f"×{e['ratio']:.0f}"), ("Peak / hour", fmt_short(e["peak_views"])),
                 ("Normal / hour", fmt_short(e["baseline_views"]))]
    st.markdown("<div class='facts'>" + "".join(f"<div class='fact'><div class='k'>{k}</div><div class='v'>{v}</div></div>"
                                                for k, v in facts) + "</div>", unsafe_allow_html=True)
    if reader:
        country_chart(e, country)
    else:
        hourly_chart(e, sinfo["lang"] if sinfo else None)

    cs = sorted(e.get("countries") or [], key=lambda c: -c["ratio"])[:12]
    arts = e.get("articles") or []
    c1, c2 = st.columns([1.2, 1], gap="medium")
    with c1:
        if cs:
            panel("Where readers spiked", "Jump among readers in each country")
            fig = go.Figure(go.Bar(x=[c["ratio"] for c in cs][::-1], y=[c.get("name", c["code"]) for c in cs][::-1],
                                   orientation="h", marker_line_width=0,
                                   marker_color=[PRIMARY if c["code"] == country or not country else NEUTRAL
                                                 for c in cs][::-1],
                                   hovertemplate="%{y}: ×%{x:.1f}<extra></extra>"))
            fig.update_layout(**plotly_layout(height=max(120, 30 + 22 * len(cs))))
            st.plotly_chart(fig, width="stretch", config=CHART)
    with c2:
        if arts:
            panel("Articles in this story")
            st.markdown(" ".join(f"<a class='artchip' href='{wiki_url(a['lang'], a['title'])}' target='_blank'>"
                                 f"<b>{a['lang']}</b>{esc(a['title'])} ↗</a>" for a in arts[:16]), unsafe_allow_html=True)
    lead = e.get("lead") or {}
    if lead.get("title"):
        st.link_button("Open on Wikipedia ↗",
                       wiki_url(about.get("lang") or lead["lang"], about.get("title") or lead["title"]))


def hourly_chart(e: dict, lang: str | None = None) -> None:
    """Hourly views of the lead article, or of the story's article on `lang` Wikipedia."""
    lead = e.get("lead") or {}
    art = next((a for a in e.get("articles") or [] if a["lang"] == lang), None) if lang else None
    if art:
        lead = {"lang": art["lang"], "title": art["title"]}
    if not lead.get("title") or not e.get("articles"):
        st.caption("No hourly series: this story was only seen in per-country data.")
        return
    peak = e["peak_time"]
    pts = da.get_article_hourly(lead["lang"], lead["title"], peak - dt.timedelta(hours=24), peak + dt.timedelta(hours=24))
    if not pts:
        st.caption("No hourly series stored for this article.")
        return
    panel(f"Hourly views of {esc(lead['title'])}", f"{LANG_NAMES.get(lead['lang'], lead['lang'])} Wikipedia · 24 h "
          "either side of the peak · dashed line = its normal level (median of the previous 7 days)")
    t = [p["t"] for p in pts]
    v = [p["views"] for p in pts]
    b = [p["baseline"] for p in pts]
    custom = [f"{x:%d %b %H:%M} UTC · {x + dt.timedelta(hours=5, minutes=30):%H:%M} IST" for x in t]
    fig = go.Figure()
    fig.add_bar(x=t, y=v, name="Views", customdata=list(zip(custom, [bb or 0 for bb in b])), marker_line_width=0,
                marker_color=[PRIMARY if (bb is not None and vv > bb) else NEUTRAL for vv, bb in zip(v, b)],
                hovertemplate="%{customdata[0]}<br>%{y:,} views · normal %{customdata[1]:,}<extra></extra>")
    fig.add_scatter(x=t, y=b, mode="lines", name="Normal", line=dict(color=BASELINE, dash="dash", width=1.5),
                    hoverinfo="skip")
    fig.update_layout(**plotly_layout(height=240))
    st.plotly_chart(fig, width="stretch", config=CHART)


def country_chart(e: dict, country: str) -> None:
    qid = da.lead_qid(e)
    meta = da.get_meta()
    start = max(meta["country_start"], e["first_detected"].date() - dt.timedelta(days=14))
    end = min(meta["as_of"], e["peak_time"].date() + dt.timedelta(days=4))
    pts = da.get_article_country_daily(qid, country, start, end) if qid else []
    if not pts:
        st.caption("No daily series stored for this story in this country.")
        return
    panel("Daily views among readers in this country", "All languages together · days under 90 views are not "
          "published and show as 0 · dashed line = normal level (median of the previous 14 days)")
    d = [p["day"] for p in pts]
    v = [p["views"] for p in pts]
    b = [p["baseline"] for p in pts]
    fig = go.Figure()
    fig.add_bar(x=d, y=v, marker_line_width=0, hovertemplate="%{x|%d %b}: %{y:,} views<extra></extra>",
                marker_color=[PRIMARY if (bb is not None and vv > bb) else NEUTRAL for vv, bb in zip(v, b)])
    fig.add_scatter(x=d, y=b, mode="lines", line=dict(color=BASELINE, dash="dash", width=1.5), hoverinfo="skip")
    fig.update_layout(**plotly_layout(height=240))
    st.plotly_chart(fig, width="stretch", config=CHART)
