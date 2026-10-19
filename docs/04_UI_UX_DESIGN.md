# 04 · UI / UX Design

The dashboard is a **Streamlit** app. A clickable mock-up of the main screen exists as a design reference (with sample data). This document is the specification the real app must match.

---

## 1. Design principles

1. **Open on the big picture, narrow on demand.** Default = Global · All categories · All languages · last 24 hours.
2. **Filters stack.** Every filter starts at "All"; each change narrows the result. Any combination is valid.
3. **Always say what is shown.** A plain sentence ("Top Politics spikes read by people in India · Hindi · last 7 days") sits above the results.
4. **Be honest about the data.** "Readers in" is country-level only. State and city filters switch to "Events about", and the UI says so.
5. **Jump, not popularity.** Every number shown is relative to that page's own normal level in that scope.

## 2. Pages

| Page (sidebar) | Purpose | Priority |
|---|---|---|
| **Radar** | Main dashboard: filters, ranked spikes, detail panel, category chart | Must |
| **Validation** | Known events vs detected events, recall and precision@50 | Should |
| **Benchmarks** | Charts of measured results (combiner, compression, formats, Spark vs MR) | Could |
| **About** | What the project is, data sources, method, limitations | Must |

## 3. Radar page layout

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  ◎ Attention Radar                         Data: 2024-05-25 → 2024-06-14 (UTC)│
│  What the world is suddenly reading about on Wikipedia                        │
├──────────────────────────────────────────────────────────────────────────────┤
│ FILTER BAR                                                                    │
│ Country [Global ▼]  State/region [— ▼]  Measure by (Readers in|Events about)  │
│ Language [All ▼]    Time window (24h | 7d | 30d)    As of [2024-06-04 ▼]      │
│ Category ( All )( Politics )( Sports )( Entertainment )( Science & Tech )...  │
│ ⓘ hint line explaining the current mode                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│ Showing: Top spikes worldwide · All languages · last 24 hours   [Reset]       │
├───────────────┬───────────────┬───────────────┬──────────────────────────────┤
│ Spikes in view│ Biggest jump  │ Top category  │ Scope                         │
├───────────────┴───────────────┴──────┬────────┴──────────────────────────────┤
│ TOP SPIKES (ranked)                  │ DETAIL PANEL (selected event)          │
│ 1  Title ............  ▂▃▇█▅▃  ×42   │ [Politics] First detected 4 Jun 08:00  │
│    Category · Place · Langs · 5h ago │ Title                                  │
│ 2  ...                               │ ×38 above normal · worldwide           │
│ ...                                  │ Normal 10,800/hr · Peak 412,000/hr     │
│                                      │ [views chart with dashed normal line]  │
│                                      │ Where readers spiked (by country) bars │
│                                      │ Articles grouped into this event       │
│                                      │ [Open on Wikipedia ↗]                  │
├──────────────────────────────────────┴───────────────────────────────────────┤
│ SPIKES BY CATEGORY (bars, click to filter)                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│ Footer: how a spike is measured · Readers in vs Events about                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

On narrow screens Streamlit stacks the columns: list first, then detail.

## 4. Filters: behaviour rules

| Control | Options | Default | Rules |
|---|---|---|---|
| Country | Global + every country present in the data (sorted, India pinned near top) | Global | Changing country resets State to "All" and Mode to "Readers in" |
| State / region | "All states / regions" + regions with events (`region_index`) | Disabled when Global | Picking a state sets Mode = **Events about** automatically |
| Measure by | Readers in · Events about | Readers in | Disabled when Global. Switching to Readers in resets State to All |
| Language | All + configured languages | All | Keeps events that spiked in that language edition |
| Time window | 24 hours · 7 days · 30 days | 24 hours | Counted back from "As of" day; clipped to loaded data |
| As of | Any detection day in the data | Last day of data | Needed because the data is historical, not live |
| Category | All + 6 categories + Other | All | Chip; also settable by clicking the category chart |
| Reset | — | — | Restores all defaults |

### Filter → query mapping

| Selection | `scope` passed to `get_top_events` |
|---|---|
| Global | `G` |
| India + Readers in | `R:IN` |
| India + Events about | `A:IN` |
| India + Rajasthan | `A:IN:IN-RJ` |

`category` → `event_rank.category` ('ALL' or name). `language` → filter on `langs`. Time window → list of `day` partitions.

### The five core user scenarios (acceptance)

| # | User does | Must see |
|---|---|---|
| 1 | Opens the dashboard | Top global spikes, all categories |
| 2 | Country = India | Top spikes among readers in India, any category |
| 3 | Global + Politics | Top political spikes worldwide |
| 4 | India + Politics | Top political spikes among readers in India |
| 5 | India + Rajasthan (+ any category / language) | Events located in Rajasthan; mode shows Events about |

## 5. Components

### 5.1 Summary tiles

| Tile | Value |
|---|---|
| Spikes in view | Count of events in the current list |
| Biggest jump vs normal | Highest `ratio` (×) |
| Most active category | Mode of categories in the list |
| Scope | "Worldwide" / "Readers in India" / "Events in Rajasthan" |

### 5.2 Spike list row

- Rank, title (one line, truncated), meta line: `Category · Place · Languages`, a 12-point sparkline, jump (`×42`, large, accent colour), time since first detection.
- A row marked suspicious shows a small "possible automated traffic" tag.
- Clicking a row selects it; the selected row has an accent border and tinted background.
- Ranking uses **score** (jump weighted by volume). A tooltip on the column header explains this.

### 5.3 Detail panel

| Part | Content | Source |
|---|---|---|
| Header | Category chip, "First detected", title, location, languages | Mongo `events` |
| Big number | `×ratio above normal` + scope phrase; normal vs peak | Mongo `events` |
| Chart | **Global:** hourly views ±24 h around the peak, from `article_hourly` for the lead article. **Readers in country:** daily views over the baseline window + detection days, from `article_country_daily`. Dashed line = baseline | Cassandra |
| Where readers spiked | Horizontal bars of per-country ratio; the selected country highlighted | Mongo `events.countries` |
| Articles grouped | Chips with language tag; each links to Wikipedia | Mongo `events.articles` |

### 5.4 Category chart

Horizontal bars of spike counts per category for the current location, language and time (from `spike_counts`). Clicking a bar sets the Category filter; clicking the active bar clears it.

### 5.5 Empty and edge states

| Situation | Message |
|---|---|
| No events for filters | "No spikes match these filters. Try a longer time window or a wider location." |
| Country with no published data | "Wikimedia does not publish per-country data for this country (privacy). Try Events about." |
| State selected but no regions | Dropdown shows "No regions with events" and is disabled |
| Database unreachable | Red banner: "Cannot reach Cassandra/MongoDB. Is Docker running?" plus a retry button |
| Time window beyond data | Note: "Showing 2024-06-01 → 2024-06-04 (data starts 2024-06-01)" |

## 6. Visual system

| Token | Value | Use |
|---|---|---|
| Background | `#F5F4EF` | Page |
| Surface | `#FFFFFF` + border `#E2E0D8`, radius 14 px | Cards |
| Ink | `#15171C` | Text, active chips |
| Muted | `#5B606B` | Labels, meta (passes 4.5:1 on white) |
| Accent (fill) | `#EA580C` | Bars above normal, selection border |
| Accent (text) | `#B4410C` | ×ratio numbers (passes 4.5:1 on white) |
| Neutral bars | `#C9C5BA` | Bars at or below normal |
| Font | IBM Plex Sans (UI), IBM Plex Mono (numbers) | |

Charts use **Plotly** with the same colours: no gridline clutter, a dashed baseline and hover tooltips showing time (UTC + IST), views and baseline.

Streamlit theme (`.streamlit/config.toml`):

```toml
[theme]
base = "light"
primaryColor = "#EA580C"
backgroundColor = "#F5F4EF"
secondaryBackgroundColor = "#FFFFFF"
textColor = "#15171C"
font = "sans serif"
```

## 7. Accessibility

- All filters are real form controls with labels (Streamlit default).
- Colour is never the only signal: the jump is also written as text (×42), and the suspicious flag uses text.
- Text contrast is at least 4.5:1; numbers use a monospaced font for alignment.
- Times are shown in UTC with IST in the tooltip, so readers in India are not confused.

## 8. Validation page

- Table: known event · date · expected articles · detected? (✓/✗) · matched event · rank in its scope · first detected (hours after the real event).
- Metrics: recall, precision@50 (from manual labels stored in `validation`), median detection delay.
- A chart for each case study (3–4 events, including Indian ones).

## 9. Benchmarks page

Bar charts read from `benchmarks/results/*.csv`:

1. MapReduce with vs without combiner: shuffle bytes and runtime
2. Map output compression off / Snappy / Gzip
3. Text vs ORC vs Parquet: storage size and query time
4. Same daily aggregation: MapReduce vs Spark
5. RDD vs DataFrame for the same aggregation

## 10. Differences from the mock-up

| Mock-up | Real app |
|---|---|
| Sample data inside the page | Live queries to Cassandra/MongoDB |
| Hourly chart in every scope | Hourly for Global/Events about; **daily** for Readers in (per-country data is daily) |
| No "As of" control | "As of" date picker, because the data is historical |
| One page | Radar, Validation, Benchmarks, About |
