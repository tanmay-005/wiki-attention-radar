# 01 · Product Requirements Document (PRD)

**Project:** Wikipedia Attention Radar
**Owner:** Tanmay Rana
**Course:** Big Data Analytics
**Target completion:** mid-November 2026
**Status:** Planning

---

## 1. Summary

Wikipedia Attention Radar processes Wikipedia's public pageview data, billions of rows, to automatically detect **real-world events** from sudden spikes in reading activity. Users explore those events on a dashboard that opens on a **global view** and can be narrowed by **country**, **state/region**, **category**, **language** and **time window**.

## 2. Problem

1. **People care about what is relevant to them.** A reader in Jaipur cares more about a Rajasthan budget or an IPL auction than a local election in another country. A single global "trending" list is not useful to most people.
2. **Existing "trending" signals are closed or biased.** News sites and social platforms decide what is important through editors or opaque algorithms, and their data is often paywalled or API-restricted.
3. **Raw popularity is misleading.** Some pages (cricket, celebrities, countries) are always popular. What matters is **unusual** attention compared with that page's own normal level, in that country and that language.
4. **The data is too large for ordinary tools.** One hour of pageview data has millions of rows; a few weeks has billions. This needs distributed storage and processing.

## 3. Solution

An open, data-driven attention monitor:

- **Detects** spikes against per-article, per-country and per-language baselines.
- **Groups** related spikes into events.
- **Labels** events with a category and a location (country → state → city).
- **Serves** results through a filterable dashboard backed by NoSQL stores built for these query patterns.

## 4. Users and use cases

| User | Need | Example |
|---|---|---|
| Curious reader | What is the world / my country talking about right now? | Opens dashboard → sees global spikes → switches to India |
| Student / researcher | Compare attention across countries and languages; study how fast interest fades | Politics + Japan vs Politics + India for the same week |
| Journalist | Early signal of breaking stories in a specific region | Events about Assam, Disasters, last 24 hours |
| Examiner / evaluator | See a complete Big Data pipeline working end to end | Live demo plus benchmark results |

## 5. Goals and non-goals

### Goals

- G1. Process at least 2 weeks of hourly pageview data (target: billions of rows) through the full Hadoop → Hive → Spark → NoSQL pipeline.
- G2. Detect spikes at **global/language** level (hourly) and **country** level (daily).
- G3. Group spikes into events and label them with category and location.
- G4. Dashboard with stacked filters: location (global / country / state), mode (Readers in / Events about), category, language, time window.
- G5. Cover every syllabus unit naturally (see section 10).
- G6. Measure detection quality against a list of known real events.
- G7. Benchmark key design choices (combiner, compression, file formats, Spark vs MapReduce).

### Non-goals

- Real-time streaming (Kafka, Spark Streaming). Data is processed in **batch**; the design allows hourly re-runs.
- Reader location below country level. Wikimedia does not publish it, for privacy reasons. State and city filters work only in **Events about** mode.
- Explaining **why** something spiked using news text. The system detects and groups spikes; it does not read news.
- User accounts, login or personalisation stored per user.
- Public cloud deployment. The project runs locally in Docker.

## 6. Functional requirements

### 6.1 Data ingestion

| ID | Requirement | Priority |
|---|---|---|
| FR-1 | Download hourly pageview dump files for a configurable date range | Must |
| FR-2 | Download daily per-country pageview files for a configurable date range | Must |
| FR-3 | Store raw files in HDFS in a date-partitioned layout | Must |
| FR-4 | Resume interrupted downloads and skip files already present | Should |

### 6.2 Processing

| ID | Requirement | Priority |
|---|---|---|
| FR-5 | MapReduce job filters to configured languages and content pages, normalises keys, merges desktop and mobile | Must |
| FR-6 | MapReduce job aggregates hourly → daily views per article | Must |
| FR-7 | MapReduce jobs demonstrate combiner, custom partitioner, compression, sorting (top-N) and searching | Must |
| FR-8 | Hive external tables over raw and processed data, with RegexSerDe, partitions, ORC and Parquet | Must |
| FR-9 | Hive UDF to normalise article titles | Must |
| FR-10 | Spark computes robust baselines and detects spikes per (language, article, hour) | Must |
| FR-11 | Spark detects spikes per (country, article, day) | Must |
| FR-12 | Spark merges consecutive spike hours into spike episodes | Must |
| FR-13 | Enrich spiking articles with Wikidata ID, category and location (country, ISO 3166-2 region, city) | Must |
| FR-14 | Group spikes into events (same entity, linked entities within a time window) | Must |
| FR-15 | Compute an event score that ranks big jumps on meaningful volume | Must |
| FR-16 | Flag suspicious, likely automated spikes | Should |

### 6.3 Serving

| ID | Requirement | Priority |
|---|---|---|
| FR-17 | Cassandra stores event rankings per scope and day, article time series, a TTL "trending now" table and counter tables | Must |
| FR-18 | MongoDB stores full event documents and a page-metadata cache | Must |
| FR-19 | Import/export demonstrated for both databases (`COPY`, `mongoimport`/`mongoexport`) | Must |

### 6.4 Dashboard

| ID | Requirement | Priority |
|---|---|---|
| FR-20 | Opens on **Global · All categories · All languages · last 24 hours** | Must |
| FR-21 | Country selector; choosing a country shows that country's top spikes regardless of category | Must |
| FR-22 | Category filter works in global and country scope | Must |
| FR-23 | Language filter works with any other filter | Must |
| FR-24 | State/region selector; selecting one switches mode to **Events about** | Must |
| FR-25 | Time window: 24 hours, 7 days, 30 days (bounded by loaded data) | Must |
| FR-26 | Ranked spike list with jump size, category, location, languages, time and a sparkline | Must |
| FR-27 | Detail panel: views chart with normal-level line, readers-by-country bars, grouped articles | Must |
| FR-28 | Category distribution chart for the current filters; clicking a bar filters by it | Should |
| FR-29 | A plain-language "Showing: ..." sentence describing the active filters, plus Reset | Must |
| FR-30 | Validation page showing detected vs known events | Should |
| FR-31 | Benchmarks page showing measured results | Could |

## 7. Non-functional requirements

| ID | Requirement |
|---|---|
| NFR-1 | **Scale:** final run processes ≥ 14 days of hourly data (≈ 2 billion raw rows) |
| NFR-2 | **Dashboard latency:** any filter change returns in < 1 s on the loaded dataset |
| NFR-3 | **Reproducibility:** one config file and scripted steps rebuild everything from raw files |
| NFR-4 | **Laptop-friendly:** runs on 16 GB RAM (8 GB with services started in groups) and ~60 GB free disk for the final run |
| NFR-5 | **Correctness:** row-count and view-sum conservation checks pass between pipeline stages |
| NFR-6 | **Privacy:** only aggregated public data; no personal data; Wikimedia API etiquette respected |
| NFR-7 | **Documentation:** every stage documented; README lets an evaluator run the dev slice |

## 8. Data scope

| Dataset | Period (default) | Purpose |
|---|---|---|
| Hourly pageviews | 2024-05-25 → 2024-06-14 (21 days) | 7 days of baseline + 14 days of detection |
| Per-country daily pageviews | 2024-05-15 → 2024-06-14 | 14 days of baseline + detection days |
| Dev slice | 2 days of hourly + 3 days of country | Fast iteration |

**Why June 2024:** the window contains many well-documented events that can be used for validation, for example the Indian general election counting day, major T20 World Cup matches, European Parliament elections and a big tech conference. The final validation list is built and date-checked in Phase 7.

**Languages (default):** English, Hindi, Tamil, Japanese, Portuguese. Configurable in `config/settings.yaml`.

## 9. Success metrics

| Metric | Target |
|---|---|
| Rows processed in final run | ≥ 1.5 billion raw rows |
| Known-event recall (detected / known) | ≥ 75% |
| Precision@50 (top 50 events manually labelled as real events) | ≥ 80% |
| Dashboard filter response | < 1 s |
| Combiner shuffle reduction | Measured and reported |
| Spark vs MapReduce speed-up on the same aggregation | Measured and reported |
| ORC vs Parquet vs text storage and query time | Measured and reported |

## 10. Syllabus coverage

| Unit | Where it appears |
|---|---|
| Introduction to Big Data | Unstructured text dumps → structured tables; 3Vs plus veracity (bots, noise, privacy noise) |
| Big Data Analytics | Descriptive (top articles), diagnostic (what spiked, where), anomaly detection; CAP and BASE via the Cassandra and MongoDB choices |
| NoSQL / NewSQL | Wide-column vs document store chosen per access pattern; SQL vs NoSQL vs NewSQL comparison in the report |
| Hadoop and HDFS | Raw zone, block size, replication, `fsck`, NameNode/DataNode UIs, Hadoop vs RDBMS justification |
| MapReduce | Mapper, reducer, combiner, partitioner, sorting, searching, compression |
| YARN | All MapReduce jobs (and optionally Spark) run on YARN; ResourceManager/NodeManager/ApplicationMaster |
| Apache Spark | RDD transformations/actions, lazy evaluation, DataFrames, Spark SQL, window functions, Spark vs MapReduce |
| MongoDB | Documents, data types, MQL, CRUD, aggregation, indexes, import/export |
| Cassandra | Keyspace, data types, CQLSH, CRUD, collections, counters, TTL, ALTER, import/export, system tables |
| Hive | Architecture, data types, file formats, HQL, SerDe (Regex, CSV), UDF, HDFS integration |

## 11. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Laptop runs out of RAM or disk | Pipeline cannot finish | Dev slice first; compose profiles; filter languages early; lite config (10 days) |
| Docker networking problems between Spark and HDFS | Jobs fail | All jobs run inside the Docker network from the `edge` container |
| Wikidata API rate limits | Enrichment stalls | Batch 50 IDs per call, cache in MongoDB, polite User-Agent, retry with back-off |
| Per-country data suppresses low counts | Missing days look like zero | Impute missing days with 0 and use a baseline floor equal to the release threshold |
| Spikes caused by bots | False events | Suspicious-spike heuristic; report precision honestly |
| Time pressure | Incomplete demo | Phased plan with "Must" items first; dev slice keeps the demo always runnable |

## 12. Deliverables

1. Source code repository with all documents
2. Working Docker-based pipeline (dev and full configs)
3. Streamlit dashboard
4. Benchmark results and validation report
5. Project report and presentation
