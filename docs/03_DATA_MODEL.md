# 03 · Data Model (HDFS, Hive, Cassandra, MongoDB)

This document defines every place data is stored, with exact schemas.

---

## 1. Source data formats

### 1.1 Hourly pageview dumps

- URL pattern: `https://dumps.wikimedia.org/other/pageviews/<YYYY>/<YYYY-MM>/pageviews-<YYYYMMDD>-<HH>0000.gz`
- One gzip file per hour (UTC). Each line is space-separated:

```
domain_code page_title count_views total_response_size
```

| Field | Meaning | Notes |
|---|---|---|
| `domain_code` | Project + platform | `en` = English Wikipedia desktop, `en.m` = English Wikipedia mobile web. Other suffixes are other projects (Wiktionary, Wikibooks, ...) and are dropped |
| `page_title` | Article title | Underscores instead of spaces; may be URL-encoded; namespaces like `Special:` or `File:` appear and are dropped |
| `count_views` | Views in that hour | Integer |
| `total_response_size` | Legacy field | Ignored (typically 0) |

**Confirmed on real data (step 1.4, file `pageviews-20240603-000000.gz`):**
- Every line has exactly 4 space-separated fields; `total_response_size` is always `0`.
- Lines are **sorted by domain code** (the file starts with `""`, `ab`, `ar`, ...; `en` comes much later).
- Besides `xx` (desktop) and `xx.m` (mobile), other domain codes exist (`commons.m`, `commons.m.m`, `ar.b`, ...). Only `<lang>` and `<lang>.m` for the configured languages are kept.
- **Namespaces are localised** on non-English wikis (Arabic `تصنيف` = Category, Russian `Файл` = File). The content-page filter therefore uses English canonical names **plus** localised names for hi, ta, ja and pt (list in `mapreduce/common_title.py`).
- The 2-day dev slice is 48 files, **2.6 GB** compressed in HDFS.

### 1.2 Per-country daily pageviews (differential privacy)

- Home: `https://analytics.wikimedia.org/published/datasets/country_project_page/`
- One file per day, **zero-padded, tab-separated**: `YYYY-MM-DD.tsv` (e.g. `2024-06-03.tsv`), about **24 MB / 410,000 rows** per day, available from 2023-02-06.
- **No header row. 7 columns** (confirmed in step 1.4; the dataset README lists 6 and calls the files `.csv`, but the published files differ):

| # | Column | Example | Meaning |
|---|---|---|---|
| 1 | `country_name` | `India` | Reader country, full name |
| 2 | `country_code` | `IN` | ISO 3166-1 alpha-2, used for scopes like `R:IN` |
| 3 | `project` | `en.wikipedia` | Wiki project (all languages and projects are included, so we filter) |
| 4 | `page_id` | `121279` | Numeric page ID (unique together with project) |
| 5 | `page_title` | `Comechingones` | Title (may be a redirect name) |
| 6 | `item_id` | `Q1113975` | **Wikidata QID**, which makes enrichment much easier |
| 7 | `views` | `141` | Noisy daily pageview count (differential privacy) |

- Rows are published only above a threshold (> 90 views for lower-risk countries; higher for medium/high-risk countries), which matches the smallest values seen in the file.

---

## 2. HDFS layout

```
/radar
├── raw
│   ├── hourly/dt=2024-06-03/pageviews-20240603-000000.gz ... -230000.gz
│   └── country_daily/dt=2024-06-03/2024-06-03.tsv
├── staged
│   ├── hourly/dt=2024-06-03/part-r-00000.gz ...     # MR1 output
│   ├── daily/dt=2024-06-03/part-r-00000.gz ...      # MR2 output
│   ├── topn/dt=2024-06-03/...                       # MR3 output
│   └── search/<title>/...                           # MR4 output
├── warehouse                                         # Hive managed tables (ORC/Parquet)
│   └── radar.db/...
├── output
│   ├── series_global/          # S1 dense hourly series (candidates only), Parquet
│   ├── spikes_global/          # S2 spike episodes, Parquet
│   ├── spikes_country/         # S3 spike episodes, Parquet
│   ├── page_meta/              # S4 enrichment, Parquet
│   └── events/                 # S5 events, Parquet
└── benchmarks/                 # outputs of benchmark runs
```

HDFS settings (single DataNode lab cluster): block size 128 MB (default), replication **1** (only one DataNode). In the report, explain that production uses 3.

---

## 3. Hive (database `radar`)

### 3.1 Raw hourly (external, RegexSerDe)

```sql
CREATE EXTERNAL TABLE radar.raw_hourly (
  domain_code STRING,
  page_title  STRING,
  views       BIGINT,
  resp_size   BIGINT
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = "^(\\S+) (\\S+) (\\d+) (\\d+)$")
STORED AS TEXTFILE
LOCATION '/radar/raw/hourly';
-- MSCK REPAIR TABLE radar.raw_hourly;   -- registers dt= partitions
-- Hour comes from the virtual column INPUT__FILE__NAME
```

> RegexSerDe returns columns as strings in some Hive versions. If `views` comes back NULL, declare all columns `STRING` and `CAST` in queries.

### 3.2 Raw country daily (external)

```sql
CREATE EXTERNAL TABLE radar.raw_country_daily (
  country_name STRING, country_code STRING, project STRING, page_id BIGINT,
  page_title STRING, item_id STRING, views BIGINT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/raw/country_daily';
-- no header row, so no skip.header.line.count
```

### 3.3 Staged (external over MapReduce output)

```sql
CREATE EXTERNAL TABLE radar.staged_hourly (
  lang STRING, platform STRING, page_title STRING, hour_ts STRING, views BIGINT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/radar/staged/hourly';

CREATE EXTERNAL TABLE radar.staged_daily (
  lang STRING, page_title STRING, day STRING, views BIGINT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/radar/staged/daily';
```

### 3.4 Warehouse tables (managed, columnar)

```sql
CREATE TABLE radar.hourly_views (
  lang STRING, page_title STRING, hour_ts TIMESTAMP,
  views_desktop BIGINT, views_mobile BIGINT, views BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE radar.daily_views (
  lang STRING, page_title STRING, day DATE, views BIGINT
)
PARTITIONED BY (dt STRING) STORED AS ORC;

CREATE TABLE radar.country_daily (
  country STRING,            -- ISO code (IN, US, ...)
  country_name STRING,
  lang STRING,               -- from project: en.wikipedia -> en (configured languages only)
  page_id BIGINT, page_title STRING,
  qid STRING, views BIGINT
)
PARTITIONED BY (dt STRING) STORED AS ORC;

-- Format comparison copies (benchmarks only)
CREATE TABLE radar.hourly_views_parquet STORED AS PARQUET AS SELECT * FROM radar.hourly_views;
CREATE TABLE radar.hourly_views_text   STORED AS TEXTFILE AS SELECT * FROM radar.hourly_views;
```

`lang` is derived from `domain_code` (`en.m` → `en`, platform `mobile`). Titles are normalised by the UDF `normalize_title` (section 3.6).

### 3.5 Spark outputs registered in Hive (optional, for HQL analysis)

```sql
CREATE EXTERNAL TABLE radar.spikes_global STORED AS PARQUET LOCATION '/radar/output/spikes_global';
CREATE EXTERNAL TABLE radar.events        STORED AS PARQUET LOCATION '/radar/output/events';
```

(Parquet external tables can be declared with explicit columns; see `hive/04_outputs.hql`.)

### 3.6 UDFs

| Function | Type | Behaviour |
|---|---|---|
| `normalize_title(s)` | Java GenericUDF | URL-decode, `_` → space, trim, collapse spaces, cap length at 255 |
| `is_content_page(title)` | Java GenericUDF | false for namespaces (`Special:`, `File:`, `Category:`, `Talk:`, `User:`, `Wikipedia:`, `Template:`, `Help:`, `Portal:`, `Draft:`, `Module:`), `Main_Page`, `-` |

Hive data types used: `STRING, BIGINT, DATE, TIMESTAMP`; complex types are demonstrated in `hive/03_analysis.hql` (`ARRAY<STRING>` via `collect_set`, `MAP<STRING,BIGINT>` via `str_to_map`).

---

## 4. Cassandra (keyspace `radar`)

### 4.1 Keyspace

```sql
CREATE KEYSPACE IF NOT EXISTS radar
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

### 4.2 Tables (query-first design)

**Q1. Ranked events for a scope, day and category** (main list)

```sql
CREATE TABLE radar.event_rank (
  scope       text,          -- 'G' | 'R:IN' | 'A:IN' | 'A:IN:IN-RJ'
  day         date,          -- UTC day of first detection
  category    text,          -- 'ALL' or a real category (each event written twice)
  score       double,
  event_id    text,
  title       text,
  ratio       double,        -- jump vs normal (×)
  peak_views  bigint,
  first_seen  timestamp,
  langs       set<text>,     -- collection: languages where it spiked
  place       text,          -- "Rajasthan, India"
  sparkline   list<int>,     -- collection: 12 points
  suspicious  boolean,
  PRIMARY KEY ((scope, day), category, score, event_id)
) WITH CLUSTERING ORDER BY (category ASC, score DESC, event_id ASC);
```

Query: `SELECT * FROM radar.event_rank WHERE scope=? AND day=? AND category=? LIMIT 50;`

**Q2. Hourly series for one article** (detail chart, global scope)

```sql
CREATE TABLE radar.article_hourly (
  lang     text,
  title    text,
  hour     timestamp,
  views    int,
  baseline int,
  PRIMARY KEY ((lang, title), hour)
) WITH CLUSTERING ORDER BY (hour ASC);
```

**Q3. Daily series for one entity in one country** (detail chart, country scope)

```sql
CREATE TABLE radar.article_country_daily (
  qid      text,
  country  text,
  day      date,
  views    int,
  baseline int,
  PRIMARY KEY ((qid, country), day)
);
```

**Q4. Trending now** (TTL demonstration)

```sql
CREATE TABLE radar.trending_now (
  scope    text,
  score    double,
  event_id text,
  title    text,
  ratio    double,
  PRIMARY KEY ((scope), score, event_id)
) WITH CLUSTERING ORDER BY (score DESC, event_id ASC)
  AND default_time_to_live = 172800;          -- 48 h
-- Individual rows can override: INSERT ... USING TTL 3600;
```

**Q5. Spike counts per category** (counter demonstration)

```sql
CREATE TABLE radar.spike_counts (
  scope    text,
  day      date,
  category text,
  spikes   counter,
  PRIMARY KEY ((scope, day), category)
);
-- UPDATE radar.spike_counts SET spikes = spikes + 1 WHERE scope='G' AND day='2024-06-04' AND category='Politics';
```

**Q6. Regions available per country** (state dropdown)

```sql
CREATE TABLE radar.region_index (
  country      text,
  region_code  text,     -- ISO 3166-2, e.g. IN-RJ
  region_name  text,
  event_count  int,
  PRIMARY KEY ((country), region_code)
);
```

**ALTER demonstration** (run once, documented in the report):

```sql
ALTER TABLE radar.event_rank ADD lead_qid text;
```

**System tables queried** for the report: `system_schema.keyspaces`, `system_schema.tables`, `system_schema.columns`, `system.local`, `system.size_estimates`.

---

## 5. MongoDB (database `radar`)

### 5.1 `events`

```json
{
  "_id": "evt_20240604_Q123456",
  "title": "2024 Indian general election",
  "category": "Politics",
  "first_detected": {"$date": "2024-06-04T03:00:00Z"},
  "peak_time":      {"$date": "2024-06-04T08:00:00Z"},
  "score": 812.4,
  "ratio": 38.2,
  "peak_views": 412000,
  "baseline_views": 10800,
  "langs": ["en", "hi"],
  "location": {
    "country": "IN", "country_name": "India",
    "region": "IN-DL", "region_name": "Delhi",
    "city_qid": null, "city_name": null,
    "source": "wikidata:P131"
  },
  "articles": [
    {"qid": "Q123456", "lang": "en", "title": "2024 Indian general election",
     "peak_views": 280000, "ratio": 41.0, "role": "lead"},
    {"qid": "Q1058", "lang": "hi", "title": "...", "peak_views": 51000, "ratio": 12.0, "role": "member"}
  ],
  "countries": [
    {"code": "IN", "ratio": 22.5, "peak_views": 950000},
    {"code": "US", "ratio": 3.4,  "peak_views": 41000}
  ],
  "curve": [{"t": {"$date": "2024-06-03T08:00:00Z"}, "v": 9800, "b": 10800}],
  "scopes": ["G", "R:IN", "A:IN", "A:IN:IN-DL"],
  "suspicious": false,
  "pipeline_run": "2026-11-05T10:00:00Z"
}
```

(Values above show the shape only.)

Indexes:

```javascript
db.events.createIndex({ category: 1, first_detected: -1 })
db.events.createIndex({ "location.country": 1, "location.region": 1 })
db.events.createIndex({ "articles.qid": 1 })
db.events.createIndex({ scopes: 1, score: -1 })
```

### 5.2 `pages` (Wikidata enrichment cache)

```json
{
  "_id": "en:2024_Indian_general_election",
  "qid": "Q...",
  "label": "2024 Indian general election",
  "instance_of": ["Q..."],
  "occupation": [],
  "category": "Politics",
  "category_source": "rule:P31",
  "country": "IN",
  "region": null,
  "city_qid": null,
  "links": ["Q...", "Q..."],
  "fetched_at": {"$date": "2026-11-03T12:00:00Z"}
}
```

Index: `db.pages.createIndex({ qid: 1 })`

### 5.3 `validation`

```json
{ "_id": "val_001", "name": "Indian general election counting day", "date": "2024-06-04",
  "expected_qids": ["Q..."], "scope": "R:IN", "matched_event_id": null, "notes": "" }
```

MongoDB data types used: `ObjectId/String _id`, `Date`, `Double`, `Int64`, `Array`, `Embedded document`, `Boolean`, `Null`.

---

## 6. Categories

| Category | Typical Wikidata signals (examples) |
|---|---|
| Politics | election, political party, politician (P106), legislature, government |
| Sports | sports competition/season/match, sports team, athlete/cricketer/footballer (P106), stadium |
| Entertainment | film, TV series, album, song, actor/singer (P106), award ceremony |
| Science & Tech | software, mobile phone model, company in tech, spacecraft, space mission, scientist |
| Business | company, stock market index, currency, central bank, businessperson |
| Disasters | natural disaster, earthquake, flood, tropical cyclone, accident, fire, terrorist attack |
| Other | anything not matched |

The exact QID → category mapping lives in `enrichment/category_rules.yaml`, documented in `07_FEATURE_DEVELOPMENT.md`.
