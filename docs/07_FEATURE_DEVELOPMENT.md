# 07 · Feature Development

How each feature is built: inputs, logic, outputs, configuration and the check that proves it works. Feature IDs match the PRD.

---

## F1 · Downloaders (FR-1, FR-2, FR-4)

**`scripts/download_hourly.py`**

```
for each hour H in [hourly_start, hourly_end] (or dev days):
    url  = f"https://dumps.wikimedia.org/other/pageviews/{Y}/{Y}-{M}/pageviews-{Y}{M}{D}-{HH}0000.gz"
    dest = /data/raw-cache/hourly/dt={Y}-{M}-{D}/<filename>
    if dest exists and size > 0 and gzip test passes: skip
    stream download to dest.part (requests, stream=True, 1 MB chunks, User-Agent from settings)
    on failure: retry 5× with exponential back-off (2, 4, 8, 16, 32 s)
    rename .part -> final; log (file, bytes, seconds)
```

- Run at most **2 parallel downloads** to be polite to Wikimedia.
- Integrity: `gzip -t` (or Python `gzip.open(...).read(1)`) after download.

**`scripts/download_country.py`**: same pattern. The published files are zero-padded TSV: `f"{d:%Y-%m-%d}.tsv"` (confirmed in step 1.4; the dataset README says `.csv` without padding). Destination `/data/raw-cache/country_daily/dt=YYYY-MM-DD/`.

**`scripts/load_hdfs.sh`**: for each local `dt=` folder, `hdfs dfs -mkdir -p` and `hdfs dfs -put -f` only the files missing in HDFS (compare with `hdfs dfs -ls`).

**Check:** re-running the downloader downloads nothing; file counts in HDFS equal the expected counts (24 per day).

---

## F2 · MR1 filter_normalise (FR-5, FR-7)

**Goal:** turn raw lines into clean `(lang, platform, title, hour, views)` rows for the configured languages only.

**Mapper `mapreduce/mr1_mapper.py`**

```
hour = parse from os.environ["mapreduce_map_input_file"]   # pageviews-20240603-120000.gz -> 2024-06-03T12
ALLOWED = {lang for lang in settings.languages}
for line in stdin:
    parts = line.rstrip("\n").split(" ")
    if len(parts) != 4: counter("bad_lines"); continue
    domain, title, views = parts[0], parts[1], parts[2]
    lang, _, sub = domain.partition(".")
    if lang not in ALLOWED: continue
    if sub == "":    platform = "desktop"
    elif sub == "m": platform = "mobile"
    else: continue                                # other projects (wiktionary, etc.)
    if not is_content_page(title): counter("non_content"); continue
    title = normalize_title(title)                # same rules as Hive UDF
    print(f"{lang}\t{platform}\t{title}\t{hour}\t{views}")
```

- **Counters** in streaming: write `reporter:counter:Radar,bad_lines,1` to **stderr**.
- **Key** = first 4 fields, **value** = views. Set `-D stream.num.map.output.key.fields=4`.

**Combiner and reducer `sum_reducer.py`:** sum `views` for consecutive equal keys (input is sorted). The key is everything before the last tab, so the same script serves MR1 (4 key fields) and MR2 (3 key fields). It also works as the combiner because summing is associative and commutative (test T-M2).

**Combiner: used in MR2, off by default in MR1.** The first full run showed `Combine input records` ≈ `Combine output records` for MR1: each hourly file already has every (lang, platform, title, hour) key exactly once, so the combiner only costs CPU. In MR2 one mapper reads all 24 hours and both platforms of a language, so the combiner collapses up to 48 rows into one before the shuffle. Benchmark B1 therefore measures MR2. (`run_mr1.sh --combiner` still turns it on for comparison.)

**Streaming gotcha:** a streaming combiner's output is split into key and value using the *reduce* output settings, which default to 1 key field. Without `-D stream.num.reduce.output.key.fields` set to the same value as `stream.num.map.output.key.fields`, everything after the combiner is keyed by language only: the job still succeeds but `Reduce input groups` drops to 5 and the reducer no longer sees equal keys next to each other. Both MR1 and MR2 set it.

**Partitioner `radar.mr.LangPartitioner` (custom, Java):** sends each language to its own reducer, so `part-00000` holds only `en`, `part-00001` only `hi`, and so on, in the order of `languages` in `settings.yaml`. Hadoop's built-in `KeyFieldBasedPartitioner -k1,1` hashes the language instead, and with five keys and five reducers two languages often land in the same file while another reducer gets nothing. Source: `mapreduce/java/LangPartitioner.java`, built into `mapreduce/lib/radar-mr.jar` by `mapreduce/build_jar.sh`, shipped with `-libjars` and given the order through `-D radar.langs=en,hi,ta,ja,pt`.

**Compression**

```
-D mapreduce.map.output.compress=true
-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
-D mapreduce.output.fileoutputformat.compress=true
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec
```

**Run script `mapreduce/run_mr1.sh <day> [--no-combiner] [--map-compress none|snappy|gzip] [--quick] [--out PATH]`** (core of the command):

```bash
mapred streaming \
  -libjars mapreduce/lib/radar-mr.jar \
  -D mapreduce.job.name="MR1 filter_normalise $DAY" \
  -D stream.num.map.output.key.fields=4 \
  -D stream.num.reduce.output.key.fields=4 \
  -D mapreduce.job.reduces=5 \
  -D radar.langs=en,hi,ta,ja,pt \
  -D mapreduce.map.output.compress=true \
  -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
  -files mr1_mapper.py,sum_reducer.py,common_title.py \
  -cmdenv RADAR_LANGS=en,hi,ta,ja,pt \
  -mapper "python3 mr1_mapper.py" -reducer "python3 sum_reducer.py" \
  -partitioner radar.mr.LangPartitioner \
  -input /radar/raw/hourly/dt=$DAY -output /radar/staged/hourly/dt=$DAY
```

The options are flags so the benchmarks (B1, B2) reuse the same script. Every run's full log, including the job counters, is saved in `benchmarks/results/logs/`.

(`mapreduce.job.reduces` = number of languages. The mapper also reports its own counters in group `Radar`: `lines_in`, `kept`, `other_domain`, `non_content`, `bad_lines`.)

**Check:** sum of views per language in the output equals the same sum computed by a Hive query on `raw_hourly` with identical filters (conservation test, `09_TESTING_QA.md` T-D2).

---

## F3 · MR2 daily_agg, MR3 top_n, MR4 search (FR-6, FR-7)

**MR2:** the mapper drops `platform` and the hour→day part and emits `lang \t title \t day \t views`. `sum_reducer.py` is the combiner and reducer, with the same `LangPartitioner`. Output `/radar/staged/daily/dt=...`.

**MR3 top_n (sorting):**
- Mapper emits `lang \t day \t views \t title`.
- Sort by `lang`, `day` ascending, then `views` numeric descending:
  ```
  -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator
  -D stream.num.map.output.key.fields=3
  -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2 -k3,3nr"
  -D mapreduce.partition.keypartitioner.options=-k1,2
  ```
- The reducer keeps the first 100 rows per (lang, day). The mapper drops rows under 100 views first, which removes most of the long tail before the shuffle.

**MR4 search:** the mapper reads MR1 output and emits only rows whose title equals `os.environ["SEARCH_TITLE"]` (passed with `-cmdenv SEARCH_TITLE=...`). Map-only job (`-D mapreduce.job.reduces=0`). `SEARCH_LANG` optionally limits it to one language. Demonstrates searching without an index.

**Check:** MR3 top-1 for `en` on the dev day matches `SELECT ... ORDER BY views DESC LIMIT 1` in Hive.

---

## F4 · Hive layer (FR-8, FR-9)

**Engine.** HiveServer2 (`apache/hive:4.0.1`, embedded Derby metastore) runs queries with **Tez in local mode**, inside the `hiveserver2` container, reading and writing HDFS (`docker/hive/conf/hive-site.xml`). Hive 4 deprecates MapReduce as an engine, and Tez on YARN would need Tez installed on the cluster; YARN itself is already exercised by MR1–MR4 and later by Spark. The container gets a 3 GB heap (`SERVICE_OPTS=-Xmx3g`) because the queries run in that JVM.

All HQL runs through `hive/run.sh` inside the container, which also sets default variables (`from`, `to`, `day`, `day2`):

```
docker compose exec hiveserver2 bash /workspace/hive/run.sh <file.hql> [--hivevar k=v ...]
```

1. **Raw tables** (`hive/01_raw_tables.hql`): database `radar` at `/radar/warehouse/radar.db`, `raw_hourly` (RegexSerDe, hour from `INPUT__FILE__NAME`), `raw_country_daily` (TSV), `MSCK REPAIR TABLE` to register the `dt=` folders.
2. **UDFs** (`hive/udf/`): `TitleRules.java` (the rules), `NormalizeTitle.java` and `IsContentPage.java` (GenericUDF wrappers: `initialize`, `evaluate`, `getDisplayString`). `build.sh` (in edge) compiles them with `--release 8` (the Hive image runs Java 8, edge has Java 11) against Hive's own jars, copied once with `docker compose cp hiveserver2:/opt/hive/lib ../hive/udf/hive-lib`, and uploads `radar-udf.jar` to `/radar/lib/`. `register.hql` creates permanent functions:
   ```sql
   CREATE FUNCTION radar.normalize_title AS 'radar.udf.NormalizeTitle'
     USING JAR 'hdfs://namenode:8020/radar/lib/radar-udf.jar';
   CREATE FUNCTION radar.is_content_page AS 'radar.udf.IsContentPage'
     USING JAR 'hdfs://namenode:8020/radar/lib/radar-udf.jar';
   ```
3. **Staged and warehouse tables** (`hive/02_warehouse.hql`): external tables over MR1/MR2/MR3 output and the external ORC warehouse tables (`03_DATA_MODEL.md` §3.3–3.4).
4. **Warehouse load** (`spark/jobs/load_warehouse.py`, run in edge with `spark-submit --master local[3] --driver-memory 3g ... --dev|--full|--day D`). The first version was a Hive `INSERT OVERWRITE ... GROUP BY lang, page_title, hour_ts`, but in Tez local mode every task runs inside the single 3 GB HiveServer2 JVM: grouping one day (about 65 million hourly rows into about 50 million keys) filled the heap and stalled for over 20 minutes without finishing a task, with or without map-side aggregation. Spark spills sorts and aggregations to disk per task and loads the same day in a few minutes. The job writes ORC files straight into the table folders (`/radar/warehouse/radar.db/<table>/dt=<day>/`), sorted by `lang, page_title` so ORC min/max statistics can skip stripes; re-running `hive/02_warehouse.hql` (`MSCK REPAIR TABLE`) registers the partitions. `country_daily` is cleaned with the Python version of the title rules (`common_title.py`), which the parity test proves identical to the Java UDFs. Hive keeps the role it is good at: schema, metastore, UDFs and SQL over the columnar tables.
5. **Analysis** (`hive/03_analysis.hql`): window functions, CTEs, joins, `ARRAY`/`MAP`, and one `TRANSFORM` through an awk script (`hive/scripts/mobile_share.awk`) to contrast an external script with a compiled UDF.
6. **Metadata recovery**: the metastore lives inside the container, so if `hiveserver2` is recreated, `hive/rebuild_metadata.sh` re-runs the DDL and function registration. Because every table is external, no data has to be reloaded.

**Check:** `SELECT radar.normalize_title('Virat_Kohli%27s_career')` returns `Virat Kohli's career`; `tests/dq/dq_hive.hql` (T-D2, load totals, T-D10) and `tests/dq/dq_spark.py` (T-D3, T-D4) pass for the dev day.

**Lesson for the report:** Hive's local engine is fine for metadata and for queries that reduce data to a few groups (sums per language, per hour, per country), but not for grouping tens of millions of distinct keys inside one JVM. The heavy, many-key work (the load, T-D3/T-D4, the B3 copies) is done in Spark; Hive queries the result.

---

## F5 · S1 build series with candidate pruning (FR-10)

**Problem:** there are millions of (lang, title) series, but only a tiny fraction ever spikes. Computing baselines for all of them wastes hours.

**Logic**

```python
hv = spark.read.orc("/radar/warehouse/radar.db/hourly_views")        # lang, page_title, hour_ts, views
det = hv.filter(col("hour_ts") >= detect_start)
candidates = (det.filter(col("views") >= cfg.global_hourly.min_views)
                 .select("lang", "page_title").distinct())           # tens of thousands
series = hv.join(broadcast(candidates), ["lang", "page_title"])     # all hours for candidates
hours  = spark.range(...)  # every hour in [hourly_start, hourly_end] as hour_ts
dense  = (candidates.crossJoin(hours)
            .join(series, ["lang", "page_title", "hour_ts"], "left")
            .fillna({"views": 0}))                                    # dumps omit zero-view hours
dense.write.partitionBy("lang").parquet("/radar/output/series_global")
```

**Check:** row count of `dense` = candidates × number of hours; no nulls in `views`.

---

## F6 · S2 global/language spike detection (FR-10, FR-12, FR-15, FR-16)

### Baseline (robust)

For each (lang, title) and each detection **day D**, the baseline uses the hourly values in the previous `baseline_hours` (168 = 7 days) **before D**:

- `median` = 50th percentile
- `sigma` = (p75 − p25) / 1.349 (IQR-based robust standard deviation, not distorted by earlier spikes)

Implementation without expensive sliding windows: **explode** each hour row into the detection days it serves, then aggregate once:

```python
# an hour h serves days D where D-7d <= h < D
served = dense.withColumn("serve_day", explode(sequence(date(h)+1, date(h)+7)))
              .filter(col("serve_day").between(detect_start, detect_end))
base = served.groupBy("lang","page_title","serve_day").agg(
          percentile_approx("views", [0.25, 0.5, 0.75], 1000).alias("q"))
```

### Spike rule (per hour)

```
b      = max(median, baseline_floor)                 # floor avoids divide-by-tiny
ratio  = views / b
z      = (views - median) / (sigma + 1)
spike  = views >= min_views AND ratio >= min_ratio AND z >= min_z
```

Defaults: `min_views 1000/hour, min_ratio 4, min_z 5, floor 50` (in `settings.yaml`).

### Spike episodes

Consecutive spike hours for the same (lang, title) merge into one **episode** (gaps of ≤ 1 hour allowed):

```python
w = Window.partitionBy("lang","page_title").orderBy("hour_ts")
flag new episode when (hour_ts - lag(hour_ts)) > 2 hours  -> running sum = episode_id
episode = groupBy(lang, page_title, episode_id).agg(
    min(hour_ts).alias("start"), max(hour_ts).alias("end"),
    max_by(hour_ts, views).alias("peak_hour"), max(views).alias("peak_views"),
    max(ratio).alias("ratio"), first(median).alias("baseline"),
    sum(views_desktop)/sum(views) as desktop_share)
```

### Suspicious flag (likely automated)

`suspicious = (end == start) AND (desktop_share > 0.95) AND ratio > 50`: a one-hour, desktop-only, enormous jump is a typical bot pattern. Suspicious episodes are kept but marked, never silently removed.

**Output:** `/radar/output/spikes_global` with columns `lang, page_title, start, end, peak_hour, peak_views, baseline, ratio, desktop_share, suspicious`.

**Check:** unit tests on synthetic series (`09_TESTING_QA.md` T-U3), plus the top 20 on the dev slice look like real events.

---

## F7 · S3 country spike detection (FR-11)

Same idea on `radar.country_daily` (`country, qid, day, views`), grouped by `(country, qid)` (summing languages of the same entity within a country).

| Setting | Value | Why |
|---|---|---|
| Baseline window | 14 previous days | Daily data needs a longer window |
| Missing days | Treated as 0 | The row was suppressed, i.e. below the release threshold |
| Baseline floor | 90 | Equal to the lowest release threshold, so a missing day never becomes an infinite jump |
| Rule | `views >= 500 AND ratio >= 3 AND z >= 4` | |

Countries with no published data simply have no rows; the dashboard shows the corresponding empty state.

**Output:** `/radar/output/spikes_country` (`country, qid, day, views, baseline, ratio`).

---

## F8 · S4 Wikidata enrichment (FR-13)

**Input:** distinct `(lang, page_title)` from global spikes (+ `qid` already present for country spikes). This is a small set (thousands), so it is collected to the driver.

**Step 1: title → QID** (only for global spikes)
`GET https://www.wikidata.org/w/api.php?action=wbgetentities&sites={lang}wiki&titles=T1|T2|...&props=info|claims|labels&languages=en&format=json`. Up to **50 titles per call**. Redirects: also try `&redirects=yes`.

**Step 2: claims used**

| Property | Meaning | Use |
|---|---|---|
| P31 | instance of | Category rules |
| P279 | subclass of | Category fallback (one level) |
| P106 | occupation (for humans, P31 = Q5) | Category for people |
| P17 | country | Event country |
| P131 | located in the administrative territorial entity | Walk up to the first-level region (state) |
| P276 | location | Events whose location is a venue/city |
| P300 | ISO 3166-2 code | Region code (e.g. IN-RJ) |
| P625 | coordinate location | Optional (map view later) |

**Step 3: region resolution.** From the item (or its P276 location), follow P131 upwards (max 5 hops) until an item with a **P300 ISO 3166-2 code** is found. That gives the state/region. The city is the first hop when it is an instance of city/town. Results are cached per QID. A single SPARQL query can resolve many items at once:

```sparql
SELECT ?item ?region ?code WHERE {
  VALUES ?item { wd:Q1 wd:Q2 }
  ?item wdt:P131* ?region .
  ?region wdt:P300 ?code .
}
```

**Step 4: category rules** (`enrichment/category_rules.yaml`). Ordered rules; the first match wins:

```yaml
- category: Politics
  p31_any: [Q40231, Q7278, Q4164871, Q1752346]     # election, political party, position, ...
  p106_any: [Q82955]                                # politician
- category: Sports
  p31_any: [Q13406554, Q27020041, Q847017, Q16510064]
  p106_any: [Q2066131, Q12299841, Q937857]          # athlete, cricketer, footballer
- category: Entertainment
  p31_any: [Q11424, Q5398426, Q482994, Q7366]       # film, TV series, album, song
  p106_any: [Q33999, Q177220, Q2526255]             # actor, singer, film director
- category: Science & Tech
  p31_any: [Q7397, Q19723451, Q40218, Q2133344]     # software, smartphone model, spacecraft, space mission
- category: Business
  p31_any: [Q4830453, Q223371, Q8142, Q66344]       # business, stock index, currency, central bank
- category: Disasters
  p31_any: [Q8065, Q7944, Q8068, Q8092, Q168983, Q2223653]  # natural disaster, earthquake, flood, cyclone, conflagration, terrorist attack
default: Other
```

> The QIDs above are a **starting list**. Verify each one on wikidata.org before relying on it, and extend the list by inspecting the "Other" bucket after the first run.

**Caching:** every lookup is stored in Mongo `pages` (`_id = "<lang>:<title>"`). Re-runs only query unknown titles.

**Etiquette:** User-Agent from settings, ≤ 1 request/second, retry on HTTP 429/5xx honouring `Retry-After`.

**Output:** `/radar/output/page_meta` (`lang, page_title, qid, category, country, region_code, region_name, city_qid, links[]`).

**Check:** a hand-picked list of 20 known titles gets the expected category and country (T-U5).

---

## F9 · S5 event grouping and scoring (FR-14, FR-15)

**Step 1: nodes.** Each spike episode (global) and each country spike is a node, keyed by its QID.

**Step 2: edges.** Connect two nodes when:
1. They have the **same QID** (e.g. the English and Hindi articles of one topic, or the global and country spikes of one entity), with peaks within 24 hours; or
2. One item **links to** the other in its Wikidata claims (`links[]`), and their peak hours are within `group_window_hours` (6).

Very generic link targets (countries, Q5 "human", languages, years) are excluded from linking, otherwise everything would merge.

**Step 3: connected components** with union-find on the driver. The number of spike nodes is small (thousands), so this is fast and simple; no graph library is needed.

**Step 4: event fields**

| Field | Rule |
|---|---|
| `event_id` | `evt_<YYYYMMDD of first_detected>_<lead QID>` |
| lead article | Member with the highest peak views |
| title | Lead article's label |
| category | Most common member category (ties → lead's) |
| location | Lead article's location; if none, the most common member location |
| first_detected / peak_time | min(start) / time of the max peak |
| ratio | Max member ratio (capped at `ratio_cap` = 100) |
| **score** | `min(ratio, 100) × log10(peak_views − baseline + 10)` |
| langs | Distinct languages of member spikes |
| countries | Country spikes attached (code, ratio, peak) |
| scopes | `G` if it has global spikes; `R:<CC>` for each attached country spike; `A:<CC>` and `A:<CC>:<REGION>` from its location |
| suspicious | All member episodes suspicious |

**Why this score:** ranking by jump alone puts tiny pages first (10 → 200 views is ×20). Multiplying by the log of the extra views rewards jumps that also involve real volume.

**Country-only events:** a country spike with no matching global spike becomes its own event with scope `R:<CC>` only.

**Output:** `/radar/output/events` (one row per event, arrays for members).

**Check:** the dev day's top events are coherent (no giant "everything" event, no duplicates of one topic in the top 10).

---

## F10 · S6 serving loaders (FR-17, FR-18, FR-19)

| Target | Method | Notes |
|---|---|---|
| `article_hourly` | Spark Cassandra connector `df.write.format("org.apache.spark.sql.cassandra")` | Only for lead + member articles of events (±7 days around the peak) |
| `article_country_daily` | Spark Cassandra connector | Only for (qid, country) pairs in events |
| `event_rank` | Python driver, prepared `INSERT`, two rows per event per scope (`ALL` + its category), `day` = first_detected day; one row per day the event is active, for 7d/30d windows | |
| `trending_now` | Python driver, `INSERT ... USING TTL` | Last detection day's top 20 per scope |
| `spike_counts` | Python driver, `UPDATE ... SET spikes = spikes + 1` | Counters cannot be set with INSERT |
| `region_index` | Python driver | From events with `A:<CC>:<REGION>` scopes |
| Mongo `events` | `pymongo` `bulk_write` of `ReplaceOne(upsert=True)` | Idempotent re-runs |

Spark submit with the connector:

```bash
spark-submit --master local[*] \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.cassandra.connection.host=cassandra \
  /workspace/spark/jobs/s6_load_serving.py
```

**Idempotency:** before loading a run, `TRUNCATE` `event_rank`, `trending_now`, `region_index`; counters are reset by `TRUNCATE spike_counts`.

**Check:** `SELECT * FROM radar.event_rank WHERE scope='G' AND day='2024-06-04' AND category='ALL' LIMIT 5;` returns the same top 5 as the Spark output.

---

## F11 · Dashboard (FR-20 to FR-31)

Built exactly as specified in `04_UI_UX_DESIGN.md`. Key implementation notes:

- `st.session_state` holds the filter state; one `on_change` callback per control enforces the rules (country change resets state/mode; picking a state sets mode = about).
- Time window → `days = [as_of - i for i in range(n)]`, clipped to available days (`SELECT DISTINCT` is not possible on partition keys, so available days come from `settings.yaml` or a small `meta` document in Mongo).
- `get_top_events` merges multiple day partitions by `event_id` (max score) before sorting.
- The detail panel fetches the Mongo document plus the Cassandra series in two cached calls.
- Plotly bar chart for the series, a dashed `hline`/line for the baseline, and the sparkline from the stored list.

**Check:** all five scenarios in `04_UI_UX_DESIGN.md` §4 and every empty state behave as specified.

---

## F12 · Validation (FR-30)

- `data/validation/known_events_2024-06.csv`: `id, name, date, scope, expected_titles_or_qids, notes`. Build it from reliable sources and double-check every date.
- Candidate events to verify for the June 2024 window: Indian general election counting day (4 Jun), the India vs Pakistan T20 World Cup match (9 Jun), the Indian PM swearing-in (9 Jun), European Parliament elections (6–9 Jun), Apple WWDC keynote (10 Jun), the Kuwait building fire (12 Jun), the Euro 2024 opening (14 Jun).
- `spark/jobs/s7_validate.py`: an event matches a known event if it contains any expected QID (or title) and its first detection is within 48 h of the date. Outputs recall, rank and detection delay to Mongo `validation` and a CSV.
- Precision@50: label the top 50 global events in `data/validation/labels_top50.csv` (`event_id, is_real, note`).

---

## F13 · Benchmarks (FR-7, PRD §9)

| ID | Compare | Measure | Script |
|---|---|---|---|
| B1 | MR1 with vs without combiner | `Map output materialized bytes`, `Reduce shuffle bytes`, runtime | `benchmarks/b1_combiner.sh` |
| B2 | Map output compression off / Snappy / Gzip | Shuffle bytes, runtime, CPU ms | `benchmarks/b2_compression.sh` |
| B3 | Text vs ORC vs Parquet | HDFS size (`du -s`), runtime of 3 HQL queries | `benchmarks/b3_formats.hql` |
| B4 | Daily aggregation: MR2 vs Spark DataFrame | Wall-clock runtime on the same input | `benchmarks/b4_mr_vs_spark.sh` |
| B5 | RDD `reduceByKey` vs DataFrame `groupBy` | Runtime, shuffle size (Spark UI) | `benchmarks/b5_rdd_vs_df.py` |

Run each 3 times and record the median in `benchmarks/results/<id>.csv` (`variant, run, metric, value`).

---

## F14 · Pipeline runner

`scripts/run_pipeline.sh [--dev|--full]` is a **host-side** wrapper. It runs, in order: load → MR1 → MR2 (via `docker compose exec edge`) → Hive repair + warehouse load (via `docker compose exec hiveserver2 beeline`) → S1 → S2 → S3 → S4 → S5 → S6 (via `docker compose exec edge spark-submit`). It stops at the first failure and prints the time per stage, which goes into the report. The in-container parts live in `scripts/run_ingest_mr.sh` and `scripts/run_spark.sh` (see `08_INTEGRATION.md` §5).
