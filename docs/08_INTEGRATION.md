# 08 · Integration

How the parts connect: external sources, connections between containers, and the **contract** between every pair of pipeline stages (what one stage promises to produce and what the next stage relies on).

---

## 1. External integrations

### 1.1 Wikimedia hourly pageview dumps

| Item | Value |
|---|---|
| Endpoint | `https://dumps.wikimedia.org/other/pageviews/<YYYY>/<YYYY-MM>/pageviews-<YYYYMMDD>-<HH>0000.gz` |
| Auth | None |
| Etiquette | Descriptive User-Agent, ≤ 2 parallel downloads, resume instead of re-downloading |
| Failure handling | Retry with exponential back-off; mark the file missing in the run log; the pipeline refuses to run S1 if any hour in the configured range is missing (missing hours would look like zero views and create fake spikes) |
| Mirrors | If the main site is slow, Wikimedia lists official mirrors on its dumps page. Same paths, same files |

### 1.2 Wikimedia per-country daily pageviews

| Item | Value |
|---|---|
| Home | `https://analytics.wikimedia.org/published/datasets/country_project_page/` (README: `00_README.html`) |
| File | `YYYY-M-D.csv` (no zero padding) |
| Columns | `country, project, page_id, page_title, item_id, gbc` |
| Notes | Differentially private (noisy) counts; rows below the release threshold are absent; some countries are never published |

### 1.3 Wikidata

| Use | Endpoint | Limits / rules |
|---|---|---|
| Title → item + claims | `https://www.wikidata.org/w/api.php?action=wbgetentities&sites=<lang>wiki&titles=...&props=info|claims|labels&format=json` | ≤ 50 titles per request; sequential requests; User-Agent required |
| Region resolution in bulk | `https://query.wikidata.org/sparql?query=...&format=json` | Queries time out after about 60 s; keep `VALUES` lists to ≤ 200 items per query; User-Agent required |

Client: `enrichment/wikidata_client.py`

```python
class WikidataClient:
    def __init__(self, user_agent: str, cache: pymongo.collection.Collection): ...
    def titles_to_items(self, lang: str, titles: list[str]) -> dict[str, dict]:   # batched, cached
    def regions_for(self, qids: list[str]) -> dict[str, dict]:                    # SPARQL, cached
```

Failure handling: on repeated failure, the page is stored with `category="Other"`, `country=None` and `enrichment_error` set, so one bad title never blocks the run. Re-runs retry only the failed ones.

### 1.4 Optional: Wikimedia ML article-topic model (fallback for "Other")

Wikimedia hosts a language-agnostic **article topic** model (and an **article country** model) on its Lift Wing inference service. It could label pages that Wikidata rules leave as "Other". This is a **Could**. Before using it, read the current model page on meta.wikimedia.org for the exact endpoint, request format and rate limits.

---

## 2. Container connectivity

All jobs run **inside the Docker network** `radar`, so services reach each other by hostname. The dashboard runs on the host and uses the `127.0.0.1` port mappings.

| From → To | Address | Protocol |
|---|---|---|
| edge → HDFS | `hdfs://namenode:8020` | HDFS RPC |
| edge → YARN | `resourcemanager:8032` (from `yarn-site.xml`) | YARN RPC |
| hiveserver2 → HDFS | `hdfs://namenode:8020` (from `hive-site.xml`) | HDFS RPC |
| edge (beeline) → Hive | `jdbc:hive2://hiveserver2:10000` | Thrift/JDBC |
| edge (Spark) → Cassandra | `cassandra:9042` | CQL native |
| edge (Spark/pymongo) → MongoDB | `mongodb://mongo:27017` | MongoDB wire |
| host (dashboard) → Cassandra | `127.0.0.1:9042` | CQL native |
| host (dashboard) → MongoDB | `mongodb://127.0.0.1:27017` | MongoDB wire |
| browser → UIs | `localhost:9870 / 8088 / 19888 / 10002 / 4040 / 8501` | HTTP |

Hostnames are set in `config/settings.yaml` with two profiles (`in_docker`, `on_host`), and code picks one through the environment variable `RADAR_ENV`:

```yaml
connections:
  in_docker: {cassandra: cassandra, mongo: "mongodb://mongo:27017", hdfs: "hdfs://namenode:8020"}
  on_host:   {cassandra: 127.0.0.1, mongo: "mongodb://127.0.0.1:27017"}
```

---

## 3. Library and connector versions

| Integration | Library | Version (pin) |
|---|---|---|
| Spark ↔ HDFS | Bundled Hadoop 3 client in `spark-3.5.3-bin-hadoop3` | — |
| Spark ↔ ORC/Parquet | Built in | — |
| Spark ↔ Cassandra | `com.datastax.spark:spark-cassandra-connector_2.12` | `3.5.1` (for Spark 3.5) |
| Python ↔ Cassandra | `cassandra-driver` | latest 3.x |
| Python ↔ MongoDB | `pymongo` | latest 4.x |
| Hive UDF compile | `hive-exec` jar from the hiveserver2 image + `hadoop classpath` | Hive 4.0.1 |
| Dashboard | `streamlit`, `plotly`, `pandas` | latest |

Spark loads data into MongoDB through `pymongo` from the driver (the number of events is small), so the Mongo Spark connector is **not needed**.

---

## 4. Stage contracts

Every stage writes to a fixed location with a fixed schema. Every stage starts with a **precondition check** and stops with a clear error if the previous stage's output is missing or malformed.

| # | Producer → Consumer | Location | Schema / format | Precondition check in consumer |
|---|---|---|---|---|
| C1 | Downloader → load_hdfs | `/data/raw-cache/hourly/dt=*/` | `pageviews-*.gz`, 24 per day | file count = 24 × days; each passes gzip test |
| C2 | load_hdfs → MR1 | `/radar/raw/hourly/dt=*/` | Same | `hdfs dfs -count` matches C1 |
| C3 | MR1 → MR2, Hive | `/radar/staged/hourly/dt=*/part-*.gz` | TSV: `lang, platform, title, hour(yyyy-MM-ddTHH), views` | `_SUCCESS` file present for every dt |
| C4 | MR2 → Hive | `/radar/staged/daily/dt=*/` | TSV: `lang, title, day, views` | `_SUCCESS` present |
| C5 | Hive → Spark S1 | `/radar/warehouse/radar.db/hourly_views/dt=*/` | ORC (see data model) | All dt partitions in config range exist |
| C6 | Hive → Spark S3 | `/radar/warehouse/radar.db/country_daily/dt=*/` | ORC | All dt partitions in country range exist |
| C7 | S1 → S2 | `/radar/output/series_global/` | Parquet, dense hourly | No null views; hours per key = expected |
| C8 | S2/S3 → S4/S5 | `/radar/output/spikes_global/`, `spikes_country/` | Parquet (see Feature Dev F6/F7) | Non-empty; column set matches |
| C9 | S4 → S5 | `/radar/output/page_meta/` | Parquet | Every spike key has a row (category may be Other) |
| C10 | S5 → S6 | `/radar/output/events/` | Parquet | Unique `event_id` |
| C11 | S6 → Dashboard | Cassandra `radar.*`, Mongo `radar.events` | See data model | Dashboard `health()` check: both DBs reachable, `event_rank` not empty |

A small helper, `spark/common/contracts.py`, implements these checks (`assert_hdfs_exists`, `assert_schema`, `assert_unique`), and every job calls it first.

---

## 5. Run orchestration

`scripts/run_pipeline.sh` is the single integration point. It runs **on the host** and calls into the containers, because `beeline` lives in the `hiveserver2` container while Hadoop and Spark live in `edge`.

```bash
#!/usr/bin/env bash
set -euo pipefail
MODE=${1:---dev}
DC="docker compose -f docker/docker-compose.yml"
step() { echo "== $1 =="; local t=$(date +%s); shift; "$@"; echo "   took $(( $(date +%s)-t ))s"; }

step "ingest + MapReduce" $DC exec -T edge bash /workspace/scripts/run_ingest_mr.sh $MODE
step "hive warehouse"     $DC exec -T hiveserver2 beeline -u jdbc:hive2://localhost:10000 \
                              -f /workspace/hive/02_warehouse.hql
step "spark S1-S6"        $DC exec -T edge bash /workspace/scripts/run_spark.sh $MODE
```

`run_ingest_mr.sh` (inside edge): `load_hdfs.sh`, then MR1 and MR2 for every day from `list_days.py`.

`run_spark.sh` (inside edge):

```bash
for J in s1_build_series s2_spikes_global s3_spikes_country s4_enrich s5_events; do
  spark-submit --master local[*] --driver-memory 3g \
    --conf spark.sql.session.timeZone=UTC /workspace/spark/jobs/$J.py $MODE
done
spark-submit --master local[*] --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.cassandra.connection.host=cassandra /workspace/spark/jobs/s6_load_serving.py $MODE
```

**Spark on YARN vs local:** jobs run with `--master local[*]` inside `edge` by default (fewer moving parts). For the report, run one job with `--master yarn --deploy-mode client` to show Spark on YARN; this needs `HADOOP_CONF_DIR` (already set in the image).

---

## 6. Integration test checkpoints

After the dev run, these must all be true (automated in `tests/integration/test_pipeline_dev.py`):

1. Raw hourly files in HDFS = 48.
2. Sum of views per language in `staged_hourly` = the same sum from `raw_hourly` with the same filters.
3. `hourly_views` row count = distinct (lang, title, hour) in `staged_hourly`.
4. `spikes_global` is non-empty, and every spike's `ratio ≥ min_ratio`.
5. Every `event_id` in Parquet exists in Mongo `events`, and in `event_rank` for scope `G` (if it has global spikes).
6. The dashboard's `get_top_events('G', [day])` returns the same top 5 as the Spark output.
