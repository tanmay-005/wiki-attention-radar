# 06 · Implementation Plan

**Start:** Mon 19 Oct 2026  **Target:** Sun 15 Nov 2026 (about 4 weeks)

Rule for the whole project: **build everything on the dev slice first (2 days of data), then run the full data once at the end.** The demo then works at every point, and long runs happen only when the code is right.

---

## Timeline

| Phase | Dates | Focus | Exit criterion |
|---|---|---|---|
| P0 | 19–21 Oct | Environment and setup | All checks in `05_SETUP.md` §13 pass |
| P1 | 21–23 Oct | Ingestion → HDFS | Dev slice in HDFS; downloader resumable |
| P2 | 23–27 Oct | MapReduce on YARN | MR1–MR4 correct on dev slice; combiner/partitioner/compression working |
| P3 | 27–30 Oct | Hive | Raw, staged and ORC tables; UDF; analysis HQL |
| P4 | 30 Oct – 5 Nov | Spark: spikes, enrichment, events | Events Parquet produced on dev slice; sensible top 20 |
| P5 | 5–7 Nov | Serving: Cassandra + MongoDB | All tables/collections loaded; queries < 100 ms |
| P6 | 7–10 Nov | Dashboard | Five core scenarios work on dev data |
| P7 | 10–12 Nov | Full run, validation, benchmarks | Full 21-day run finished; recall/precision and benchmarks recorded |
| P8 | 12–15 Nov | Report, README polish, demo | Report, slides, demo script, recorded backup video |

Buffer: P4 is the hardest phase and has the most days. If behind, cut "Should/Could" items (see the end of this file), never the Musts.

---

## P0 · Environment and setup (19–21 Oct)

**Do**
1. Install Docker Desktop and set the memory limits.
2. Create the repo, skeleton folders, `.gitattributes`, `.gitignore`, copy the `docs/`.
3. Write the Dockerfile, entrypoint and Hadoop/Hive configs; `docker compose build`.
4. Start the `hadoop` profile; pass the UI checks, Pi job and streaming smoke test.
5. Start `hive` and `nosql`; run the init scripts.
6. Write `config/settings.yaml` and `spark/common/config.py` (loader used by every script).

**Deliverables:** `docker/`, `config/settings.yaml`, `scripts/init_cassandra.cql`, `scripts/init_mongo.js`
**Report screenshots:** NameNode UI, YARN UI, `docker compose ps`

## P1 · Ingestion (21–23 Oct)

**Do**
1. `scripts/download_hourly.py`: builds the URL list from `settings.yaml`; `--dev` / `--full`; streams to `/data/raw-cache`; skips complete files; retries with back-off; logs size and duration.
2. `scripts/download_country.py`: same for the daily per-country files (`YYYY-MM-DD.tsv` files).
3. `scripts/load_hdfs.sh`: `hdfs dfs -put` into `dt=` folders; skips existing files.
4. Inspect real files and **update `03_DATA_MODEL.md`** with the confirmed delimiter, header and country format.
5. HDFS demos for the report: `hdfs fsck /radar/raw -files -blocks | head`, `-du -h`, `-count`, `-setrep`.

**Deliverables:** three scripts and a confirmed data format note.

## P2 · MapReduce (23–27 Oct)

**Do**
1. MR1 `filter_normalise`: mapper, combiner (sum), reducer (sum), partition by `lang`, Snappy map-output compression, Gzip output.
2. MR2 `daily_agg` with combiner.
3. MR3 `top_n` per (lang, day) using `KeyFieldBasedComparator` secondary sort.
4. MR4 `search` for one title across all hours.
5. Local pipe tests: `cat sample | mapper | sort | reducer` (see `09_TESTING_QA.md`).
6. Run all on YARN with the dev slice; save counters to `benchmarks/results/`.
7. Benchmarks B1 (combiner on/off) and B2 (compression off/Snappy/Gzip).

**Deliverables:** `mapreduce/*.py`, `mapreduce/run_*.sh`, counters CSV.
**Report screenshots:** YARN application list, a job's counters page, JobHistory.

## P3 · Hive (27–30 Oct)

**Do**
1. `hive/01_raw_tables.hql`: `raw_hourly` (RegexSerDe), `raw_country_daily` (CSV SerDe or delimited); `MSCK REPAIR`.
2. Java UDFs `normalize_title`, `is_content_page`: compile with `javac` against the Hive and Hadoop jars and register with `ADD JAR` / `CREATE FUNCTION`.
3. `hive/02_warehouse.hql`: staged external tables → CTAS / `INSERT OVERWRITE` into ORC `hourly_views`, `daily_views`, `country_daily` (dynamic partitions).
4. `hive/03_analysis.hql`: top articles per day and language, views by hour of day, mobile share, `collect_set`/map examples, `INPUT__FILE__NAME` usage.
5. Benchmark B3: text vs ORC vs Parquet (size + 3 queries).

**Deliverables:** HQL scripts, UDF source and jar build script, B3 results.

## P4 · Spark (30 Oct – 5 Nov)

**Do**
1. `spark/jobs/s0_rdd_demo.py`: RDD transformations/actions on raw lines (top languages), `toDebugString` lineage, caching demo.
2. `s1_build_series.py`: read ORC from HDFS → candidate pruning → dense hourly series (zero-fill) → Parquet.
3. `s2_spikes_global.py`: robust baseline (median + IQR) over the previous 168 hours → spike hours → spike episodes.
4. `s3_spikes_country.py`: same per (country, qid) on daily data with 14-day baselines and floor 90.
5. `s4_enrich.py`: Wikidata lookup (title → QID, claims) with cache in MongoDB `pages`; category rules; location + ISO 3166-2 region.
6. `s5_events.py`: union-find grouping, event score, scopes, suspicious flag → `events` Parquet.
7. Review the top 20 events on the dev slice by eye; tune thresholds in `settings.yaml`.
8. Benchmark B4: Spark vs MapReduce daily aggregation; B5: RDD vs DataFrame.

**Deliverables:** `spark/jobs/*.py`, `enrichment/*`, tuned settings.
**Report screenshots:** Spark UI DAG and stages for S2.

## P5 · Serving (5–7 Nov)

**Do**
1. `s6_load_serving.py`: Spark Cassandra connector for `article_hourly` and `article_country_daily`; Python driver (prepared statements) for `event_rank`, `trending_now` (TTL), `spike_counts` (counters), `region_index`.
2. MongoDB: `events` upserts by `_id`; `mongoexport` a sample to JSON and `mongoimport` it back into `events_import_demo`.
3. Cassandra: `COPY event_rank TO 'event_rank.csv'` and `COPY ... FROM` into a demo table; one `ALTER TABLE`; system table queries.
4. CRUD demo scripts for both databases (`scripts/crud_demo_cassandra.cql`, `scripts/crud_demo_mongo.js`).

**Deliverables:** loader job, CRUD/import/export demo scripts with outputs.

## P6 · Dashboard (7–10 Nov)

**Do**
1. `dashboard/data_access.py` with the functions in `02_ARCHITECTURE.md` §4, plus caching.
2. `dashboard/app.py` (Radar page): filters, summary line, tiles, list, detail panel, category chart.
3. `dashboard/pages/2_Validation.py`, `3_Benchmarks.py`, `4_About.py`.
4. Walk through the five core scenarios and the edge states from `04_UI_UX_DESIGN.md`.

**Deliverables:** working dashboard on dev data.

## P7 · Full run, validation, benchmarks (10–12 Nov)

**Do**
1. Download the full period (start the downloads on day 1 of this phase, or earlier overnight).
2. `scripts/run_pipeline.sh --full` end to end; record the time per stage.
3. Build `data/validation/known_events_2026-10.csv` (date-checked) and run `spark/jobs/s7_validate.py`.
4. Label the top 50 events (real / noise) for precision@50.
5. Re-run benchmarks on the full data where feasible.

**Deliverables:** run log with timings, validation results, final benchmark CSVs.

## P8 · Report and demo (12–15 Nov)

**Do**
1. Report sections: problem, dataset, architecture, each tool with screenshots, algorithms, results (validation + benchmarks), CAP/BASE discussion, SQL vs NoSQL vs NewSQL, limitations, future work.
2. Slides (about 12) and a 5-minute demo script: Global → India → Politics → Rajasthan → event detail → validation → benchmarks.
3. Record a backup demo video in case of live problems.
4. Final README check: someone else can run the dev slice by following it.

---

## Priorities (what to cut if late)

| Must | Should | Could |
|---|---|---|
| Ingestion, MR1–MR2, Hive raw + ORC + UDF, S1–S6, Cassandra + Mongo loaders, Radar page with all filters, full run, validation recall, report | MR3/MR4 demos, suspicious flag, Validation page, B1–B4 benchmarks, precision@50 | Benchmarks page, B5, Parquet copy, Lift Wing topic model fallback, IST display |

## Working rhythm

- Commit at the end of each step with a clear message (`P2: MR1 filter_normalise with combiner`).
- Tick the matching step in the build tracker only after its **Done when** check passes.
- Write report notes and take screenshots **while** working on each phase, not at the end.
