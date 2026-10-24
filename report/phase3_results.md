# Phase 3 results: Hive

Notes and numbers from the Phase 3 runs on the dev days (2024-06-03 and 2024-06-04), for the final report. Hive 4.0.1 (HiveServer2 with an embedded Derby metastore) runs queries with Tez in local mode inside its container and reads and writes HDFS.

## Tables

| Layer | Tables | Format | Filled by |
|---|---|---|---|
| Raw | `raw_hourly` (RegexSerDe, hour from `INPUT__FILE__NAME`), `raw_country_daily` | gzip text / TSV | the downloaders (Phase 1) |
| Staged | `staged_hourly`, `staged_daily`, `staged_topn` | gzip text | MR1, MR2, MR3 (Phase 2) |
| Warehouse | `hourly_views`, `daily_views`, `country_daily` | ORC (ZLIB), sorted by language and title | Spark (`spark/jobs/load_warehouse.py`) |

Every table is external with an explicit location, so the files stay exactly where Spark reads them and the whole schema can be recreated from the DDL in seconds if the metastore is lost.

Partitions registered: 21 hourly days and 31 country days of raw data; 2 dev days of staged and warehouse data.

| Warehouse table | 2024-06-03 | 2024-06-04 |
|---|---|---|
| hourly_views | 49,275,080 rows, 274,484,789 views | 48,579,336 rows, 282,118,840 views |
| daily_views | 7,945,069 rows, 274,484,789 views | 7,922,830 rows, 282,118,840 views |
| country_daily | 284,383 rows, 141 countries | 275,331 rows, 135 countries |

MR1 produced 64.9 million hourly rows for 2024-06-03 (one per platform); after putting desktop and mobile side by side there are 49.3 million, so about 15.6 million articles were read on both platforms in the same hour.

## Java UDFs

`normalize_title(title)` and `is_content_page(lang, title)` are GenericUDFs built on `TitleRules.java`, a port of the Python rules MR1 uses. They are registered as permanent functions from a jar in HDFS (`/radar/lib/radar-udf.jar`), compiled for Java 8 because the Hive image runs Java 8.

- A parity test runs the Java and Python versions on every namespace name in several spellings, tricky percent-encodings and random titles; locally it was also run on 200,000 random titles. There were no differences.
- On real data the two implementations agree to the view (T-D2 below).

## Data-quality checks (2024-06-03)

| Check | Result |
|---|---|
| T-D2 view conservation: raw dumps cleaned by the Hive UDFs vs MR1 output, per language, 24 files each | en 237,354,936 / hi 1,243,566 / ja 28,022,508 / pt 7,535,285 / ta 328,494: identical, PASS |
| Warehouse load: staged text vs ORC | 274,484,789 = 274,484,789, PASS |
| Hours per language | 120 of 120, PASS |
| T-D3 daily = sum of hourly (Spark), every title | 7,945,069 titles, 0 mismatches, PASS |
| T-D4 no duplicate (lang, title, hour) (Spark) | 0 duplicates, PASS |
| T-D10 country data | 284,383 rows, no rows under the 90-view privacy floor, only configured languages, 99.6% with a Wikidata ID, PASS |

T-D2 is the strongest check in the project so far: two separate implementations of the cleaning rules (Python in MapReduce, Java in Hive) produce exactly the same totals on 157 million raw lines.

## Analysis (2024-06-03 unless noted)

**Mobile share per language (A2):** en 66.1%, ja 65.0%, pt 70.7%, hi 91.8%, ta 86.9%. Hindi and Tamil Wikipedia are read almost entirely on phones.

**Busiest hour in UTC (A3):** hi and ta 07:00 (12:30 in India), ja 13:00 (22:00 in Japan), pt 01:00 (22:00 in Brazil), en 20:00 (afternoon in the US). The peaks follow the readers' time zones.

**Top articles (A1):** the same lists MR3 produced with a MapReduce secondary sort: Claudia Sheinbaum, the 2024 Mexican election, Rob Burrow and Kylian Mbappé (en); Eiko Masuyama and Yuka Saso (ja); Indian election pages (hi); Karunanidhi and Annamalai (ta).

**Countries reading English Wikipedia (A5):** United States 55.1M views, India 10.3M, United Kingdom 9.8M, Canada 3.5M, Australia 1.9M.

**India's top articles (A6):** election pages in all three Indian-interest languages (2019 general election and 18th Lok Sabha in English; the 2019 election and the list of prime ministers in Hindi; Karunanidhi and Annamalai in Tamil).

**Complex types (A7):** `collect_set` and `str_to_map` build an `ARRAY` of languages and a `MAP` of views per language for each Wikidata item; for example Q866 (YouTube) is read in all five languages.

**TRANSFORM (A8):** an awk script streamed through Hive labels the busiest English article-hours: Claudia Sheinbaum 69–80% mobile ("mixed"), Dick Van Dyke about 88% ("mobile-heavy").

**Biggest day-over-day jumps, 2024-06-03 → 06-04 (A4, Spark):** 2024-06-04 was the counting day of India's general election. Faizabad Lok Sabha constituency (the Ayodhya seat) rose from 655 to 68,568 views (105×), Engineer Rashid from 79 to 20,445 (205×), Samajwadi Party from 3,589 to 253,618 (71×), the list of NDA members passed 1.5 million views (55×), and राष्ट्रीय जनता दल rose 96× on Hindi Wikipedia. Other entries (an animated film, a novel, a New York building) are one-day spikes of the kind Phase 4's baselines are designed to judge; a plain two-day ratio cannot tell real events from noise.

## Benchmark B3: text vs ORC vs Parquet (hourly_views, 2024-06-03, 49.3M rows)

| Format | Size | Q1 count + sum | Q2 top 10 Japanese titles | Q3 views per hour |
|---|---|---|---|---|
| Text (uncompressed) | 2.46 GB | 29.7 s | 35.5 s | 76.5 s |
| ORC (ZLIB) | 194 MB | 2.6 s | 5.2 s | 5.3 s |
| Parquet (Snappy) | 293 MB | 3.5 s | 5.7 s | 105.7 s |

- ORC is 12.7× smaller than text and 7–14× faster on all three queries. Parquet is 8.4× smaller than text and almost as fast as ORC on Q1 and Q2.
- Q2 reads only two columns and one language; both columnar formats skip the other columns, and the sorted ORC file also lets Hive skip stripes for other languages.
- Parquet's Q3 was the slowest of all. Q3 is the only query that reads the timestamp column. Spark writes Parquet timestamps in the old INT96 format, which Hive has to convert value by value, so the likely cause is that conversion rather than Parquet itself (not verified further).
- Each query ran once on one laptop, so differences of a second or two are noise; the sizes are exact.

## What did not work, and what changed

1. **Hive 4 refused external tables inside its managed warehouse folder.** The managed folder moved to `/radar/hive-managed` (unused); the tables stay under `/radar/warehouse`.
2. **HiveServer2 would not start after `docker compose restart`** ("running as process 8. Stop it first"): the old PID file survived and the new process got the same PID. The PID file now lives on a tmpfs that is emptied on every restart.
3. **The warehouse load stalled in Hive.** `INSERT ... GROUP BY lang, page_title, hour_ts` over one day (65 million rows into about 50 million keys) ran for more than 20 minutes without finishing a single task, with or without map-side aggregation. In Tez local mode every task runs inside the one 3 GB HiveServer2 JVM, and several tasks run at once, each reserving its own sort buffer. Spark loaded both days in 7.5 minutes.
4. **The two-day join (A4) ran out of memory in Hive** even after moving the small side into an analysed temporary table; it runs in Spark in seconds.

Conclusion for the report: in this single-machine setup Hive is used for what it does well (schema and metastore, UDFs, SQL that reduces data to a few groups, columnar scans: 49 million ORC rows summed in 12 seconds), and Spark does the heavy many-key grouping and joining. The same split is common in production, where Hive usually runs on a cluster with Tez or LLAP rather than in local mode.

## Screenshots to add (report/assets/)

- `p3_hive_ui.png`: HiveServer2 web UI (localhost:10002)
- `p3_udf_register.png`: `register.hql` result row and `DESCRIBE FUNCTION EXTENDED`
- `p3_dq_hive.png`: T-D2 table; `p3_dq_spark.png`: the two Spark PASS lines
- `p3_analysis_*.png`: A1–A8 tables (A4 from Spark)
- `p3_b3.png`: the three B3 lines
