# Phase 7 results: full run and validation

Notes and numbers from Phase 7, for the final report. Data: hourly page views for 6–26 October 2026 in 14 Wikipedia languages (detection from 13 October) and per-country daily views for 26 September – 26 October 2026.

## Full run: time per stage

The full rebuild (`scripts/rebuild_languages.sh`, 28 October 2026) re-processed the raw dumps already in HDFS for 14 languages. Nothing was downloaded again.

| Stage | Input | Time |
|---|---|---|
| Namespace names of the 14 wikis (`fetch_namespaces.py`) | 14 siteinfo requests | 20 s |
| Remove old MapReduce output and warehouse partitions | 21 + 31 days | 1.3 min |
| MapReduce MR1 + MR2 (filter, normalise, daily sums) | 21 days of hourly dumps | 141 min (364–428 s per day) |
| Spark warehouse load to ORC (hourly, daily, country) | 21 + 31 days | 71 min |
| Spark S1–S5b (series, spikes, enrichment, events, context) | | 16 min |
| Serving load S6 (Cassandra + MongoDB) | 14,407 events | 81 s |
| **Total** | | **232 min** |

MapReduce dominates: every hourly dump is read whole (all languages and projects) to keep 14 wikis. The earlier download for the same period took about 6.5 hours on top of this.

One mistake during this run is worth recording. When the language list was extended, the per-language thresholds of Hindi, Tamil, Japanese and Portuguese were lost from `settings.yaml`, so those languages fell back to the English threshold of 1,000 views an hour (Hindi found 1 candidate page instead of 278). The run log shows the thresholds in use, which is how it was caught. After restoring them only Spark S1–S6 had to run again (about 45–50 minutes): 21,943 candidate pages, 10,734 global spike episodes, 13,814 country episodes and 14,407 events.

## How the validation works

- **Known events** (`data/validation/known_events_2026-10.csv`, 64 events) were taken from Wikipedia's Current events pages and deaths list for 13–26 October (`scripts/dump_news.py`), by fixed rules and before looking at the radar's output:
  - news with its own article (an election, a disaster, a shooting, a final, a launch, an award): 37
  - the same for India, checked in the Readers in India list: 5
  - every Indian on the deaths list: 11
  - deaths whose entry mentions prime minister, Nobel, Emmy, Olympic champion or Hall of Fame: 11
- A known event is **found** when a detected story contains its Wikipedia article (matched by Wikidata item) and was first detected between the start of the day before the listed day and 48 hours after it ends.
- **Rank** is the position of that story in its list (worldwide, or readers in India) on the days it was listed.
- **Precision@50**: the top 50 worldwide stories of the period, labelled real or not. A story is real when (a) a Wikipedia news or deaths line explains it, (b) the edit summaries of its article name what happened (died, married, appointed, a record, a TV broadcast), or (c) its article had an edit burst around the spike: at least 10 edits and at least five times its normal rate. Edit histories come from `scripts/top50_evidence.py`. Anything else counts as not real (unexplained), as the test plan says.

## Results

| Metric | Result | Target |
|---|---|---|
| Recall | 35 of 64 = 55% | 75% |
| Precision@50 | 27 of 50 = 54% | 80% |
| Median detection delay | 0 h after the listed day starts (UTC) | report it |
| Found events in the top 10 of their day | 12 of 35 | |
| Indian events ranked higher in Readers in India than worldwide | 6 of 6 | higher in R:IN |

Recall by group:

| Group | Found |
|---|---|
| News with its own article | 23 of 37 (62%) |
| News in India | 1 of 5 (20%) |
| Deaths in India | 6 of 11 (55%) |
| Notable deaths | 5 of 11 (45%) |

Both quality targets are missed. The rules were not changed after seeing the results, because tuning them on the same test set would only make the numbers look better.

## Why known events were missed

S7 records a reason for every miss (`benchmarks/results/validation_2026-10.csv`, and the "Why missed" column on the Validation page):

| Reason | Misses |
|---|---|
| Peak below 1,000 views an hour on English Wikipedia | 26 |
| … of which between 500 and 999 views an hour | 9 |
| Detected, but outside the window | 2 |
| Busy before the day too, so no spike against its normal level | 1 |

- **Volume.** Most misses are real events that did not draw enough readers on English Wikipedia: new articles about a train derailment in Pakistan (105 views/h at peak), the Irish public sector strike (161), the Peshawar school roof collapse (106), the Kolkata Durga Puja crowd crush (180). Nine came close (the Trappes shootings at 973, Hurricane Isaias at 888, the Karak church shooting at 884). The 1,000 views/h threshold for English was set in Phase 4 to keep the top of the list clean; a lower one would find more of these and also let in more noise. This is the main trade-off of the method.
- **Death dates vs. announcements.** The deaths list gives the day a person died. Yasuo Fukuda died on 22 October but his death was reported on 26 October, and the radar found it that day (it is #31 in the top 50). Abhijeet Dipke was detected 5 days after his detention, when he was in the news again.
- **Languages we do not read.** Nikolay Koshman (Russia), the Czech Senate election and the Romanian prime minister were read mostly on Wikipedias that are not processed.
- **End of the data.** Susan Saint James and David Rigert died on 26 October, the last day of data, so any spike came after it.

What works well: the biggest stories are found on the day and at the top (2026 Ballon d'Or #1, Serbian election #1, Tom Skerritt #1, Lula #2, the PHP2Shell vulnerability #4), and every Indian event found ranks higher among readers in India than worldwide (Deva #1 vs #7, Chef Damu #1 vs #12, Jogen Chowdhury #9 vs #446).

## What the top 50 shows

- 23 of the top 50 are from Japanese Wikipedia, mostly actors, voice actors and singers. 14 of the 23 unexplained stories are Japanese pages, and 9 of those got no edits at all around the spike. These are probably television appearances (Japanese TV sends viewers to Wikipedia), but nothing in the data confirms it, so they count as not real.
- 49 of the top 50 hit the ×100 cap on the jump, so at the top of the list the order comes almost entirely from the number of extra readers. The cap stops one-off counting glitches from dominating, but it also means the ratio no longer separates the top stories.
- Of the 15 top stories whose article got two edits or fewer around the spike, none could be explained. Requiring some editing activity as a second signal (real events get written about while people read them) is the clearest improvement for future work.

## Benchmarks on the final data (20 October 2026)

All benchmarks were run again on one day of the final data (`benchmarks/run_benchmarks.sh 2026-10-20`, 35 minutes, plus B3 in Hive). MR1 keeps 57.3 million rows for that day in 14 languages; the daily aggregation gives 8,215,230 (language, title) rows and 210,930,243 views. In every benchmark both variants produced exactly the same result. Each setting ran once (B3: best of three), on one laptop, so differences of a few seconds are noise.

| Benchmark | Result |
|---|---|
| B1 combiner in MR2 | Shuffle 128 MB with the combiner, 282 MB without (55% smaller); run time 184 s vs 182 s |
| B2 map-output compression in MR1 | Shuffle: none 2,816 MB, Snappy 908 MB, Gzip 597 MB. CPU: none 813 s, Snappy 746 s, Gzip 1,031 s. Run time: none 364 s, Snappy 329 s, Gzip 422 s |
| B3 storage formats in Hive (one day of `hourly_views`) | Size: text 2,229 MB, ORC 186 MB, Parquet 275 MB. Q1 count + sum: 24.2 / 2.1 / 2.9 s. Q2 top 10 of one language: 31.7 / 4.7 / 7.3 s. Q3 views per hour: 65.0 / 4.7 / 94.0 s |
| B4 MapReduce vs Spark (daily aggregation) | MR2 on YARN 180 s, Spark DataFrame 79 s (2.3 times faster) |
| B5 RDD vs DataFrame (same aggregation) | RDD `reduceByKey` 206 s, DataFrame `groupBy` 58 s (3.6 times faster), although the DataFrame shuffled more (211 MB vs 172 MB) |

What they show:

- **B1.** The combiner halves the data that crosses the shuffle but does not save time here. On one machine the shuffle never leaves the host, and reading and decompressing the input dominates. On a real cluster, where the shuffle goes over the network, the smaller shuffle would matter much more.
- **B2.** Gzip makes the smallest shuffle but costs the most CPU; Snappy is cheap enough to be the fastest overall. That is why the jobs use Snappy for the shuffle and Gzip for the stored output.
- **B3.** ORC is 12 times smaller than plain text and the fastest on all three queries, because a columnar file lets Hive read only the columns a query uses and skip stripes by their min/max statistics. Parquet is close on Q1 and Q2 but very slow on Q3, which groups by the timestamp column; the likely cause is Hive converting the INT96 timestamps Spark writes into Parquet. This is why the warehouse is stored as ORC.
- **B4 and B5.** Spark beats Hadoop Streaming by keeping the work inside the JVM: no Python process per task and no job setup on YARN for each stage. Inside Spark, the DataFrame API beats the RDD API for the same reason: the RDD version sends every row through Python functions, while the DataFrame version runs as generated JVM code planned by Catalyst.

## Serving queries (B6)

`benchmarks/b6_latency.py` ran the dashboard's own queries 30 times each against the loaded data (day 26 October, story "2026 Ballon d'Or"). Times are measured by the client; the Cassandra server time comes from query tracing, the MongoDB plan from `explain("executionStats")`.

| Store | Query | Rows | Median | 95th percentile | How it is answered |
|---|---|---|---|---|---|
| Cassandra | Top stories worldwide for one day and category | 100 | 11.5 ms | 19.1 ms | one partition, already sorted by score |
| Cassandra | Top stories read in India | 57 | 5.6 ms | 7.2 ms | one partition |
| Cassandra | Stories per category (counters) | 8 | 2.1 ms | 3.4 ms | one partition |
| Cassandra | Hourly series of one article (48 h) | 25 | 3.1 ms | 4.4 ms | one partition, range on the clustering column |
| Cassandra | Trending now (TTL table) | 10 | 2.0 ms | 3.3 ms | one partition |
| MongoDB | One story by `_id` (detail panel) | 1 | 0.5 ms | 0.7 ms | IDHACK (the `_id` index) |
| MongoDB | Stories located in India | 350 | 1.9 ms | 2.3 ms | IXSCAN on `location.country, location.region` |
| MongoDB | Stories containing one Wikidata item | 1 | 0.6 ms | 0.7 ms | IXSCAN on `articles.qid` |
| MongoDB | One category, newest first | 50 | 0.8 ms | 1.0 ms | IXSCAN on `category, first_detected` (no sort in memory) |

Every query is either a single-partition read in Cassandra or an index lookup in MongoDB; none scans a table or a collection. The databases answer in milliseconds, so almost all of the page's 725 ms (uncached) is Streamlit building the page and its charts.
