# Phase 5 results: serving with Cassandra and MongoDB

Notes and numbers from the Phase 5 runs, for the final report. Cassandra 4.1.12 (one node, 1 GB heap) and MongoDB 7.0 run in their own containers. `spark/jobs/s6_load_serving.py` (started with `scripts/run_s6.sh`) loads both from the Phase 4 output in HDFS. The full output of every demo is in `report/outputs/`.

## Why two databases

The dashboard asks two kinds of question.

- **"Give me the top events for this scope, day and category, already sorted."** This is a fixed query asked over and over, and it should touch exactly one partition. That is what Cassandra is built for: the table is designed around the query, and the sort order is stored on disk.
- **"Show me everything about this event."** That is a nested object: articles, countries, location and the hourly curve. MongoDB stores it as one document, so the detail panel reads it in a single lookup.

## Cassandra tables (query-first design)

| Table | Partition key | Clustering | Rows | Size on disk | Partitions | Written with |
|---|---|---|---|---|---|---|
| `event_rank` | (scope, day) | category, score DESC, event_id | 89,660 | 12.0 MB | 2,538 | Python driver, prepared INSERT |
| `article_hourly` | (lang, title) | hour | 4,533,984 | 43.2 MB | 8,965 | Spark Cassandra connector |
| `article_country_daily` | (qid, country) | day | 503,564 | 4.8 MB | 16,406 | Spark Cassandra connector |
| `trending_now` | scope | score DESC, event_id | 643 | 36 KB | 163 | prepared INSERT ... USING TTL 172800 |
| `spike_counts` | (scope, day) | category | counters | 207 KB | 2,361 | prepared UPDATE ... spikes + 1 |
| `region_index` | country | region_code | 430 | 17 KB | 90 | prepared INSERT |

Sizes and partition counts come from `nodetool tablestats` after `nodetool flush`.

- **event_rank.** Every event is written once for each of its scopes (G, R:IN, A:IN, A:IN:IN-RJ ...), each day it is active, and twice per day (category 'ALL' and its own category). That duplicates rows on purpose so that every dashboard filter is a single-partition read. The clustering order puts the best score first, so `LIMIT 50` needs no sorting at read time. Collection columns hold the languages (`set<text>`) and a 12-point sparkline (`list<int>`).
- **article_hourly / article_country_daily.** These hold the series for the detail charts, with a baseline computed the same way as in detection: the median of the previous 168 hours, or of the previous 14 days.
- **trending_now** shows TTL. Rows expire 48 hours after they are written, and `TTL(title)` showed 172,731 seconds left.
- **spike_counts** shows counters. They can only be changed with `UPDATE ... SET spikes = spikes + n`. On 2024-06-04 the global scope had 804 spikes: 298 Politics, 197 Other, 173 Entertainment, 113 Sports.

## Loading (S6)

| Step | Rows | Time |
|---|---|---|
| event_rank (prepared INSERT, 64 requests in flight) | 89,660 | 13–15 s |
| spike_counts (counter UPDATE) | 78,698 | 12–13 s |
| trending_now, region_index | 643, 430 | under 1 s |
| MongoDB events (bulk ReplaceOne with upsert) | 16,822 | 1 s |
| article_hourly (Spark connector) | 4,533,984 | 48 s |
| article_country_daily (Spark connector) | 503,564 | 22 s |
| Whole job | | 114 s (39 s with `--skip-series`) |

The two series tables are large, so they go through the Spark Cassandra connector, which writes in parallel batches grouped by partition: about 94,000 rows a second for `article_hourly`. The small tables need per-row control (TTL, counters), so they use the Python driver with prepared statements: the query is parsed once and each row only sends its values.

Re-runs give the same result. The four small Cassandra tables are truncated first. The series tables and MongoDB are written with upserts: an existing key is overwritten. Mongo documents left over from an earlier run are deleted by their `pipeline_run` time.

**Check.** After the load, the top 5 for scope G on 2024-06-04 were read back from Cassandra and compared with the Spark output: same events, same order. MongoDB held 16,822 events, as expected.

## Cassandra demos (`scripts/cassandra_demo.sh`)

- **Queries.** The main dashboard query (one partition, already sorted). The same table for another scope (Politics read in India: the 2019 and 2024 election pages, Chandrababu Naidu, Samajwadi Party, JD(U)). A range on a clustering column (Sports on 06-09 with score > 450: Alcaraz, Zverev, Jack Nicklaus), which needs no `ALLOW FILTERING` because the partition and category are fixed.
- **CRUD** on a demo row: INSERT, SELECT, UPDATE of a text column plus adding to a set (`langs + {'ta'}`) and appending to a list (`sparkline + [6]`), `WRITETIME`, DELETE, then a count of 0.
- **TTL and counters** (above), a range read from `article_hourly` (Carlos Alcaraz on 06-09: 174,182 views at 18:00 UTC against a baseline of 828), and `region_index` for India (30 states and territories).
- **ALTER TABLE** `event_rank ADD lead_qid text`. Existing rows show `null` for the new column, and nothing is rewritten. Running the demo a second time gives "column already exists", as expected.
- **System tables.** `system_schema.keyspaces` (SimpleStrategy, replication factor 1), `system_schema.tables` (only `trending_now` has a default TTL), `system_schema.columns` (partition key, clustering and regular columns of `event_rank`), and `system.local` (cluster "radar", version 4.1.12).
- **COPY.** `COPY event_rank TO` a CSV exported 89,660 rows in 2.4–2.9 s. `COPY ... FROM` loaded all 89,660 into `event_rank_import_demo` with 0 skipped: 3.4 s on the first run, 52 s on the second (the cause was not investigated). Sets and lists survive the round trip.

## MongoDB (`scripts/mongo_demo.sh`)

Collections: `events` 16,822, `pages` 16,804 (S4 page cache), `wikidata_cache` 26,608 (S4 Wikidata items), `meta` (one document: data range, as-of day, categories, run time).

- **Document shape.** One event holds its articles (lead and members, in several languages), the countries where it spiked, its location, the ±48-hour curve and its scopes. The top global event, Carlos Alcaraz, has 3 language articles, spikes in 36 countries and 38 scopes.
- **Index use.** "Top 5 events read in India" (`{scopes: "R:IN"}` sorted by score) runs as an IXSCAN on `scopes_1_score_-1`: 5 documents examined for 5 returned, in under 1 ms. `scopes` is an array, so this is a multikey index.
- **Other queries.** Events about a region (`location.region = "IN-RJ"`: Barmer, Jaipur Rural, Jodhpur and Kota Lok Sabha constituencies). Events containing an item (`articles.qid`): Carlos Alcaraz appears in 4 events (first detected on 06-02, 06-04 and twice on 06-07): separate spikes during the French Open, which he won on 06-09.
- **Aggregation.** Events, average jump and best score per category (`$group`). Countries with the most spiking events (`$unwind` on the countries array, then `$group`): United States 4,849, Japan 2,982, United Kingdom 2,499, India 2,279.
- **BSON types.** `_id` string, dates as `date`, `double`, `int`, `array`, embedded `object`, `bool`, and `long` in the demo document.
- **CRUD.** insertOne, find, updateOne with `$set`, `$push` and `$inc`, deleteOne, then a count of 0.
- **Export and import.** `mongoexport` wrote the 2,201 events read in India to JSON, and `mongoimport` loaded all 2,201 into `events_import_demo`. Dates came back as real dates, because mongoexport writes them as `{"$date": ...}` in extended JSON.

## What did not work, and what changed

1. **A region code that is not a state.** India's region list included `IN.AP.EG` (East Godavari district). Wikidata keeps it in the same property as state codes (P300). Only codes in the ISO 3166-2 format (`IN-RJ`) are accepted now, in S4 and in the loader, so the list has 30 real states and territories.
2. **`system.size_estimates` was empty.** Cassandra refreshes it only every few minutes, so right after the load it listed 102 ranges with zeros. Table sizes now come from `nodetool flush` and `nodetool tablestats`.
3. **Writing nulls.** Hours before the baseline window have no baseline. Writing a null into Cassandra creates a tombstone (a delete marker), so the connector runs with `spark.cassandra.output.ignoreNulls=true` and leaves those cells out instead.
