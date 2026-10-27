-- Step 5.x: Cassandra queries, CRUD, collections, TTL, counters, ALTER and system tables.
-- Runs after the serving load (bash scripts/run_s6.sh), from scripts/cassandra_demo.sh.
-- Demo rows use scope 'DEMO', so the real data is not touched.

USE radar;

-- 1. The dashboard's main query: one partition (scope, day), already sorted by score
SELECT event_id, title, score, ratio, langs, place
FROM event_rank WHERE scope = 'G' AND day = '2024-06-04' AND category = 'ALL' LIMIT 5;

-- 2. Same table, another scope and category: Politics read by people in India
SELECT event_id, title, score, ratio
FROM event_rank WHERE scope = 'R:IN' AND day = '2024-06-04' AND category = 'Politics' LIMIT 5;

-- 3. Range on a clustering column (no ALLOW FILTERING needed: the partition and category are fixed)
SELECT title, score FROM event_rank
WHERE scope = 'G' AND day = '2024-06-09' AND category = 'Sports' AND score > 450;

-- 4. CRUD on a demo row: create
INSERT INTO event_rank (scope, day, category, score, event_id, title, ratio, peak_views, first_seen,
                        langs, place, sparkline, suspicious)
VALUES ('DEMO', '2024-06-04', 'ALL', 123.4, 'evt_demo_1', 'Demo event', 12.5, 5000,
        '2024-06-04 03:00:00+0000', {'en', 'hi'}, 'Jaipur, India', [1, 2, 3, 40, 5], false);

-- read
SELECT * FROM event_rank WHERE scope = 'DEMO' AND day = '2024-06-04';

-- update: a plain column, a set (add 'ta') and a list (append 6); WRITETIME shows when a cell was written
UPDATE event_rank SET title = 'Demo event (edited)', langs = langs + {'ta'}, sparkline = sparkline + [6]
WHERE scope = 'DEMO' AND day = '2024-06-04' AND category = 'ALL' AND score = 123.4 AND event_id = 'evt_demo_1';
SELECT title, langs, sparkline, WRITETIME(title) FROM event_rank WHERE scope = 'DEMO' AND day = '2024-06-04';

-- delete
DELETE FROM event_rank
WHERE scope = 'DEMO' AND day = '2024-06-04' AND category = 'ALL' AND score = 123.4 AND event_id = 'evt_demo_1';
SELECT count(*) FROM event_rank WHERE scope = 'DEMO' AND day = '2024-06-04';

-- 5. TTL: the table default is 48 h; one row overrides it with 60 s
INSERT INTO trending_now (scope, score, event_id, title, ratio)
VALUES ('DEMO', 1.0, 'evt_demo_ttl', 'Expires in 60 seconds', 1.0) USING TTL 60;
SELECT event_id, title, TTL(title) FROM trending_now WHERE scope = 'DEMO';
SELECT event_id, title, TTL(title) FROM trending_now WHERE scope = 'G' LIMIT 3;

-- 6. Counters: only UPDATE with + or -
SELECT * FROM spike_counts WHERE scope = 'G' AND day = '2024-06-04';
UPDATE spike_counts SET spikes = spikes + 5 WHERE scope = 'DEMO' AND day = '2024-06-04' AND category = 'Sports';
UPDATE spike_counts SET spikes = spikes - 2 WHERE scope = 'DEMO' AND day = '2024-06-04' AND category = 'Sports';
SELECT * FROM spike_counts WHERE scope = 'DEMO' AND day = '2024-06-04';

-- 7. The other serving tables
SELECT * FROM region_index WHERE country = 'IN';
SELECT hour, views, baseline FROM article_hourly
WHERE lang = 'en' AND title = 'Carlos Alcaraz' AND hour >= '2024-06-09 12:00:00+0000' AND hour <= '2024-06-09 22:00:00+0000';
SELECT * FROM article_country_daily LIMIT 5;

-- 8. ALTER TABLE (a new column; existing rows simply have no value for it)
ALTER TABLE event_rank ADD lead_qid text;
SELECT event_id, title, lead_qid FROM event_rank WHERE scope = 'G' AND day = '2024-06-04' AND category = 'ALL' LIMIT 2;
DESCRIBE TABLE event_rank;

-- 9. System tables (table sizes come from nodetool in cassandra_demo.sh: system.size_estimates
--    is only refreshed every few minutes and was still empty right after the load)
SELECT keyspace_name, replication FROM system_schema.keyspaces WHERE keyspace_name = 'radar';
SELECT table_name, default_time_to_live FROM system_schema.tables WHERE keyspace_name = 'radar';
SELECT column_name, kind, position, type FROM system_schema.columns
WHERE keyspace_name = 'radar' AND table_name = 'event_rank';
SELECT cluster_name, release_version, data_center, rack FROM system.local;
