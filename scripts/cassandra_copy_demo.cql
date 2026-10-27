-- Step 5.x: export a table to CSV with COPY TO and load it into another table with COPY FROM.
USE radar;

CREATE TABLE IF NOT EXISTS event_rank_import_demo (
  scope text, day date, category text, score double, event_id text, title text, ratio double,
  peak_views bigint, first_seen timestamp, langs set<text>, place text, sparkline list<int>, suspicious boolean,
  PRIMARY KEY ((scope, day), category, score, event_id)
) WITH CLUSTERING ORDER BY (category ASC, score DESC, event_id ASC);
TRUNCATE event_rank_import_demo;

COPY event_rank (scope, day, category, score, event_id, title, ratio, peak_views, first_seen, langs, place, sparkline, suspicious)
TO '/workspace/data/exports/event_rank.csv' WITH HEADER = true;

COPY event_rank_import_demo (scope, day, category, score, event_id, title, ratio, peak_views, first_seen, langs, place, sparkline, suspicious)
FROM '/workspace/data/exports/event_rank.csv' WITH HEADER = true;

SELECT event_id, title, score, langs, sparkline FROM event_rank_import_demo
WHERE scope = 'G' AND day = '2024-06-04' AND category = 'ALL' LIMIT 3;
