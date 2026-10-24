-- Step 3.7: analysis queries on the warehouse tables (default day 2024-06-03, next day 2024-06-04).
-- Run (from docker/):  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/03_analysis.hql
-- Shows: window functions, CTEs, complex types (ARRAY, MAP) and a TRANSFORM script.
-- A4 (a join of two full days) is in Spark: spark/jobs/day_over_day.py

USE radar;

-- A1. Top 5 articles per language (window function RANK)
SELECT lang, rnk, page_title, views
FROM (
  SELECT lang, page_title, views,
         RANK() OVER (PARTITION BY lang ORDER BY views DESC) AS rnk
  FROM daily_views
  WHERE dt = '${hivevar:day}' AND views >= 1000     -- rank only popular pages: keeps the sort small
) t
WHERE rnk <= 5
ORDER BY lang, rnk;

-- A2. How much of each Wikipedia is read on phones
SELECT lang,
       SUM(views) AS total_views,
       ROUND(100.0 * SUM(views_mobile) / SUM(views), 1) AS mobile_pct
FROM hourly_views
WHERE dt = '${hivevar:day}'
GROUP BY lang
ORDER BY total_views DESC;

-- A3. Busiest hour (UTC) for each language: shows the readers' time zones
SELECT lang, hour(hour_ts) AS peak_hour_utc, views
FROM (
  SELECT lang, hour_ts, SUM(views) AS views,
         ROW_NUMBER() OVER (PARTITION BY lang ORDER BY SUM(views) DESC) AS rn
  FROM hourly_views
  WHERE dt = '${hivevar:day}'
  GROUP BY lang, hour_ts
) t
WHERE rn = 1
ORDER BY lang;

-- A5. Countries reading English Wikipedia the most
SELECT country, country_name, SUM(views) AS views, COUNT(*) AS articles
FROM country_daily
WHERE dt = '${hivevar:day}' AND lang = 'en'
GROUP BY country, country_name
ORDER BY views DESC
LIMIT 10;

-- A6. Top 3 articles per language among readers in India
SELECT lang, page_title, views
FROM (
  SELECT lang, page_title, views,
         ROW_NUMBER() OVER (PARTITION BY lang ORDER BY views DESC) AS rn
  FROM country_daily
  WHERE dt = '${hivevar:day}' AND country = 'IN'
) t
WHERE rn <= 3
ORDER BY lang, views DESC;

-- A7. Complex types: the same Wikidata item read in several languages.
-- collect_set builds an ARRAY<STRING>, str_to_map builds a MAP<STRING,STRING>.
SELECT qid,
       collect_set(lang) AS languages,
       str_to_map(concat_ws(',', collect_set(concat(lang, ':', CAST(views AS STRING)))), ',', ':') AS views_by_lang,
       SUM(views) AS total_views
FROM (
  SELECT qid, lang, SUM(views) AS views
  FROM country_daily
  WHERE dt = '${hivevar:day}' AND qid RLIKE '^Q[0-9]+$'
  GROUP BY qid, lang
) t
GROUP BY qid
HAVING size(collect_set(lang)) >= 3
ORDER BY total_views DESC
LIMIT 10;

-- A8. TRANSFORM: the same kind of per-row logic as a UDF, but as an external script
-- (awk) that Hive streams rows through. Slower than a Java UDF, but needs no compiling.
-- Map-only on purpose (no join, no grouping): the busiest English article-hours of the day.
ADD FILE /workspace/hive/scripts/mobile_share.awk;

SELECT TRANSFORM (lang, page_title, views_mobile, views)
       USING 'awk -f mobile_share.awk'
       AS (lang STRING, page_title STRING, views BIGINT, mobile_pct DOUBLE, label STRING)
FROM hourly_views
WHERE dt = '${hivevar:day}' AND lang = 'en' AND views >= 50000;

-- A4 (biggest day-over-day jumps) joins 8 million rows per day. In Hive's local mode
-- every task shares one 3 GB JVM, and the join ran out of memory even with a map-join
-- setup, so A4 runs in Spark instead: spark/jobs/day_over_day.py
