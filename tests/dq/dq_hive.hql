-- Step 3.6: data-quality checks on one day (default 2024-06-03, change with --hivevar day=...).
-- The two heavy checks (T-D3, T-D4) are in tests/dq/dq_spark.py.
-- Each query prints its numbers and a PASS/FAIL column.
-- Run (from docker/):  docker compose exec hiveserver2 bash /workspace/hive/run.sh tests/dq/dq_hive.hql
--
-- T-D2 reads the whole raw day (about 157 million lines), so expect it to take several minutes.

USE radar;
-- always count the real rows, never metadata
SET hive.compute.query.using.stats=false;

-- T-D1 + T-D2: all 24 hourly files are there, and MR1 neither lost nor invented views.
-- The raw side applies the same rules as MR1 using the Java UDFs, so the sums must be equal.
WITH raw AS (
  SELECT lang, SUM(views) AS views, COUNT(DISTINCT file_name) AS files
  FROM (
    SELECT split(domain_code, '\\.')[0] AS lang, views, INPUT__FILE__NAME AS file_name
    FROM raw_hourly
    WHERE dt = '${hivevar:day}'
      AND views IS NOT NULL
      AND domain_code RLIKE '^(en|hi|ta|ja|pt)(\\.m)?$'
      AND radar.is_content_page(split(domain_code, '\\.')[0], page_title)
      AND radar.normalize_title(page_title) <> ''
  ) kept
  GROUP BY lang
),
staged AS (
  SELECT lang, SUM(views) AS views
  FROM staged_hourly
  WHERE dt = '${hivevar:day}'
  GROUP BY lang
)
SELECT 'T-D2 view conservation' AS check_name,
       coalesce(r.lang, s.lang) AS lang,
       r.files AS raw_files,
       r.views AS raw_views,
       s.views AS staged_views,
       IF(r.views = s.views AND r.files = 24, 'PASS', 'FAIL') AS result
FROM raw r FULL OUTER JOIN staged s ON r.lang = s.lang
ORDER BY lang;

-- Loading the warehouse kept every view (staged text -> ORC)
SELECT 'Warehouse load' AS check_name,
       s.views AS staged_views, h.views AS warehouse_views,
       IF(s.views = h.views, 'PASS', 'FAIL') AS result
FROM (SELECT SUM(views) AS views FROM staged_hourly WHERE dt = '${hivevar:day}') s
CROSS JOIN (SELECT SUM(views) AS views FROM hourly_views WHERE dt = '${hivevar:day}') h;

-- T-D3 (daily = sum of hourly) and T-D4 (no duplicate hours) group tens of millions of rows,
-- which is too heavy for Hive's local engine here; they run in Spark: tests/dq/dq_spark.py

-- Sanity: every hour of the day has data for every language (24 x 5 = 120)
SELECT 'Hours per language' AS check_name,
       COUNT(DISTINCT concat(lang, '|', CAST(hour_ts AS STRING))) AS lang_hours,
       IF(COUNT(DISTINCT concat(lang, '|', CAST(hour_ts AS STRING))) = 120, 'PASS', 'FAIL') AS result
FROM hourly_views WHERE dt = '${hivevar:day}';

-- T-D10: country data has only our languages, no missing or negative counts, and the
-- differential-privacy floor (90 views) holds. qid coverage is reported, not required.
SELECT 'T-D10 country data' AS check_name,
       COUNT(*) AS n_rows,
       COUNT(DISTINCT country) AS countries,
       SUM(IF(views IS NULL OR views < 90, 1, 0)) AS bad_views,
       SUM(IF(lang NOT IN ('en', 'hi', 'ta', 'ja', 'pt'), 1, 0)) AS bad_lang,
       ROUND(100.0 * SUM(IF(qid RLIKE '^Q[0-9]+$', 1, 0)) / COUNT(*), 1) AS pct_with_qid,
       IF(COUNT(*) > 0
          AND SUM(IF(views IS NULL OR views < 90, 1, 0)) = 0
          AND SUM(IF(lang NOT IN ('en', 'hi', 'ta', 'ja', 'pt'), 1, 0)) = 0, 'PASS', 'FAIL') AS result
FROM country_daily WHERE dt = '${hivevar:day}';
