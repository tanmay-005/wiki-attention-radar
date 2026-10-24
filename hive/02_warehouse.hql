-- Steps 3.4 and 3.5 (tables): staged tables over the MapReduce output and the
-- columnar warehouse tables. Metadata only, safe to re-run. The ORC data is written
-- by spark/jobs/load_warehouse.py; run this file again afterwards so MSCK REPAIR
-- registers the new dt= partitions.
--
-- Run (from docker/):  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/02_warehouse.hql
--
-- Every table is EXTERNAL with an explicit location, including the ORC warehouse:
--  * Hive 4 turns plain managed tables into ACID tables or moves them to its own
--    external folder; external tables keep the files exactly where Spark expects them.
--  * The metastore lives inside the hiveserver2 container. If the container is
--    recreated, re-running these DDL files brings every table back with its data.

USE radar;

-- 3.4 Staged: MR1 / MR2 / MR3 output (tab separated, gzip; Hive reads .gz directly)
CREATE EXTERNAL TABLE IF NOT EXISTS staged_hourly (
  lang STRING, platform STRING, page_title STRING, hour_ts STRING, views BIGINT
)
COMMENT 'MR1 output: one row per language, platform, title and hour'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/staged/hourly';

CREATE EXTERNAL TABLE IF NOT EXISTS staged_daily (
  lang STRING, page_title STRING, day STRING, views BIGINT
)
COMMENT 'MR2 output: one row per language, title and day'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/staged/daily';

CREATE EXTERNAL TABLE IF NOT EXISTS staged_topn (
  lang STRING, day STRING, rank_no INT, views BIGINT, page_title STRING
)
COMMENT 'MR3 output: top 100 titles per language and day'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/staged/topn';

MSCK REPAIR TABLE staged_hourly;
MSCK REPAIR TABLE staged_daily;
MSCK REPAIR TABLE staged_topn;

-- 3.5 Warehouse: ORC, desktop and mobile side by side, proper TIMESTAMP / DATE types
CREATE EXTERNAL TABLE IF NOT EXISTS hourly_views (
  lang          STRING,
  page_title    STRING,
  hour_ts       TIMESTAMP,
  views_desktop BIGINT,
  views_mobile  BIGINT,
  views         BIGINT
)
COMMENT 'Hourly views per article, both platforms (input for Spark S1)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/radar/warehouse/radar.db/hourly_views'
TBLPROPERTIES ('orc.compress' = 'ZLIB', 'transactional' = 'false');

CREATE EXTERNAL TABLE IF NOT EXISTS daily_views (
  lang       STRING,
  page_title STRING,
  day        DATE,
  views      BIGINT
)
COMMENT 'Daily views per article'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/radar/warehouse/radar.db/daily_views'
TBLPROPERTIES ('orc.compress' = 'ZLIB', 'transactional' = 'false');

CREATE EXTERNAL TABLE IF NOT EXISTS country_daily (
  country      STRING,
  country_name STRING,
  lang         STRING,
  page_id      BIGINT,
  page_title   STRING,
  qid          STRING,
  views        BIGINT
)
COMMENT 'Daily views per country and article, configured languages only'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/radar/warehouse/radar.db/country_daily'
TBLPROPERTIES ('orc.compress' = 'ZLIB', 'transactional' = 'false');

MSCK REPAIR TABLE hourly_views;
MSCK REPAIR TABLE daily_views;
MSCK REPAIR TABLE country_daily;

SHOW TABLES;
SHOW PARTITIONS staged_hourly;

-- What the warehouse holds (all zero before the Spark load has run)
SELECT dt, COUNT(*) AS n_rows, SUM(views) AS views FROM hourly_views GROUP BY dt ORDER BY dt;
SELECT dt, COUNT(*) AS n_rows, SUM(views) AS views FROM daily_views  GROUP BY dt ORDER BY dt;
SELECT dt, COUNT(*) AS n_rows, COUNT(DISTINCT country) AS countries, SUM(views) AS views
FROM country_daily GROUP BY dt ORDER BY dt;
