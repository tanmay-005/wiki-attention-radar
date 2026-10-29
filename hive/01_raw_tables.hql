-- Steps 3.1 and 3.2: the radar database and external tables over the raw files.
-- Only metadata is created here; the files in HDFS are never touched, so this is
-- safe to re-run at any time (for example after the hiveserver2 container is recreated).
--
-- Run (from docker/):  docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/01_raw_tables.hql

CREATE DATABASE IF NOT EXISTS radar
  COMMENT 'Wikipedia Attention Radar'
  LOCATION '/radar/warehouse/radar.db';

USE radar;

-- 3.1 Hourly dumps: "domain_code page_title views response_size", space separated, gzip.
-- RegexSerDe splits each line; lines that do not match come back as all-NULL rows.
-- The hour is not in the file, only in its name, so queries read it from INPUT__FILE__NAME.
CREATE EXTERNAL TABLE IF NOT EXISTS raw_hourly (
  domain_code STRING,
  page_title  STRING,
  views       BIGINT,
  resp_size   BIGINT
)
COMMENT 'Wikimedia hourly pageview dumps, one gz file per hour'
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = '^(\\S+) (\\S+) (\\d+) (\\d+)$')
STORED AS TEXTFILE
LOCATION '/radar/raw/hourly';

MSCK REPAIR TABLE raw_hourly;

-- 3.2 Per-country daily file: 7 tab-separated columns, no header row.
CREATE EXTERNAL TABLE IF NOT EXISTS raw_country_daily (
  country_name STRING,
  country_code STRING,
  project      STRING,
  page_id      BIGINT,
  page_title   STRING,
  item_id      STRING,
  views        BIGINT
)
COMMENT 'Wikimedia per-country daily pageviews (differential privacy, min 90 views)'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/raw/country_daily';

MSCK REPAIR TABLE raw_country_daily;

-- Checks: 21 hourly partitions, 31 country partitions, and real rows from both tables.
SHOW PARTITIONS raw_hourly;
SHOW PARTITIONS raw_country_daily;

SELECT domain_code, page_title, views,
       regexp_extract(INPUT__FILE__NAME, 'pageviews-(\\d{8})-(\\d{2})', 2) AS hour
FROM raw_hourly
WHERE dt = '2026-10-20' AND domain_code = 'en'
LIMIT 5;

SELECT country_code, country_name, project, page_title, item_id, views
FROM raw_country_daily
WHERE dt = '2026-10-20' AND country_code = 'IN'
LIMIT 5;

-- First real Hive job (Tez): rows per country file. Proves the query engine works.
SELECT dt, COUNT(*) AS n_rows, COUNT(DISTINCT country_code) AS countries
FROM raw_country_daily
WHERE dt BETWEEN '2026-10-19' AND '2026-10-21'
GROUP BY dt
ORDER BY dt;
