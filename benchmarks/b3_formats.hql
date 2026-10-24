-- Benchmark B3: Hive tables over the text and Parquet copies of one day of hourly_views
-- (copies written by benchmarks/b3_make_copies.py). Run by benchmarks/b3_formats.sh.
USE radar;

CREATE EXTERNAL TABLE IF NOT EXISTS hourly_views_text (
  lang STRING, page_title STRING, hour_ts TIMESTAMP,
  views_desktop BIGINT, views_mobile BIGINT, views BIGINT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/radar/benchmarks/b3/hourly_views_text';

CREATE EXTERNAL TABLE IF NOT EXISTS hourly_views_parquet (
  lang STRING, page_title STRING, hour_ts TIMESTAMP,
  views_desktop BIGINT, views_mobile BIGINT, views BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/radar/benchmarks/b3/hourly_views_parquet'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE hourly_views_text;
MSCK REPAIR TABLE hourly_views_parquet;
