-- Make Hive's partition list match the folders in HDFS again: partitions whose folder was deleted
-- (the June 2024 days, removed by scripts/remove_2024.sh) are dropped, new folders are added.
USE radar;

MSCK REPAIR TABLE raw_hourly SYNC PARTITIONS;
MSCK REPAIR TABLE raw_country_daily SYNC PARTITIONS;
MSCK REPAIR TABLE staged_hourly SYNC PARTITIONS;
MSCK REPAIR TABLE staged_daily SYNC PARTITIONS;
MSCK REPAIR TABLE staged_topn SYNC PARTITIONS;
MSCK REPAIR TABLE hourly_views SYNC PARTITIONS;
MSCK REPAIR TABLE daily_views SYNC PARTITIONS;
MSCK REPAIR TABLE country_daily SYNC PARTITIONS;
MSCK REPAIR TABLE hourly_views_text SYNC PARTITIONS;
MSCK REPAIR TABLE hourly_views_parquet SYNC PARTITIONS;

SELECT 'hourly_views' AS tbl, MIN(dt) AS first_day, MAX(dt) AS last_day, COUNT(DISTINCT dt) AS days FROM hourly_views
UNION ALL SELECT 'country_daily', MIN(dt), MAX(dt), COUNT(DISTINCT dt) FROM country_daily;
