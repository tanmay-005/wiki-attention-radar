-- Step 3.3: register the Java UDFs as permanent functions in the radar database.
-- Build first (inside edge): bash hive/udf/build.sh
-- Run (from docker/):        docker compose exec hiveserver2 bash /workspace/hive/run.sh hive/udf/register.hql

USE radar;

DROP FUNCTION IF EXISTS radar.normalize_title;
CREATE FUNCTION radar.normalize_title AS 'radar.udf.NormalizeTitle'
  USING JAR 'hdfs://namenode:8020/radar/lib/radar-udf.jar';

DROP FUNCTION IF EXISTS radar.is_content_page;
CREATE FUNCTION radar.is_content_page AS 'radar.udf.IsContentPage'
  USING JAR 'hdfs://namenode:8020/radar/lib/radar-udf.jar';

-- Expected: Virat Kohli's career | Rock & Roll | false | false | false | true | true
SELECT radar.normalize_title('Virat_Kohli%27s_career') AS t1,
       radar.normalize_title('Rock_%26_Roll')          AS t2,
       radar.is_content_page('en', 'Category:1990s_births')  AS en_category,
       radar.is_content_page('hi', 'श्रेणी:भारत')             AS hi_category,
       radar.is_content_page('en', 'Main_Page')              AS en_main,
       radar.is_content_page('en', 'Furiosa:_A_Mad_Max_Saga') AS en_article_with_colon,
       radar.is_content_page('ja', 'マッドマックス:フュリオサ')   AS ja_article_with_colon;

DESCRIBE FUNCTION EXTENDED radar.normalize_title;
