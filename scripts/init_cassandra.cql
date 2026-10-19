-- Wikipedia Attention Radar: Cassandra schema (see docs/03_DATA_MODEL.md §4)
-- Run: docker compose exec cassandra cqlsh -f /workspace/scripts/init_cassandra.cql

CREATE KEYSPACE IF NOT EXISTS radar
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE radar;

-- Q1 ranked events per scope + day + category (main list)
CREATE TABLE IF NOT EXISTS event_rank (
  scope       text,
  day         date,
  category    text,
  score       double,
  event_id    text,
  title       text,
  ratio       double,
  peak_views  bigint,
  first_seen  timestamp,
  langs       set<text>,
  place       text,
  sparkline   list<int>,
  suspicious  boolean,
  PRIMARY KEY ((scope, day), category, score, event_id)
) WITH CLUSTERING ORDER BY (category ASC, score DESC, event_id ASC);

-- Q2 hourly series for one article (global charts)
CREATE TABLE IF NOT EXISTS article_hourly (
  lang     text,
  title    text,
  hour     timestamp,
  views    int,
  baseline int,
  PRIMARY KEY ((lang, title), hour)
) WITH CLUSTERING ORDER BY (hour ASC);

-- Q3 daily series for one entity in one country (country charts)
CREATE TABLE IF NOT EXISTS article_country_daily (
  qid      text,
  country  text,
  day      date,
  views    int,
  baseline int,
  PRIMARY KEY ((qid, country), day)
);

-- Q4 trending now (TTL demo, rows expire after 48 h)
CREATE TABLE IF NOT EXISTS trending_now (
  scope    text,
  score    double,
  event_id text,
  title    text,
  ratio    double,
  PRIMARY KEY ((scope), score, event_id)
) WITH CLUSTERING ORDER BY (score DESC, event_id ASC)
  AND default_time_to_live = 172800;

-- Q5 spike counts per category (counter demo)
CREATE TABLE IF NOT EXISTS spike_counts (
  scope    text,
  day      date,
  category text,
  spikes   counter,
  PRIMARY KEY ((scope, day), category)
);

-- Q6 regions available per country (state dropdown)
CREATE TABLE IF NOT EXISTS region_index (
  country      text,
  region_code  text,
  region_name  text,
  event_count  int,
  PRIMARY KEY ((country), region_code)
);
