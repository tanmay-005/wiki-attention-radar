"""About page: what the project is, the data, the method and its limits."""
import streamlit as st

from data_access import CFG

P = CFG["period"]
st.title("About Attention Radar")
st.markdown(f"""
Attention Radar finds topics that people suddenly started reading about on Wikipedia, worldwide and country by
country, and groups related articles into stories. It is a Big Data Analytics course project built on the Hadoop
ecosystem, using public Wikimedia data. The data loaded now covers **{P['hourly_start']:%d %b} to
{P['hourly_end']:%d %b %Y}** (hourly) and {P['country_start']:%d %b} to {P['country_end']:%d %b %Y} (per country);
spikes are detected from {P['detect_start']:%d %b}.

#### Data
- **Hourly pageviews** for English, Hindi, Tamil, Japanese and Portuguese Wikipedia (desktop and mobile):
  about 157 million raw lines and 1.3 GB compressed a day.
- **Daily pageviews per country** (Wikimedia's privacy-protected dataset): counts under 90 views a day are not
  published, and there is nothing below country level.
- **Wikidata** for each article's item: what it is (for the category), where it is (for country and state), and
  which items it is linked to (for grouping).
- **Wikipedia itself** for each story's short description, first lines and picture, and for the news: the daily
  "Current events" pages and the monthly "Deaths in" lists, matched to stories through Wikidata.

#### How it works
1. **HDFS** stores the raw files. **MapReduce** jobs clean them and add up views per hour and per day.
2. **Hive** holds the tables (ORC format) and two custom functions that clean titles exactly like the MapReduce jobs.
3. **Spark** builds each article's hourly series, compares every day with the article's own normal level
   (the median of the previous 7 days), finds spikes and groups them into stories with Wikidata.
4. **Cassandra** serves the ranked lists and the series behind the charts; **MongoDB** stores each story as one
   document. This dashboard reads both.

#### What a spike means here
A spike is a jump against the article's **own** normal level, not raw popularity: a small page going from 50 to
5,000 views an hour counts, a big page staying big does not. Ranking uses the jump (capped at ×100) weighted by
the extra views, so a large jump only ranks high when many people were involved.

#### Readers in vs Events about
- **Readers in a country** uses the per-country data: what people in that country read more than usual.
  It is daily and has no states.
- **Events about a place** uses Wikidata to locate a story's subject, for example a constituency in Rajasthan,
  whoever read it. This is the only way to filter by state or region.

#### Limits
- The data is loaded in batches, not live: Wikimedia publishes hourly files about an hour late and per-country
  files a day late. "As of" picks the day you look from.
- Only five language editions are covered, so stories mostly read in other languages are missed.
- About a third of stories are in "Other": places, diseases, history and similar topics that fit none of the six
  categories.
- Related pages are only grouped when Wikidata links them, so one real-world event can show as several stories.
- A news line is only shown when Wikipedia's own news pages mention the story; spikes from TV, social media or
  search trends often have none.
- One-hour spikes read almost only on desktop are flagged as possible bot traffic.
""")
