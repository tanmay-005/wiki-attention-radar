# Phase 6 results: the dashboard, story context and the switch to 2026 data

Notes from Phase 6, for the final report. The dashboard is a Streamlit app (`dashboard/app.py`) that runs on Windows and reads Cassandra and MongoDB through the ports Docker publishes. It has four pages: Overview, Validation, Benchmarks and About.

## Moving to recent data (October 2026)

The first version of the dashboard showed June 2024, the period used while building the pipeline. For the final version the whole pipeline was run again on recent data:

| Period | Dates |
|---|---|
| Hourly page views (baseline + detection) | 6–26 Oct 2026 (21 days) |
| Detection | 13–26 Oct 2026 (14 days) |
| Per-country daily views | 26 Sep – 26 Oct 2026 (31 days) |

`scripts/run_all.sh` does the whole run without supervision (download, HDFS, MapReduce, warehouse, Spark S1–S5b, serving). The download alone took about 6.5 hours, at about 2.3 MB/s, because Wikimedia allows only two parallel downloads per client. The June 2024 data is still in HDFS and in the Hive tables, so the two periods can be compared.

A side finding: daily views in October 2026 are about 210–225 million across our languages, against 250–285 million in June 2024, even though 2026 includes nine more languages. This matches the drop in human page views that Wikimedia itself has reported, and it lowers how many articles cross the fixed hourly thresholds.

## What each story is about (S5b)

A list of spiking titles does not say why people were reading. `spark/jobs/s5b_context.py` adds two things to every event:

- **About:** the short description, the first lines and a thumbnail of the lead article, from the Wikipedia API (20 titles per call, cached in MongoDB `page_info`). Japanese, Hindi, Tamil and Portuguese Wikipedia have no short descriptions of their own, so the English Wikidata description is used for them: 4,993 of 5,637 filled this way.
- **Why people were reading:** lines from Wikipedia's own news pages. The daily `Portal:Current events` pages and the deaths list are parsed, and a line is attached to an event when they share a Wikidata item and the dates are close. The monthly deaths page for October 2026 had become a redirect to the yearly page, so the parser reads the `== October ==` section of `Deaths in 2026`.

## Layout and colours

The layout follows common analytics dashboards: filters in a sidebar, four KPI tiles, then pairs of panels (new stories per day next to the category breakdown, the ranked table next to the selected story, the reader map next to the language breakdown). Clicking a category bar filters the page; clicking it again clears the filter. "Open full story" shows the news lines, the hourly chart with its normal level, the reader countries and the articles.

The theme is dark. The category colours were checked with a palette validator for contrast against the panel colour and for colour-blind separation. Blue marks "above normal" and the main accent; grey marks "normal". Red is used only for warnings, always with a label.

## Readers in, Events about, and states

- **Readers in <country>** ranks stories by how much more people in that country read them than usual, using Wikimedia's daily per-country data. It is ranked by that country's own daily spike, so the list for India is led by Indian stories, and it also shows foreign stories that Indian readers suddenly read.
- **Events about <country / state>** lists worldwide spikes whose subject Wikidata places there (where a place is, a company's headquarters, a politician's seat). Place of birth is not used.
- **Readers in <state>.** Wikimedia publishes reader locations only per country. For a state, the closest public signal is its language Wikipedia, so the pipeline was extended from 5 to 14 languages (adding Telugu, Marathi, Kannada, Malayalam, Bengali, Gujarati, Punjabi, Odia and Assamese). Tamil Wikipedia spikes are listed under Tamil Nadu, Telugu under Andhra Pradesh and Telangana, and so on. Hindi Wikipedia is read across north India, so those states share one list; the dashboard says so. Only India has this view: in most countries every state reads the same Wikipedia.
- The chosen state stays selected when switching between the two modes, with a message when a mode has no data for it.

## Acceptance tests (step 6.7)

The 14 tests from `docs/09_TESTING_QA.md` were run against the real databases with `scripts/run_acceptance.py`, which drives the page with Streamlit's AppTest. All 14 pass (`tests/acceptance_dashboard.md`).

The first run failed the timing test (1,199 ms for a new filter combination). The page was asking Cassandra one day at a time: 7 to 30 queries for the table and 14 to 30 for the daily chart, each waiting for the last. They are now sent together as prepared statements with the driver's concurrent execution, and the slowest uncached view dropped to 725 ms. With the 5-minute cache it is 135 ms.

## Tests

71 unit and dashboard tests pass (`pytest tests/unit tests/dashboard`). The dashboard tests use a fake data layer, so they need no database.
