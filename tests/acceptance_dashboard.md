# Dashboard acceptance tests (step 6.7)

A1–A14 from `docs/09_TESTING_QA.md` section 6, run with `scripts/run_acceptance.py`.

- Run: 2026-10-29 01:48, against the real Cassandra and MongoDB (nosql containers)
- Data: 06 Oct – 26 Oct 2026, detection from 13 Oct, 14407 events
- Method: the Overview page is driven with Streamlit's AppTest (filters set, buttons and the mode control clicked, the page checked after each step). A category-bar click (A10) and "Open full story" (A9) are made through the session state those clicks set.
- Time: render time of the page. Cold = Streamlit's cache cleared first, so every query goes to the databases; warm = the same view again within the 5-minute cache.

| ID | Test | Result | Cold (ms) | Warm (ms) | Notes |
|---|---|---|---|---|---|
| A1 | Open the app | Pass | 725 | 111 | 100 stories listed; top: 2026 Ballon d'Or |
| A2 | Country = India | Pass | 202 | 77 | 100 stories read in India |
| A3 | Global + Politics | Pass | 141 | 70 | 100 stories, all Politics |
| A4 | India + Politics | Pass | 157 | 62 | 48 stories, all Politics |
| A5 | India, Events about, a state | Pass | 126 | 56 | Maharashtra: 10 stories (30 days), each located there |
| A6 | India, Readers in, Tamil Nadu; switch modes both ways | Pass | 143 | 95 | Tamil Nadu kept both ways; Readers in 51 stories, Events about 5 |
| A7 | Language = Hindi | Pass | 220 | 68 | 43 stories, all with a Hindi Wikipedia article |
| A8 | 24 hours vs 7 days | Pass | 218 | 135 | 24 h: 635 stories, 7 days: 3948; no duplicates in the table |
| A9 | Detail panel and full story | Pass | 479 | 123 | detail panel shows '2026 Ballon d'Or'; full story of 'Tom Skerritt' has the hourly chart |
| A10 | Category bar click and second click | Pass | 252 | 99 | Sports set by the click, cleared by the second click |
| A11 | Reset filters | Pass | 237 | 94 | all filters back to the defaults |
| A12 | Cassandra unreachable | Pass | – | – | red 'Cannot reach Cassandra' banner, no stack trace (Cassandra pointed at a closed port) |
| A13 | Country without per-country data | Pass | 32 | 3 | Afghanistan: privacy message shown |
| A14 | Every filter change under 1 s | Pass | – | – | slowest cold 725 ms, slowest warm 135 ms |

Result: 14 of 14 passed.
