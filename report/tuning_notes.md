# Phase 4 tuning notes (step 4.12)

Threshold checks on the real S2/S3 output (detection period 2024-06-01 to 06-14, baseline from 2024-05-25).
`spark/jobs/s2_inspect.py` prints the counts used here.

## Global hourly (S2)

| Run | Settings | Episodes | en | hi | ja | pt | ta | Suspicious |
|---|---|---|---|---|---|---|---|---|
| 1 | min_ratio 4, min_z 5, no minimum length; min_views en 1000, ja 200, pt 100, hi 40, ta 15 | 37,073 | 9,321 | 2,772 | 17,003 | 5,006 | 2,971 | 42 |
| 2 | min_ratio 10, min_hours 2 (rest unchanged) | 12,338 | 3,831 | 957 | 5,140 | 1,706 | 704 | 43 |

**Run 1.** The top 20 were real events, but almost every candidate article had at least one episode (9,802 of 9,961 Japanese candidates, 6,218 of 6,547 English). A ratio of 4 over a two-week period is easy to reach for any page that is popular at all.

**Near-zero baselines.** 25 episodes had a baseline under 5 views an hour and a peak above 5,000. The concern was automated traffic, but 24 of them were read mainly on phones (desktop share 1% to 58%). Automated traffic is usually desktop. Likely causes are TV programmes (several Brazilian pages all peak at 19:00 UTC on Sunday 06-09), Google Doodles and Wikipedia's own main page (for example *Imber friendly fire incident*, *Bordel militaire de campagne*). Only *Threat actor* was 100% desktop, and it lasted 7 hours, so it is not a one-hour bot hit. These are kept as real attention.

**Run 2.** Raising the ratio to 10 and dropping episodes that last a single hour cut the episodes by 67%. One-hour suspicious episodes are still kept, so bot hits stay visible and flagged. The top 20 did not change. Japanese keeps the most episodes, which fits how often Japanese pages get sudden bursts from TV and news portals.

Top 20 (same in both runs) and what caused them:

| Article | Peak (UTC) | Cause |
|---|---|---|
| Carlos Alcaraz, Alexander Zverev | 06-09 18:00 | French Open men's final |
| Iga Świątek | 06-08 15:00 | French Open women's final |
| Carlo Ancelotti, Toni Kroos, Borussia Dortmund, List of European Cup and UEFA Champions League finals | 06-01 20:00–22:00 | Champions League final |
| List of NDA members, National Democratic Alliance, INDIA alliance | 06-04 07:00–11:00 | Indian election results |
| Kinjarapu Ram Mohan Naidu, Annpurna Devi | 06-09 15:00 | New Indian ministers sworn in |
| Mohan Charan Majhi | 06-11 13:00 | Named Chief Minister of Odisha |
| Jerry West | 06-12 15:00 | Death |
| Jamal Musiala | 06-14 20:00 | Scored in the Euro 2024 opening match |
| Kristaps Porziņģis | 06-07 02:00 | NBA Finals game 1 |
| Dick Van Dyke | 06-03 02:00 | not checked |
| 日本航空350便墜落事故, ホテルニュージャパン火災, ポンティアック戦争 | 06-11 to 06-13 | Japanese pages about old events with near-zero baselines; most likely TV, not checked |

## Per country daily (S3)

| Run | Settings | Candidates | Episodes | US | JP | GB | IN | Zero baseline |
|---|---|---|---|---|---|---|---|---|
| 1 | min_views 500, min_ratio 3, min_z 4, floor 90 | 95,562 | 55,316 | 17,787 | 9,567 | 7,964 | 6,371 | 26,489 (48%) |
| 2 | min_views 1000, min_ratio 10 (rest unchanged) | 43,334 | 17,041 | 4,849 | 2,982 | 2,499 | 2,279 | 9,876 (58%) |

**Run 1.** The top 20 were real and in the right countries: Claudia Sheinbaum in the US, Michael Mosley in the UK, the election results, Pawan Kalyan and Ramoji Rao in India, Gainax and Eiko Masuyama in Japan. But 55,316 episodes came from 95,562 candidates. Wikimedia does not publish a (country, article, day) count under 90 views, so many items had a baseline of all zeros. The floor then sets the baseline to 90, and any day with 500 views is already 5.5 times that, which passes a ratio of 3. 48% of the episodes started from a zero baseline, and half of those peaked under 815 views.

**Run 2.** Ratio 10 and a peak of at least 1,000 views cut the episodes by 69%, and the top 20 did not change. A peak of 2,000 was also measured (8,793 episodes), but it removes small countries much faster than large ones (Germany 251 → 107, Australia 737 → 309), so 1,000 was kept. Zero-baseline episodes are still 58%, but their median peak is now 1,566 views instead of 815. These are items that really went from almost no readers in a country to over a thousand in a day.

## Enrichment categories (S4)

16,804 spiking pages (global + country), 16,780 with a Wikidata item (99.9%, T-D7 PASS). Every run after the first reads the Wikidata items from the MongoDB cache, so only the title lookups and new classes are fetched (91 to 179 API calls, under 4 minutes).

| Run | Change | Other | Entertainment | Sports | Politics | Business | Science & Tech | Disasters |
|---|---|---|---|---|---|---|---|---|
| 1 | P31 / P106 match, one level of P279 for classes | 8,177 (49%) | 4,078 | 1,941 | 2,078 | 379 | 70 | 81 |
| 2 | classes walked up to three levels of P279 | 7,431 (44%) | 4,158 | 2,155 | 2,374 | 431 | 93 | 162 |
| 3 | occupations walked the same way | 5,248 (31%) | 4,511 | 3,665 | 2,391 | 534 | 293 | 162 |

**Run 1.** Half the pages were "Other", including pages that clearly have a category: past Indian general elections, Champions League and NBA finals, a plane crash, a mass shooting. On Wikidata these items sit several levels under the classes in `category_rules.yaml` (2019 Indian general election → Lok Sabha election → general election → public election), and only one level was checked.

**Run 2.** Walking up to three levels moved 746 pages out of "Other", all of them matched two or three levels up. Elections, finals and disasters left the sample. The nearest matching level wins, so a specific class is never overruled by a distant one.

**Run 3.** The remaining sample was full of athletes whose occupation names a sport (baseball player, golfer, mixed martial artist) instead of "athlete". Walking occupations the same way moved about 2,180 more pages; Sports went from 2,155 to 3,665.

**What stays "Other".** Places (Barbados, Ayodhya), diseases, history (Battle of Khasham), food, music charts, court cases and officials without a matching occupation. These do not belong to any of the six dashboard categories, so 31% "Other" is accepted.

## Events (S5)

Nodes: 12,338 global episodes and 17,041 country episodes. Window for linked items 6 hours, same item 24 hours (country spikes: the same or next day), hub limit 25, limit 50 per event.

| Run | Change | Events | Single-spike events | Largest event | T-D8 |
|---|---|---|---|---|---|
| 1 | links used between any two spikes | 16,446 | 11,001 | Toni Kroos, 363 spikes | FAIL |
| 2 | links only between global (hourly) spikes; groups over 50 split | 16,880 | 11,419 | Claudia Sheinbaum, 54 spikes of one item | FAIL (check counted spikes) |
| 3 | size measured in different items, for both the split and the check | 16,822 | 11,414 | Angus Gunn, 20 items, 56 spikes | PASS |

**Run 1.** Country spikes only have a date, and "within a day" is loose. Linked items (a player, his club, his teammates, their national teams) chained through the country spikes into one 363-spike blob around the Champions League final.

**Run 2.** Wikidata links now only join hourly spikes, where the 6-hour window is precise. Country spikes join an event only when they are about the same item. Kroos fell to 19 spikes. The new largest event was Claudia Sheinbaum: 6 language spikes and 48 country spikes, all about her. That is one story read worldwide, so the check, which counted spikes, was measuring the wrong thing.

**Run 3.** Both the safety split and T-D8 now count different Wikidata items per event. Only 1 group had to be split. The largest event is the Euro 2024 opening match (Germany against Scotland, 06-14): Angus Gunn, the Scotland team, Steve Clarke, Scott McTominay, Andrew Robertson, Flower of Scotland and others, 20 items. That is a coherent story. The most spikes in one event is 117 (Claudia Sheinbaum, including country spikes).

**Known limit.** The Indian election result is spread over several top events (the 2024 and 2019 election pages, NDA, the INDIA alliance, the list of NDA members). These pages do not link to each other through the Wikidata properties used, so they are not grouped. The dashboard shows them side by side on the same day, which is acceptable.

## Decision

- Global hourly: `min_ratio: 10.0` and `min_hours: 2` in `settings.yaml`, with per-language `min_views` and `baseline_floor` unchanged. Fewer, longer episodes, with the same events at the top and bot hits still flagged.
- Country daily: `min_views: 1000` and `min_ratio: 10.0`, with the 14-day window, floor 90 and min_z 4 unchanged. This removes the small climbs out of the privacy floor and keeps a fair share for smaller countries.
- Categories: rules unchanged; classes and occupations are matched up to three levels up the Wikidata class tree.
- Events: Wikidata links join only hourly (global) spikes; event size is counted in different items, with a limit of 50.
