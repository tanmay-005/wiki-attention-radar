# 09 · Testing and QA

Testing has five layers:

1. **Unit tests**: pure functions (parsers, rules, scoring).
2. **Local MapReduce tests**: mapper/reducer pipes without Hadoop.
3. **Data-quality tests**: numbers stay consistent between stages.
4. **Integration tests**: the dev pipeline end to end.
5. **Validation and acceptance**: does it find real events, and does the dashboard behave as specified?

Tests live in `tests/` and run with `pytest` inside `edge` (`docker compose exec edge pytest /workspace/tests -q`).

---

## 1. Unit tests (`tests/unit/`)

| ID | Function | Cases |
|---|---|---|
| T-U1 | `parse_domain(domain)` | `en` → (en, desktop); `en.m` → (en, mobile); `en.b`, `commons.m`, `en.wikt` → dropped; empty → dropped |
| T-U2 | `normalize_title`, `is_content_page` | `Virat_Kohli` → `Virat Kohli`; `%27` decoded; `Special:Search` → false; `Main_Page` → false; `-` → false; title of 300 chars → truncated to 255; same output as the Hive UDF: `tests/unit/test_udf_parity.py` runs Java `TitleRules` and Python `common_title` on every namespace name in several spellings, tricky percent-encodings and 3,000 random titles |
| T-U3 | `spike_rule(views, median, sigma, cfg)` | Flat series → no spike; one 10× hour above min_views → spike; 10× on a tiny page below min_views → no spike; series with an old spike in the baseline → baseline not inflated (robust median); median 0 → floor applied, no division by zero |
| T-U4 | `merge_episodes(hours)` | Consecutive hours → 1 episode; gap of 1 hour → same episode; gap of 3 hours → 2 episodes; peak hour = max views |
| T-U5 | `categorise(item)` | 20 fixture items (saved Wikidata JSON) → expected categories; human + politician → Politics; unknown → Other |
| T-U6 | `resolve_region(item, cache)` | City in Rajasthan → IN-RJ; country item → no region; item with no P131 → None |
| T-U7 | `union_find` / `group_events` | Same QID merges; linked QIDs within 6 h merge; linked but 2 days apart → separate; generic link (Q5, country) does not merge |
| T-U8 | `event_score` | Monotonic in ratio and in volume; ratio capped at 100 |
| T-U9 | `scope_keys(event)` | Event in Rajasthan with India readers → `G`, `R:IN`, `A:IN`, `A:IN:IN-RJ` |
| T-U10 | `country_file_name(date)` | 2024-06-03 → `2024-06-03.tsv` |
| T-U11 | Filter rules (dashboard state) | Country change resets state and mode; state pick sets mode = about; Readers in resets state |

Saved Wikidata responses live in `tests/fixtures/wikidata/*.json`, so unit tests never call the network.

---

## 2. Local MapReduce tests (`tests/mr/`)

Hadoop Streaming is "stdin → stdout", so jobs can be tested with Unix pipes on a fixture file:

```bash
export mapreduce_map_input_file=pageviews-20240603-120000 PYTHONPATH=mapreduce
cat tests/fixtures/mr/pageviews-20240603-120000 \
  | python3 mapreduce/mr1_mapper.py 2>/dev/null \
  | LC_ALL=C sort -t$'\t' -k1,4 \
  | python3 mapreduce/sum_reducer.py > /tmp/mr1_out.tsv
diff <(LC_ALL=C sort /tmp/mr1_out.tsv) <(LC_ALL=C sort tests/fixtures/mr/expected_mr1.tsv) && echo "T-M1 OK"
```

The same checks, plus T-M2 to T-M5 and MR4, run with `bash tests/mr/test_combiner.sh` (pytest, a second or two).

| ID | Test |
|---|---|
| T-M1 | MR1 output equals the expected fixture (`tests/fixtures/mr/`: 30 hand-made lines covering every filter rule) |
| T-M2 | Running the reducer as a combiner and then the reducer gives the same result as the reducer alone |
| T-M3 | MR2 sums hours into days correctly across midnight (hours 23 and 00 go to different days) |
| T-M4 | MR3 keeps exactly 100 rows per (lang, day), in descending order |
| T-M5 | Malformed lines (3 fields, non-numeric views) are counted, not crashing |

---

## 3. Data-quality tests (`tests/dq/`, run after each stage on real data)

| ID | Check | How |
|---|---|---|
| T-D1 | Completeness | Every configured hour has a raw file; every dt has `_SUCCESS` after MR1/MR2 |
| T-D2 | View conservation | Σ views by language in `staged_hourly` = Σ views in `raw_hourly` after the same filters (HQL using the UDFs) |
| T-D3 | Daily = Σ hourly | For every (lang, title) of the day: `daily_views.views` = Σ `hourly_views.views` (full check, not a sample; `tests/dq/dq_spark.py`) |
| T-D4 | No duplicates | `hourly_views` has unique (lang, title, hour_ts) (`tests/dq/dq_spark.py`) |
| T-D5 | Dense series | `series_global`: hours per key = expected; no nulls |
| T-D6 | Spike sanity | All spikes satisfy the configured thresholds; no spike in the baseline-only period |
| T-D7 | Enrichment coverage | ≥ 90% of spike keys have a QID; % "Other" category reported |
| T-D8 | Event uniqueness | `event_id` unique; no event with > 50 members (a sign of over-merging) |
| T-D9 | Serving parity | Top 10 per scope in Cassandra = top 10 from Parquet; every event in Cassandra exists in Mongo |
| T-D10 | Country data | Per-country rows only for published countries; no negative counts after noise handling (clip at 0) |

Results are appended to `benchmarks/results/dq_<run>.csv` and included in the report.

---

## 4. Integration tests (`tests/integration/`)

`test_pipeline_dev.py` runs after `run_pipeline.sh --dev` and asserts the six checkpoints in `08_INTEGRATION.md` §6. It is marked `@pytest.mark.integration` so unit tests can run alone:

```bash
pytest -m "not integration"     # fast, any time
pytest -m integration           # after a dev run, with containers up
```

---

## 5. Detection validation (quality of results)

| Metric | Definition | Target |
|---|---|---|
| Recall | Known events matched by a detected event within 48 h | ≥ 75% |
| Precision@50 | Of the top 50 global events, % labelled as real events | ≥ 80% |
| Detection delay | Hours between the real event time and `first_detected` (median) | Report it |
| Country relevance | For known Indian events, rank in `R:IN` vs rank in `G` | Higher in `R:IN` |

The labelling procedure for precision@50: for each event, check the lead article's Wikipedia page and news for that date. Mark `is_real = 1` if an identifiable real-world occurrence explains the spike (including releases, deaths, matches, anniversaries); `0` for bots, artefacts or unexplained spikes. Record a one-line note.

---

## 6. Dashboard acceptance tests (manual, recorded in `tests/acceptance_dashboard.md`)

| ID | Steps | Expected |
|---|---|---|
| A1 | Open the app | Global, All categories, All languages, 7 days; list not empty; summary sentence correct |
| A2 | Country = India | List = `R:IN`; Mode = Readers in; State enabled |
| A3 | Global + Politics | Only Politics events; category bar highlighted |
| A4 | India + Politics | Only Politics events in `R:IN` |
| A5 | India → Events about → State = Rajasthan | List = `A:IN:IN-RJ`; only stories located in Rajasthan |
| A6 | India → Readers in → State = Tamil Nadu, then switch to Events about and back | List = `R:IN:IN-TN` (Tamil Wikipedia spikes, explained in the header); the state stays selected in both modes, with an empty-state message where a mode has no data |
| A7 | Language = Hindi | Every row's languages include Hindi |
| A8 | Time window 7 days | More or equal events than 24 h; no duplicates |
| A9 | Click a list row | Detail panel updates; chart shows the baseline line; country bars present |
| A10 | Click a category bar | Category filter set; clicking again clears it |
| A11 | Reset | All defaults restored |
| A12 | Stop Cassandra, reload | Red "cannot reach database" banner, no stack trace |
| A13 | A country with no data | Empty-state message from `04_UI_UX_DESIGN.md` |
| A14 | Timing | Every filter change renders in < 1 s (the `[timing]` line app.py prints in the terminal for each run) |

---

## 7. Performance tests

| ID | What | Record |
|---|---|---|
| T-P1 | Time per pipeline stage, dev and full | Stage, minutes, input size |
| T-P2 | Cassandra query latency (`TRACING ON` in cqlsh) for Q1–Q3 | ms |
| T-P3 | Mongo `explain("executionStats")` for detail lookups | Index used, ms |
| T-P4 | Benchmarks B1–B5 | See Feature Development F13 |

---

## 8. Definition of done (per phase)

A phase is done when:
1. Its scripts run without manual steps.
2. Its unit/local tests pass.
3. Its data-quality checks pass on the dev slice.
4. Screenshots/outputs for the report are saved in `report/assets/`.
5. The matching steps are ticked in the build tracker.
