# Forecast evaluation — 18 September 2026

Brisbane unleaded 91, daily median price in cents per litre. This is the dated,
reproducible evidence required by item 2 of `docs/agent-handoff.md`.

**Verdict in one line: persistence still wins. No model earned the right to be
trusted, and `/api/forecast` and the buying planner keep saying so.**

## Regenerating this report

```
.venv\Scripts\python.exe -m qld_history_import --years 2025 2026 --db <database>
.venv\Scripts\python.exe -m forecasting --db <database> --root . --today 2026-09-18
```

`--db` is the SQLite file holding the imported backfill; `--root` is the workspace
whose CSV and `snapshots` hold the collected series. The numbers below came from a
**scratch** database outside the repository, because the multi-year import has
deliberately not been written into the tracked 71 MB `fuel_data.db` — that remains
the user's decision (item 3). Nothing in this evaluation requires the backfill to
be present: without it the code path runs unchanged on collected days alone.

## Data

| | |
| --- | --- |
| Reported (official backfill) | 608 days, 2025-01-01 to 2026-08-31, **zero calendar gaps** |
| Collected (this app) | 24 days, 2025-12-03 to 2026-09-18 |
| Merged series used | 611 days, 2025-01-01 to 2026-09-18, 15 calendar gaps (all September 2026) |
| Overlap | 21 days present in both; the collected value wins, and stays labelled `collected` |
| Raw reported events | 132,841, from 20 monthly source files, reconciled 1:1 against the per-day counts |
| Stations per reported day | min 270, median 485, max 502 |
| Carried-forward station-days | 66.6% of counted station-days are a held previous report, not a same-day one |
| Explicit unavailability rows | 1,033 (stored as the 0 sentinel, masked out of the median) |
| Reported daily median price | 151.9 to 259.9, median 185.9 |

### Provenance, and why a backtest over reported days is legitimate

The monthly files are a **retrospective record**: they are published about a month
after the events they contain, so this app could not have held them at the time.
Each row, however, carries the retailer's own report timestamp, and those prices
were publicly visible then through the same live Queensland API this app polls.
The reconstruction only ever carries a report forward (`ffill`, 14-day expiry), so
a day's value depends solely on reports timestamped on or before that day. A
point-in-time backtest over them is therefore a fair test of the models.

What is *not* claimed, and what the code prevents:

- Reported days are never relabelled as observations this app collected. They stay
  `provenance: reported` in `forecasting.merge_history` and are counted separately
  in `forecast.provenance`.
- A later monthly file cannot change an earlier prediction: every day ≤ the origin
  is all the models see, and `tests/test_local.py::ForecastTests` asserts that
  truncating the reported series at the origin produces a byte-identical forecast.
- The backfill never counts as collection. `recent_consecutive_days` counts only
  collected days, and staleness is measured from the last **collected** day, so a
  month-old backfill cannot make a stopped collector look current.
- **Every origin evaluated here is a reported origin** (260 of 260 selection, 133 of
  133 held-out). Nothing below is evidence about days this app collected itself.

`qld_history_import.py`'s module docstring states the backfill "must never enter a
point-in-time backtest as if it were live data". This report honours the second
half of that sentence rather than the first: the data is used *as reported events
with their own timestamps*, never as live data, and every output says which days
came from where.

## Method

Unchanged machinery in `forecasting.py`, now fed the merged series:

- Origins are days with a complete 14-day window (7 days of input, 7 days of
  target). The most recent 400 are used (`MAX_ORIGINS`).
- Chronological split: first 65% (260 origins) select the model, the rest are
  held out. Held-out origins whose target window overlaps the last selection
  origin's are **purged**, leaving 133 held-out origins, 2026-04-14 to 2026-08-24.
- Each horizon 1–7 is scored separately. MAE, in cpl, against the day's actual
  median price.
- Baseline: `persistence` — today's median, carried flat.

## Per-horizon held-out MAE (cpl, 133 origins, lower is better)

| Model | Selection MAE | h1 | h2 | h3 | h4 | h5 | h6 | h7 | Held-out mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| persistence (baseline) | 9.65 | **1.26** | 2.47 | 3.63 | 4.75 | 5.81 | 6.82 | 7.77 | 4.64 |
| damped_trend | 8.82 | **1.24** | **2.21** | **3.26** | **4.29** | **5.32** | **6.25** | **7.18** | 4.25 |
| weekly | 16.53 | 8.30 | 8.25 | 8.18 | 8.11 | 8.01 | 7.90 | 7.77 | 8.07 |
| **analog — selected** | **8.69** | 2.60 | 4.97 | 7.08 | 9.15 | 10.79 | 12.33 | 13.66 | 8.65 |

Read this table twice.

1. **The selected model loses at every horizon.** `analog` won the selection period
   (8.69 vs the baseline's 9.65) and then came 4th out of 4 on held-out data, with
   error growing from 2.6 cpl at one day to 13.7 cpl at seven — roughly double the
   baseline at every horizon. That is textbook overfitting, and it is exactly what
   the purged held-out split exists to catch. `beats_baseline` is
   `[false × 7]` and the API reason string says so in words.
2. **`damped_trend` beats the baseline at all seven horizons — and it was not
   selected.** Adopting it now, after seeing the held-out result, would burn the
   only unbiased evidence in this report. It also would not survive scrutiny: see
   below.

### Is `damped_trend`'s edge real?

Mean per-origin error difference (`damped_trend − persistence`, negative favours
`damped_trend`), with a moving-block bootstrap (14-day blocks, 4,000 draws, seed
20260918) that respects the overlap between adjacent origins:

| Horizon | Mean difference | Origins where it beats persistence | Bootstrap 95% interval |
| --- | ---: | ---: | --- |
| 1 | −0.03 | 29% | [−0.25, +0.27] |
| 2 | −0.26 | 33% | [−0.84, +0.37] |
| 3 | −0.36 | 33% | [−1.16, +0.54] |
| 4 | −0.46 | 32% | [−1.54, +0.69] |
| 5 | −0.49 | 32% | [−1.77, +0.83] |
| 6 | −0.56 | 31% | [−2.08, +0.98] |
| 7 | −0.59 | 32% | [−2.25, +1.08] |

Every interval straddles zero, and `damped_trend` is the better forecast on only
about a third of origins. Its lower mean comes from a minority of large-move days
where a trend helps a lot, paid for by being slightly worse most of the time. On
this evidence the edge is not distinguishable from noise, and a model selected on
the selection period would not have picked it anyway.

## Recent regime — last 90 days (65 held-out origins, from 2026-06-21)

| Model | h1 | h2 | h3 | h4 | h5 | h6 | h7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| persistence | 1.26 | 2.49 | 3.66 | 4.79 | 5.86 | 6.81 | 7.71 |
| damped_trend | 1.35 | 2.37 | 3.50 | 4.55 | 5.64 | 6.54 | 7.46 |
| analog (selected) | 3.47 | 6.78 | 9.53 | 12.14 | 14.06 | 15.34 | 16.52 |

The recent slice is drawn from held-out origins only, so it is not a second look at
the selection period. It reinforces the verdict: `analog` degrades further, and
`damped_trend`'s already-marginal edge disappears at h1 in the recent regime. Note
the Brisbane price cycle means error grows nearly linearly with horizon for every
model — a week ahead, every method is 7–8 cpl out at best.

## Uncertainty calibration

The published band is the 80th percentile of held-out absolute errors, which makes
its nominal 80% in-sample. The honest test fits the radius on the **selection**
period and measures how often the held-out actual falls inside it:

| Model | Band coverage (target 0.80) |
| --- | ---: |
| persistence | 0.938 |
| damped_trend | 0.924 |
| weekly | 0.977 |
| analog (selected) | 0.764 |

Per horizon for the selected model: 0.767, 0.774, 0.789, 0.744, 0.744, 0.744, 0.782.

**The band is miscalibrated in both directions.** For the baseline and the trend
model it is far too wide — a nominal 80% band that contains 92–94% of outcomes
overstates uncertainty and makes any "even at the upper estimate" argument too
easy to pass. For `analog` it is too narrow at 76%, understating the risk of the
model that was actually selected. The selection period contained larger price
swings than the held-out period, which is why a radius fitted there over-covers
later. Treat the band as a rough historical error range, never as a probability.

## Verdict

**Do not trust the model.** Specifically:

- No model is validated. The selected one (`analog`) is clearly worse than doing
  nothing at every horizon.
- The only model with a positive held-out record (`damped_trend`) was not selected,
  and its advantage is inside bootstrap noise. It is not adopted.
- Therefore the honest forecast for Brisbane unleaded remains "today's price", with
  an explicit error range that widens by roughly 1.1 cpl per day of horizon.
- 608 days of official history did not fix this. The binding constraint is not data
  volume — it is that a four-model shortlist of naive time-series rules does not
  capture a retail price cycle driven by discrete retailer decisions. More data was
  necessary and was not sufficient.

This is a negative result and it is published as one.

## What the app does with it

- `/api/forecast` gains `provenance`, per-model `horizon_mae`, `band_coverage`,
  `beats_baseline`, `recent` and `calibration`; its `reason` string now states both
  that retrospective reported days were used and that the selected model did not
  beat the baseline. No threshold was loosened to make a forecast appear.
- `buying_plan.compare_plan` now requires the selected model to have beaten
  persistence **on held-out data at the horizon being priced**, on top of the
  existing `status == 'ready'` and 7 consecutive collected days. A model that wins
  at day 1 and loses at day 7 no longer justifies a 7-day wait. With today's
  evidence the planner keeps returning "No reliable price signal to justify
  waiting" and the `Unvalidated baseline` basis.
- With the backfill loaded and collection as it stands, the forecast status is
  `ready` but `beats_baseline` is all false and `recent_consecutive_days` is 3, so
  every recommendation gate stays shut.

## Limitations and what was not verified

- All 393 evaluated origins are reported origins. Nothing here measures how the
  models behave on days this app collected itself; only 24 collected days exist and
  they contain gaps, so no collected-only backtest is possible.
- The backfill was read from a scratch database. The tracked `fuel_data.db` still
  has no reported events, so a `/api/forecast` served by the running server shows
  the pre-backfill `limited` state until the user imports it.
- The running server (port 8000) was **not restarted**, so it is still executing the
  pre-change code. The new dashboard path was verified instead by building
  `LocalData` against a temporary workspace holding the backfill: it produced
  `status: stale`, no points, `recent_consecutive_days: 0` and 0 collected days —
  correct, since that workspace has the backfill but no collection.
- Building the forecast over 400 origins on the 608-day series takes about 7
  seconds; the dashboard caches it, so the cost lands on the first request after
  each collection. Lower `forecasting.MAX_ORIGINS` if that latency matters.
- The bootstrap assumes 14-day blocks are long enough to absorb the autocorrelation
  from overlapping 7-day target windows. It is a sanity check on the size of the
  edge, not a formal test.
- Wholesale, currency and traffic context remain outside the model (item 9). No
  feature was added or tested here.
- Attribution: Queensland fuel price reporting, © State of Queensland, CC BY 4.0.
