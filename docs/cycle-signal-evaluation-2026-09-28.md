# Cycle, wholesale and station-signal evaluation — 28 September 2026

Brisbane unleaded 91. Follows `forecast-evaluation-2026-09-18.md`, with the same
data, split and purge.

**Verdict in one line: no price model beats persistence yet. Two findings changed the
plan. The retail cycle has largely stopped since February 2026. And the share of
stations already at restored prices gives about a week's warning of a restoration
when the cycle does run. That share is now shown live as an observed signal, not a
forecast.**

## Data

| Source | Span | Where |
| --- | --- | --- |
| Official QLD station reports (the backfill, 20 monthly files) | 2025-01-01 to 2026-08-31 | Scratch database, via `python -m qld_history_import --years 2025 2026 --db <scratch>` |
| AIP Brisbane ULP terminal gate price (`AIP_TGP_Data_25-Sep-2026.xlsx`) | daily, 2004 to 2026-09-25 | aip.com.au, historical TGP data |
| EIA Brent spot (`RBRTEd.xls`) and NY Harbor conventional gasoline (`EER_EPMRU_PF4_Y35NY_DPGd.xls`) | daily | eia.gov |
| RBA AUD/USD (`f11.1-data.csv`) | daily, 2023 to 2026-09-25 | rba.gov.au |

None of these files is in the repository.

The wholesale and overseas series are the publishers' current files, not vintages
recorded at the time. They were used with publication lags (AIP 1 day, EIA 4 days,
RBA 1 day), but later revisions cannot be ruled out.

## Finding 1 — the retail cycle has largely stopped

Monthly medians, c/L:

| | 2025 range | Feb–Sep 2026 |
| --- | --- | --- |
| Retail median minus Brisbane TGP | 12 to 41, cycling | 6 to 8, flat (15 during the March shock) |

- **2025:** ten cycle restorations, typically 35–48 days apart, each about +40 c/L over 1–3 days.
- **Since February 2026:**
  - One muted restoration (2026-05-16, +14 c/L).
  - A wholesale shock in March (+10 c/L steps to 260).
  - A cost-driven climb in July–August of 2–4 c/L a day with no restoration day.
  - Retail now follows the TGP within a day or two.

The held-out period (2026-04-14 to 2026-08-24, 133 origins) sits wholly in this new regime.

## Finding 2 — no tested price model beats persistence

Held-out MAE in c/L, averaged over horizons 1–7. Lower is better.

| Model | Mean MAE | h7 | Note |
| --- | --- | --- | --- |
| Persistence | 4.64 | 7.77 | Published baseline |
| Damped 7-day trend (existing) | 4.25 | 7.18 | Not separable from persistence: block-bootstrap 95% CI crosses 0 at every horizon |
| Cycle timing from the median (days since restoration, empirical survival) | 6.62 | 12.99 | Predicted +40 c/L jumps that never came in 2026 |
| Margin-over-TGP error correction, fixed fit | 7.09 | 12.73 | Coefficients learned in the 2025 cycle regime |
| Same, refit on trailing 60 / 120 / 240 days | 8.06 / 5.81 / 5.29 | – | – |
| Petrol (AUD, GST) 7-day change + TGP 7-day change, expanding refit | 4.49 | 7.42 | CI vs persistence crosses 0; loses to damped trend |

- **Upstream does lead wholesale.** The petrol change published by day t correlates
  0.37 with the TGP change over t..t+7. But retail now passes TGP through so quickly
  that little of it is left to forecast one week ahead.
- **Selection rule.** The harness's fixed 65/35 selection picks `analog`, the best
  model on 2025 data and the worst held-out (13.66 at h7). With the backfill loaded,
  the published path would be analog's.
- **Caveat on reuse.** This held-out window has now been examined repeatedly during
  diagnosis. Do not use it again to choose settings. The next valid test is forward:
  collected days after 2026-09-28, scored from the existing prediction log.

## Finding 3 — station-level early warning

`hi20[t]` is defined on the backfill's as-of reconstruction: the last report per
station per Brisbane day, held up to 14 days.

> hi20[t] = share of stations whose held price at the end of day t is at least 20 c/L above day t's median.

A restoration spreads through the stations over 5–14 days. The median only jumps
once about half the stations have restored, so hi20 rises well before the median
moves.

hi20 in the days before each cycle restoration (%):

| start | -14 | -10 | -7 | -5 | -3 | -1 |
| --- | --- | --- | --- | --- | --- | --- |
| 2025-02-12 | 7.9 | 16.9 | 31.4 | 37.5 | 37.8 | 43.8 |
| 2025-05-14 | 2.3 | 4.4 | 14.9 | 22.7 | 27.5 | 40.0 |
| 2025-07-01 | 1.0 | 9.0 | 19.0 | 32.3 | 40.0 | 44.6 |
| 2025-08-10 | 0.6 | 1.3 | 0.4 | 12.0 | 32.8 | 42.9 |
| 2025-09-16 | 3.6 | 1.7 | 6.5 | 16.6 | 28.1 | 38.6 |
| 2025-10-22 | 1.4 | 0.6 | 3.7 | 18.7 | 23.1 | 41.8 |
| 2025-11-25 | 0.4 | 0.8 | 1.0 | 11.3 | 23.8 | 37.9 |
| 2026-01-04 | 5.2 | 7.8 | 18.5 | 24.4 | 30.6 | 42.5 |
| 2026-02-17 | 2.7 | 6.1 | 16.9 | 22.8 | 29.9 | 40.1 |
| 2026-05-16 | 5.2 | 10.6 | 16.3 | 22.1 | 24.6 | 25.0 |

- **Away from restorations:** outside the 14 days before a start, hi20 averages 1.2%.
- **Warning thresholds:**
  - At least 10% gave a median 7 days' warning (range 5–10).
  - At least 37% at end of day flagged 8 of 9 scored starts the evening before (precision 0.73 for "tomorrow").
- **Scores:** leave-one-cycle-out log loss for "restoration within 3 days" was 0.044, against 0.240 for the days-since hazard. At 7 days it was 0.157 against 0.334. The gain held in 10 of 10 and 9 of 10 cycles.
- **Weaker alternatives:**
  - Brand leadership: the four majors restore together about a day ahead of the city, with no consistent single leader.
  - Daily "hiked today" share.
  - Trough depth.
- **Failure mode:** cost-driven climbs. July 2026 reached 18.5% with no restoration.

**Caveats:**
- Only ten events.
- hi20 tracks the same spread the median eventually crosses, so it predicts when the median crosses, not when the first retailer moves.
- Live data has no 14-day expiry. The live signal therefore counts only stations that reported within 14 days, to keep the thresholds comparable.

**What shipped:** `local_data.restoration_signal`, exposed as `/api/dashboard` →
`restoration`, with levels `quiet` (under 10%), `building` (10% to under 37%) and
`imminent` (37% or more). It is shown on the planner.
- It is an observed indicator. It does not enter `build_forecast` and does not change any price path.
- On 2026-09-28 it read 0.8% of 499 stations: quiet, as expected with the cycle dormant.

## Reproducing hi20

```python
from data_store import FuelDataStore
from qld_history_import import _daily_last_reports, UNAVAILABLE_CPL
_, wide = _daily_last_reports(FuelDataStore('<scratch db>').get_reported_events('Unleaded'))
held = wide.ffill(limit=14).mask(lambda f: f.eq(UNAVAILABLE_CPL))
med = held.median(axis=1)
hi20 = held.ge(med + 20, axis=0).sum(axis=1) / held.notna().sum(axis=1)
```

## Next

1. **Forward test.** Log persistence, damped trend and petrol + TGP pass-through from
   today, and score them on collected days only.
2. **Wholesale context.** Keep collecting TGP, petrol and FX daily. The pass-through
   term needs vintages recorded at the time, not today's revised files.
3. **Watch hi20 for a returning cycle.** If restorations resume, fit P(restoration
   within h | hi20) and test it as a jump term on top of the damped trend.
