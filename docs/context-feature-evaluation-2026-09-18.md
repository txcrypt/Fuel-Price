# Context-feed evaluation — 18 September 2026

Do the wholesale, currency and overseas spot feeds predict Brisbane unleaded 91?
This is the dated, reproducible evidence required by item 9 of
`docs/agent-handoff.md`. It is the companion to `docs/forecast-evaluation-2026-09-18.md`
and uses the same origins, the same split and the same bootstrap.

**Verdict in one line: no feed earned inclusion. Two could not be tested at all,
and the two that could are, if anything, worse than doing nothing.**

Nothing in this report changes `build_forecast`. The four context sources remain
dated context, exactly as they were before it.

## Regenerating this report

```
.venv\Scripts\python.exe -m forecasting --db <database> --root . --today 2026-09-18 --context
```

`--db` is the SQLite file holding the imported backfill *and* the context vintages.
As with the forecast evaluation, the numbers below came from a **scratch** database
outside the repository; the tracked `fuel_data.db` still holds no reported events.

## What was tested

For each feed separately, a single lagged feature:

> `x(t)` = the change in that feed's value over the last `window` days, measured
> between the two most recent observations the origin `t` could actually read.

The candidate model is persistence plus a linear correction: `price(t) + α + β·x(t)`,
with `α` and `β` fitted by least squares **on the selection period only** and applied
unchanged to the held-out period. One feed at a time — pooling feeds would let extra
inputs buy apparent fit, which is the failure mode this test exists to prevent.

### Two controls, not one

| Control | What it is | Why it is here |
| --- | --- | --- |
| `persistence` | today's median, carried flat | the published baseline |
| `drift` | `price(t) + α`, intercept only, no feature | isolates the feed |

A feed has to beat **both**. Beating persistence alone proves nothing: over a period
with a net price drift, an intercept fitted on earlier days beats persistence without
reading the feed at all. Only the gap between `drift` and the candidate is the feed's
own contribution. A feed also has to have a bootstrap interval that stays below zero,
so a win inside noise is not adopted — the same rule that kept `damped_trend` out of
the forecast.

### Point-in-time discipline

Two independent cut-offs are applied at every origin, in `forecasting.context_feature`:

1. **Publication lag.** An observation dated within the feed's lag of the origin is
   discarded, because the publisher had not released it yet. EIA's lag was verified
   live during this audit: the latest observation was 2026-09-15 for an 18 September
   fetch, so 4 days is used. AIP and RBA use 1 day.
2. **Vintage.** Only values whose `known_at` falls on or before the end of the origin
   day are visible. A revision published afterwards cannot reach an earlier origin.

Non-publication days stay non-publication days. The feature spans the two most recent
*published* observations, so a weekend simply widens the span it measures rather than
inventing a Saturday value. This is why the EIA features are usable at 393 of 400
origins despite the series having only about 250 observations a year.

## Data

| | |
| --- | --- |
| Price series | 611 days, 2025-01-01 to 2026-09-18 — 587 reported, 24 collected |
| Origins | 400 (`MAX_ORIGINS`), complete 14-day windows |
| Selection | 260 origins |
| Held out, after purging overlapping target windows | 133 origins, 2026-04-14 to 2026-08-24 |

These are the same 400 origins as the forecast evaluation, and the persistence MAE
below reproduces that report's baseline row exactly (1.26 / 2.47 / 3.63 / 4.75 / 5.81
/ 6.82 / 7.77). That agreement is the cross-check that this evaluation is scoring the
same thing the other one did.

### Feed history actually available

| Feed | Observed days stored | Usable selection origins | Usable held-out origins |
| --- | ---: | ---: | ---: |
| `aip` Brisbane wholesale ULP | 6 | 0 | 0 |
| `rba` USD per AUD | 3 | 0 | 0 |
| `eia_gasoline` US Gulf Coast petrol | 488 | 260 | 133 |
| `eia_brent` Brent crude | 496 | 260 | 133 |

**`aip` and `rba` are untestable, not disproven.** This app has held them since
17-18 September 2026 only: six observations and three observations respectively. No
origin in 2025 or the first half of 2026 could read either feed, so there is nothing
to regress. That is a statement about stored history, not about the relationship.
The AIP table yields roughly a week per fetch and the RBA RSS feed one observation per
publication, so this becomes testable after roughly a year of continuous collection —
which the vintage store added by this item now preserves properly.

The EIA rows came from a **one-off extended pull** of the same two series from the same
endpoint the app already uses — one request each, back to 2024-10-01 — because 60 days
of stored history cannot test anything. Their `known_at` is therefore **reconstructed**
as observation date plus publication lag, not recorded. See *Limitations*.

## Per-horizon held-out MAE (c/L, 133 origins, lower is better)

Feature window 7 days. Lower is better; **bold** marks a value below persistence.

### `eia_gasoline` — US Gulf Coast conventional petrol, USD/gal

| Model | h1 | h2 | h3 | h4 | h5 | h6 | h7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| persistence (baseline) | **1.26** | **2.47** | **3.63** | **4.75** | **5.81** | 6.82 | 7.77 |
| drift (intercept only) | 1.36 | 2.62 | 3.82 | 4.96 | 6.03 | 7.04 | 8.02 |
| + lagged feed | 1.70 | 2.81 | 3.86 | 4.86 | 5.82 | **6.77** | **7.63** |
| bootstrap 95% (feed − persistence) | [+0.29, +0.59] | [+0.15, +0.54] | [−0.01, +0.45] | [−0.28, +0.47] | [−0.58, +0.61] | [−0.80, +0.73] | [−1.07, +0.86] |

### `eia_brent` — Brent crude, USD/bbl

| Model | h1 | h2 | h3 | h4 | h5 | h6 | h7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| persistence (baseline) | **1.26** | **2.47** | **3.63** | 4.75 | 5.81 | 6.82 | **7.77** |
| drift (intercept only) | 1.36 | 2.62 | 3.82 | 4.96 | 6.03 | 7.04 | 8.02 |
| + lagged feed | 1.48 | 2.79 | 3.76 | **4.67** | **5.67** | **6.81** | 7.80 |
| bootstrap 95% (feed − persistence) | [+0.13, +0.30] | [+0.12, +0.49] | [−0.13, +0.35] | [−0.51, +0.36] | [−0.71, +0.49] | [−0.60, +0.62] | [−0.62, +0.68] |

Read these tables the same way as the forecast evaluation's.

1. **At h1 and h2 both feeds are significantly *worse* than persistence.** Those
   bootstrap intervals lie entirely above zero. This is not a null result at the short
   horizons; it is a measured loss. Adding an overseas spot price to tomorrow's
   Brisbane forecast makes it worse.
2. **Where either feed is nominally ahead, the interval straddles zero.** `eia_brent`
   at h4 is 0.08 c/L better on average with an interval of [−0.51, +0.36]. `eia_gasoline`
   at h7 is 0.14 c/L better with [−1.07, +0.86]. Those are noise, at horizons where
   every method is already 5-8 c/L out.
3. **Both feeds beat the `drift` control at the longer horizons and still do not beat
   persistence.** That is the control earning its place: the apparent long-horizon
   improvement is mostly the intercept being *less bad* than an unconstrained drift,
   not the feed carrying information.

Horizons earned, by the stated rule (beats persistence **and** beats drift **and**
bootstrap interval entirely below zero): **none, for either feed.**

## Is the null an artefact of the 7-day window?

No. The same test at four feature windows, reporting held-out MAE and whether any
horizon earned inclusion:

| Window | `eia_gasoline` h1 | h4 | h7 | earned | `eia_brent` h1 | h4 | h7 | earned |
| ---: | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :---: |
| 3 days | 1.87 | 4.99 | 7.97 | none | 2.08 | 5.01 | 7.95 | none |
| 7 days | 1.70 | 4.86 | 7.63 | none | 1.48 | 4.67 | 7.80 | none |
| 14 days | 1.36 | 4.74 | 7.78 | none | 1.45 | 5.08 | 8.42 | none |
| 28 days | 1.54 | 5.38 | 8.71 | none | 1.65 | 5.51 | 8.47 | none |

Persistence is 1.26 / 4.75 / 7.77 at those horizons. No window at any horizon produced
a qualifying result, and the short windows are decisively worse. Note that trying four
windows is itself four chances to get lucky — it is reported because searching and not
saying so is how a null quietly becomes a false positive. Nothing was adopted, so the
search cost nothing; had a window won, that win would have needed discounting for the
search.

## What this does not claim

- **The retail−wholesale gap is not station profit.** That difference contains
  freight, storage, retail operating costs, taxes already included in both legs, and
  margin. `gap_note` in `/api/market-context` says so verbatim and is printed verbatim
  in the console. Nothing here estimates profit.
- **Brent is not a refined-petrol benchmark, and neither is the EIA Gulf Coast
  series.** The ACCC identifies Singapore Mogas 95 as the relevant benchmark for
  Australian retail petrol. No free daily Mogas 95 feed has been verified. `eia_gasoline`
  is US Gulf Coast conventional regular petrol — a different market with its own
  specification, seasonality and tax treatment — and it is never relabelled as
  Mogas 95 anywhere in the code, the API or the console.
- **A null result here is not proof that crude and currency do not affect pump
  prices.** They plainly do, over months. What is measured is narrower and is the only
  thing that would justify changing the app: whether a *lagged daily change* in these
  feeds improves a *1-7 day* forecast of the Brisbane median, out of sample, against
  persistence. It does not.

## Verdict

**No feed earned inclusion.**

- `aip` and `rba`: untestable on 6 and 3 stored observations. Reported as untestable,
  never as a null.
- `eia_gasoline` and `eia_brent`: tested properly on 393 origins, and both are
  significantly worse than persistence at h1-h2 and inside noise everywhere else.
- Therefore `build_forecast` is unchanged, `buying_plan` is unchanged, and the console
  keeps saying these feeds are context only.

That is two published negative results in one day, on the same series, by the same
standard. The constraint identified in the forecast evaluation still holds: the
Brisbane cycle is driven by discrete retailer pricing decisions, and neither more
history nor more inputs has yet touched that.

## Limitations and what was not verified

- **The EIA `known_at` timestamps are reconstructed, not recorded.** The app has only
  held these series since 2026-09-18, so for the backfilled rows `known_at` is set to
  observation date + 4 days. This assumes EIA does not revise these two spot series
  after publication. That assumption was **not verified against EIA's revision policy**,
  and it is exactly the assumption the new `context_vintages` table exists to stop
  anyone needing to make again: from now on every first sighting and every revision is
  recorded with the timestamp this app actually held it.
- The one-off EIA pull used two requests against the same endpoint the app already
  polls. No loop, no new provider, no new dependency. The production fetcher is
  unchanged and still requests 60 observations per refresh.
- All 393 usable origins are reported origins, inherited from the forecast evaluation.
  Nothing here measures behaviour on days this app collected itself.
- One feature shape was tested per feed: a lagged change, linear, one feed at a time.
  Levels, ratios, AUD-converted series, interactions between feeds and non-linear
  responses were not tested. A richer feature could in principle succeed where this
  one failed; on this evidence there is no reason to assume it would, and any such
  test must clear the same two controls and the same bootstrap.
- The bootstrap reuses the forecast evaluation's settings (14-day moving blocks, 4,000
  draws, seed 20260918) to absorb the autocorrelation from overlapping 7-day target
  windows. It sizes the edge; it is not a formal test.
- The running server (port 8000) was **not restarted**, so it still serves the
  pre-change `/api/market-context`. The new fields were verified by unit test
  server-side and by injecting the new payload shape into the live console client-side.
- Attribution: Queensland fuel price reporting, © State of Queensland, CC BY 4.0.
  US Energy Information Administration, public domain. AIP and RBA as cited in
  `/api/market-context`.
