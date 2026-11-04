# Mechanism-separated Brisbane forecasts — 4 November 2026

**There is now a useful week-ahead research contender.** Separating station price
restorations from wholesale movements lowers retrospective seven-day MAE from
12.26 to 7.78 c/L over 1,254 forecast origins, a 36.5% reduction. A fixed online
selector that can abstain achieves 8.39 c/L, a 31.5% reduction, and falls back to
persistence during the already-examined quiet October period.

This is not a claim of high guaranteed accuracy or independent confirmation.
All September/October dates were already inspected before this model was designed.
The longer archive and year blocks are chronological diagnostic tests. The new
specification was fixed before its first fit, but the wider research process is
adaptive. Nothing here changes the production dashboard or buying advice.

## Question and expanded evidence

Does the local diffusion of restored station prices predict the retail cycle,
while upstream inputs predict a separate cost component? Can a fixed rule select
these components only after their matured errors demonstrate an advantage?

The data team acquired 2023–2024 official monthly reports and combined them with
the existing 2025–2026 archive. The primary data now cover 1,400 calendar days,
1 January 2023–31 October 2026, 515 stations and 311,960 unique report events.
Prices are reconstructed as the final reported station price per day, carried
forward for at most 14 days, then equally weighted across available stations.
Unavailability is excluded. This remains a reconstructed city median, not a
transaction-weighted price or a direct midnight observation at every station.

The 2023 source timestamps use ISO format in some files; the isolated acquisition
adapter handles that explicitly rather than silently dropping those rows. The
extended history changes only four January 2025 daily medians in the previous
archive, from removing its cold start. The later 2025 and all 2026 medians are
unchanged. Data provenance and raw source hashes are retained separately.

## The accounting decomposition

Define R as the retail median and W as the latest Brisbane TGP published by the
end of each origin date. Define M = R − W. Exactly:

`R(t+h) − R(t) = [W(t+h) − W(t)] + [M(t+h) − M(t)]`.

The two forecasts are estimated separately and added once. The future W term is
a training **label**, never a feature used before that date arrives. A training
example is eligible only when its target date is no later than the current
forecast origin. The origin and target use the same as-of wholesale definition,
preventing a lag mismatch or double counting.

M is an accounting retail–wholesale gap, **not retailer profit**. It includes
other costs and aggregation differences. A large expansion is a proxy for a
cycle restoration, not proof of the retailer's motive or a causal identification
of market power. Current historical upstream files use assumed publication lags
(AIP/RBA one calendar day; EIA four), rather than recorded historical vintages.

## Fixed specification

| Model | Mechanism and constraints |
| --- | --- |
| Persistence | Carry current retail median forward |
| Damped trend | Existing median seven-price daily slope, slope bounded ±4, damping 0.8 |
| Cost only | Predict future TGP change from known TGP three-/seven-day changes and AUD-converted US Gulf gasoline seven-day change; nonnegative ridge, zero intercept, 50% shrinkage to zero |
| Diffusion hurdle | Predict whether endpoint M rises at least 10 c/L; call a jump only at probability ≥0.5 and hi20 ≥10%; use the median of past positive jump sizes; otherwise allow only observed negative gap erosion |
| Combined | Add predicted TGP change and predicted M change to R, once each |
| Online selector | Choose among the original challengers using matured paired errors only; otherwise use persistence |

The event classifier uses five origin features: hi20, its backward three-day
change, matched station hike breadth averaged over three days, q90-minus-median,
and backward three-day M change. There is no absolute price-level predictor and
no assumed fixed calendar cycle. Logistic scaling uses eligible training data
only. Its ridge penalty is 5, with a smoothed historical event-rate intercept.

Cost fitting uses the last 365 calendar days, exponential recency weighting with
180-day decay, ridge penalty 20 and RMS scaling. RMS scaling preserves an exact
zero-change anchor; all-zero cost changes produce exactly zero forecast change.
The already-specified 50% shrinkage and ±30 c/L cost cap limit extrapolation.
US Gulf gasoline remains a proxy from a different market, not Singapore Mogas.

The hurdle uses 730 calendar days and at least 120 matured examples. It needs at
least five positive labels to estimate a jump size, bounded 10–60 c/L. Negative
gap erosion activates only when the median recent daily M slope is below
−0.25 c/L/day; its slope is bounded at −3 and damped geometrically at 0.8. A quiet
state therefore receives no automatic positive drift or scheduled cycle jump.

Before selecting a challenger, the selector examines at most 180 calendar days
of already-matured paired losses. It needs four contiguous blocks of length
max(14,horizon), at least 10% mean MAE reduction, negative loss differences in at
least 75% of blocks, and a negative mean-plus-1.96-block-standard-error value.
This last check is a conservative heuristic, not a formal significance test.
Late-scored live outcomes cannot vote before their actual score timestamp.

These settings were not adjusted to make October look good. Parameter state,
probabilities, component predictions, selection choices, input hashes and losses
are exported for inspection. No calibrated price interval is claimed here.

## Chronological results

Models refit using only matured labels at every origin. Baselines and challengers
are scored on identical qualified dates. Forecast counts are 1,266/1,254/1,240/
1,212 at horizons 1/7/14/28 respectively. They are overlapping observations.

MAE in c/L over all diagnostic origins:

| Model | h1 | h7 | h14 | h28 |
| --- | ---: | ---: | ---: | ---: |
| Persistence | 1.812 | 12.255 | 21.175 | 24.366 |
| Damped trend | 1.767 | 10.932 | 20.156 | 26.556 |
| Cost only | 1.869 | 12.100 | 21.171 | **24.189** |
| Diffusion hurdle | 2.030 | 8.110 | 16.926 | 26.026 |
| Combined | 1.985 | **7.781** | **16.818** | 25.774 |
| Online selector | **1.744** | 8.392 | 17.699 | 24.425 |

The principal gain is at a week. Tomorrow's selector improvement is only 3.75%,
below the practical 10% research threshold; the individual mechanisms lose at h1.
At two weeks the combined and selector improve by 20.6% and 16.4%, respectively,
but substantial absolute error remains. At four weeks no meaningful advantage is
established: cost-only's 0.7% apparent edge is inside bootstrap uncertainty, while
the combined model and selector lose.

For context, the combined model also improves h7 MAE by 28.8% relative to damped
trend, not only relative to persistence. Retrospective usefulness is therefore
more than a comparison against a single especially weak baseline.

## Year and origin-state checks

Year blocks exclude origins whose targets cross the year boundary. Training
continues chronologically within each year; these are walk-forward year slices,
not a claim that the entire year was forecast from its first day.

| Year | h7 origins | Persistence MAE | Combined MAE | Selector MAE | Selector reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2023, partial after warm-up | 219 | 12.887 | 8.201 | 9.755 | 24.3% |
| 2024 | 359 | 13.374 | 7.734 | 7.746 | 42.1% |
| 2025 | 358 | 12.636 | 7.138 | 8.052 | 36.3% |
| 2026 through October | 297 | 10.155 | 8.434 | 8.750 | 13.8% |

The sign of the weekly improvement is consistent across the four year slices,
but uncertainty is materially weaker in 2026. With 56-day blocks, the 2026
combined-minus-persistence interval is −3.41 to +0.62 c/L and the selector's is
−2.95 to +1.13. It would overstate the evidence to declare the current regime
independently validated.

Origin regimes are defined without looking ahead: diffusion visible when hi20 is
at least 10%; otherwise wholesale moving when known TGP moved at least 5 c/L over
seven days; otherwise quiet/other. They describe observable states, not causes.

| Origin state | h7 origins | Persistence MAE | Combined MAE | Selector MAE |
| --- | ---: | ---: | ---: | ---: |
| Diffusion visible | 172 | 31.186 | 11.970 | 14.732 |
| Wholesale moving | 236 | 9.130 | 7.356 | 7.798 |
| Quiet/other | 846 | 9.278 | 7.047 | 7.268 |

The largest gain occurs when some stations have already restored while the city
median has yet to cross them. That is consistent with diffusion providing advance
information about the **median's** movement. It is not prediction of the first
station to move. Quiet/other includes gradual erosion and cycle states below the
hi20 threshold; it should not be read as an invariant flat-price regime.

In the specifically inspected late-September/October period, the selector uses
persistence and matches its MAE at every horizon. The ungated combined model still
loses at h1/h7/h28. The fallback is doing useful work; the underlying components
did not suddenly become reliable in every state.

## Dependence-aware uncertainty and remaining error

Derived moving-block bootstraps use 2,000 draws and 56-/84-day calendar blocks,
long enough to span more than the typical multiweek cycle. The all-origin h7
combined-minus-persistence MAE difference is −4.475 c/L:

- 56-day block descriptive 95% interval: −5.674 to −3.442 c/L.
- 84-day block interval: −5.896 to −3.354 c/L.

For the selector, the difference is −3.864 c/L; the corresponding intervals are
−5.256 to −2.802 and −5.535 to −2.733. Two-week differences also remain below zero
in these pooled diagnostics. Tomorrow's selector interval crosses zero, as does
the four-week comparison. Year slices with fewer than four blocks receive no
interval. These intervals are unadjusted, retrospective descriptions after an
adaptive research process, not prospective guarantees.

The weekly combined error distribution remains consequential: median 4.67 c/L,
90th percentile 17.02, 95th percentile 28.63, maximum 56.31. On 22.9% of origins it
misses by at least 10 c/L, compared with 43.9% for persistence. This is meaningful
improvement with a substantial remaining risk; it does not satisfy a promise of
uniformly high accuracy.

## Event and component interpretation

At h7 the margin-expansion probability has Brier loss 0.0522 versus 0.1292 for
the trailing historical-frequency baseline. The fixed jump calls have precision
87.0% and recall 67.6% across 1,254 **overlapping forecast days**, including 188
positive endpoint labels and 146 calls. These are not 188 independent restoration
episodes. Tomorrow's event Brier loss is worse than the frequency baseline,
consistent with the poor h1 point forecasts. The horizon distinction matters.

The wholesale component alone reduces future-TGP h7 MAE from 3.682 to 3.341 c/L,
whereas the margin component reduces its own zero-change baseline from 12.011 to
7.780. Consequently most pooled weekly retail improvement comes from the local
margin/diffusion component. Wholesale has more contribution on wholesale-moving
days and in 2026. Component error magnitudes do not add directly to retail MAE,
because signed errors can cancel. Neither decomposition nor forecast improvement
proves an international price's causal effect.

## Report-expiry sensitivity

The same six original models are rerun at h7 on extended seven-day and 30-day
expiry histories without changing any setting:

| Expiry convention | Persistence MAE | Combined MAE | Combined reduction | Selector MAE | Selector reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| 7 days | 12.324 | 8.019 | 34.9% | 8.649 | 29.8% |
| 14 days, primary | 12.255 | 7.781 | 36.5% | 8.392 | 31.5% |
| 30 days | 12.226 | 7.704 | 37.0% | 8.374 | 31.5% |

This stability makes the weekly edge less likely to be an artefact of exactly
14-day expiry. Different rows define different reconstructed targets; their
absolute errors do not identify the correct real-world freshness convention.
Under both alternatives the selector still uses persistence in the seen late
2026 period.

## Operational interface and reproducibility

The callback is
`research.mechanism_models:predict_candidates(frame, upstream, origin, horizon, prior_losses=None)`.
It returns model names mapped to point predictions and JSON-serializable state.
Upstream rows require either explicit `known_at` vintages or explicitly labelled
research `assumed_available_date`; missing publication timing is rejected.
Legacy naive collector timestamps are interpreted as Brisbane local time, while
aware offsets are respected. Later revisions cannot enter an earlier origin.

Mechanism keys are omitted with explicit reasons in selector state when training
or current source coverage is insufficient. The current sparse collection and
short recorded upstream-vintage history must not silently borrow retrospective
features as if they were observed live. The shadow logger can retain a frozen
historical **training** corpus separately, but current issuance gates still need
completed, sufficiently covered live origin observations.

Reproduce the primary diagnostic run from the repository root:

```powershell
$env:OPENBLAS_NUM_THREADS='1'
$env:OMP_NUM_THREADS='1'
.\.venv\Scripts\python.exe research/mechanism_models.py
.\.venv\Scripts\python.exe research/mechanism_diagnostics.py
.\.venv\Scripts\python.exe -m unittest tests.test_research_mechanisms -v
```

Primary outputs are under `.local/research-2026-11-04/mechanisms/`: every forecast,
component/state parameters, daily state features, year/regime metrics, selector
choices, long-block uncertainty, event scores, component scores and tail errors.
Expiry runs are `mechanisms-7day/` and `mechanisms-30day/`. The manifest records
all fixed settings, source/code hashes and versions. Source daily SHA-256 is
`2babbe0562867244fc97b8ed7986dbc7935f6288f044b541c26e821e649f130a`.

The eight mechanism tests pass, covering future-price and revision invariance,
actual availability and local timestamp handling, absence of invented release
times, the zero-change cost anchor, additive decomposition, matured/scored loss
gates and retail-ablation isolation from the original selector. Passing these
checks establishes implemented timing safeguards, not empirical accuracy.

The next defensible step is a frozen forward shadow trial of the weekly
contender alongside persistence, retaining losses and explicit abstentions. A
new shock or restoration must be evaluated before any production promotion.
Source attribution: Queensland fuel price reporting, © State of Queensland,
CC BY 4.0; upstream publishers as recorded in the acquisition manifest.
