# Brisbane fuel forecasting: research design

Research lead: Codex. Date: 4 November 2026, Australia/Brisbane.

## Question and scope

Using only information available by the end of Brisbane day t, can we forecast
the station-weighted Brisbane metropolitan U91 daily median at t+1, t+7, t+14 and
t+28 more accurately than carrying the current median forward? Can observed
station price dispersion warn of a substantial rise, and can upstream costs
explain additional out-of-sample variation?

The end-of-day origin is intentional: today's incomplete median cannot be used
to promise an exact forecast from the current clock time. Prices are cents per
litre. A forecast of a city median is not a forecast of an individual station,
the cheapest reachable station, or the price of diesel. Other Australian cities
and studies inform mechanisms; empirical conclusions here apply to Brisbane U91.

## Falsifiable hypotheses

1. **Persistence is difficult to beat at one day.** Test direct change forecasts,
   rather than judging a high level-price correlation or R-squared as success.
2. **Local cycle state adds information.** Quantiles, the share of stations at
   least 20 c/L above the median, and price-change breadth improve direct forecasts
   or warnings beyond aggregate-price lags. Thresholds already examined in the
   September studies are hypotheses, not independently established discoveries.
3. **Upstream cost pressure adds information at longer horizons.** Lagged Brisbane
   terminal-gate prices (TGP), Brent, US Gulf gasoline, and AUD/USD improve forecasts
   after controlling for recent local movements. Compare nested feature sets on
   identical origins. US Gulf gasoline is explicitly a proxy, not Singapore Mogas 95.
4. **There is no stable calendar-only cycle.** A seven-day seasonal rule and fixed
   cycle timing should fail when Brisbane shifts between cycling and cost-driven
   regimes. Compare performance by time period, not only a pooled average.
5. **Excise changes confound simple oil/retail relationships.** Known announced
   excise steps and structural breaks must be considered before attributing a
   price movement to international markets.

## Evidence and safeguards

- Read production data without modifying it. Save research inputs and raw outputs
  under `.local/research-2026-11-04`; keep scripts and reports reviewable in Git.
- Official monthly QLD files are retrospective price-report events. Reconstruction
  may carry the latest known report forward for at most 14 days. Never backfill
  an earlier day using a later report. Exclude explicit unavailability sentinels.
- Separate that event-time reconstruction from locally collected snapshots and
  genuine forecasts archived before their outcomes. Retrospective event timing
  does not prove publication completeness or absence of later corrections.
- Historic AIP, EIA and RBA downloads are current files, not historical vintages.
  Assumed availability lags: AIP and RBA one calendar day, EIA four calendar days.
  Sensitivity and revision risk must qualify conclusions using those files.
- Previous studies inspected April–August 2026 repeatedly: this is development
  evidence. Dates after 28 September are a new temporal assessment for this work,
  but a short retrospective test is not prospective validation or preregistration.
- Restrict every model's training labels to outcomes matured by its origin;
  fit normalization on those training rows only. Never randomly split time series.
  Record fixed candidate settings and report every tested candidate.
- Evaluate each horizon separately. Compare MAE and RMSE against persistence on
  the same origins, plus direction where informative. Report counts and regime
  coverage. Overlapping forecasts are dependent: use time-block resampling and
  show when there are too few independent blocks to support confidence claims.
- Estimate prediction intervals from errors that matured before each origin;
  evaluate coverage and width on subsequent observations. Historical coverage
  is not a guarantee under new shocks.

## Decision standard

An exploratory winner is a candidate for a future trial, not a validated product.
For any future promotion, seek at least 10% MAE reduction versus persistence at
the intended horizon, an improvement robust to dependence-aware uncertainty and
reasonable lag assumptions, calibrated useful intervals, and no material loss
in the relevant recent regime. Predeclare the promotion trial before collecting
its outcomes. The 10% threshold is a practical research choice, not a previously
established industry standard. A short final period cannot establish it reliably.

An honest result may be that no candidate meets that standard. Do not adjust
gates or deploy a model just to satisfy the desired high accuracy.

## Economic interpretation

Test upstream lead/lag relationships in differences, with local lags and excise
regimes. Shared trends can generate impressive correlations with no forecast
value. Predictive precedence (including Granger-style comparisons) does not
identify causation. Establishing a causal effect needs defensible exogenous
variation, an instrument or credible policy design, controls, and sensitivity to
shared demand and supply shocks. This project can measure forecast contribution
and mechanism consistency; observational associations alone cannot prove cause.

## Teams and deliverables

- Data team: provenance, coverage, read-only exports and acquisition feasibility.
- Modelling team: reproducible walk-forward candidate comparison and uncertainty.
- Literature team: primary Australian papers, external mechanisms and limitations.
- Research lead: upstream acquisition and lag analysis, adversarial review,
  integrated findings and next experimental protocol.

Reports: `research-data-audit-2026-11-04.md`,
`research-model-results-2026-11-04.md`, `research-literature-2026-11-04.md`,
and the integrated `research-findings-2026-11-04.md` in this directory.
