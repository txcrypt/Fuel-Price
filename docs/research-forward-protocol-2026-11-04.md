# A forward trial that could justify a forecast

Proposed on 4 November 2026. This document does not install a job, issue a current
price forecast, or authorize a production model change. It states the evidence
still needed after the retrospective investigation.

## Freeze before collecting outcomes

Choose one challenger per horizon from **development** data only, using the
recorded development selection rule. Archive its exact source, dependency
versions, hyperparameters, data definitions and hash. Keep persistence as the
control. If a selector or ensemble is proposed instead, freeze its complete
selection rule as the challenger; do not choose whichever constituent happens to
win the subsequent trial.

October has now been examined. No newly designed rule may call October its
untouched confirmation set. Candidate changes start a separate future trial.
The current experiments do not establish an improved forecasting algorithm.

## Observation and issuance

Use a specified Brisbane boundary and U91 fuel definition. Collect consistently,
retain unavailable reports and distinguish price-change time from capture time.
Use equal station weights and a single documented freshness policy. Maintain a
fixed-cohort sensitivity in addition to the available-station market target.

Issue after the Brisbane day has closed, for example 00:10 AEST. The origin is the
previous completed Brisbane date t. Targets t+1, t+7, t+14 and t+28 are daily
closing medians under the same definition. A forecast made during the day needs
a different target/cutoff and cannot inherit end-of-day accuracy claims.

Retain an immutable record containing:

- exact issuance timestamp in UTC and AEST, origin cutoff and target date;
- model identifier/version, code hash, dependency versions and candidate settings;
- raw input snapshot hashes, station coverage, provenance and all source vintages;
- point forecast, interval endpoints, jump probability and explicit event definition;
- the simultaneous persistence forecast using precisely the same origin target;
- exclusion reason if data are stale, missing or outside the validated conditions.

Store observed outcomes separately, with their own timestamps and provenance.
Never overwrite an issued forecast following a data correction. Score originally
available outcomes and corrected outcomes separately where possible. Effective
excise dates must be accompanied by their genuine prior announcement information.
The full April 2026 change must not be retroactively exposed at the earlier,
partial announcement.

## Duration and analysis

Seek at least 180 completed consecutive origin days, then wait for the last
28-day outcome to mature. The count is a practical minimum, not a power guarantee:
180 overlapping h28 origins contain only about six nonoverlapping 28-day blocks.
If the period contains no restorations, it cannot validate restoration forecasts.
Require several distinct rise episodes before assessing event sensitivity.

For every horizon, compare the challenger and control on identical dates:

- MAE, RMSE, signed bias and severe-error frequency;
- block-resampled paired loss differences, with block lengths at least comparable
  to the forecast horizon and sensitivity to longer cycle-length blocks;
- empirical interval coverage with interval width, scored from prior matured
  residuals; no distribution-free coverage guarantee under regime changes;
- performance in quiet, cycling, wholesale-shock and policy-change conditions
  identified using information available at the origin;
- target sensitivity to mean versus median, cohort composition and report expiry.

Report simultaneous uncertainty if making four horizon-specific success claims;
a simple prespecified option is Bonferroni adjustment across the four horizons.
Do not treat daily overlapping origins as independent replications. Declare
missing-data handling and outage exclusion before scoring; disclose exclusions
and score the operational cost of withholding forecasts.

For a jump warning, define the event in advance. The current research uses an
**endpoint** median at least 10 c/L above origin; it does not measure any rise
within the window, the first station moving, or an executable buying opportunity.
Brier score during a period with no events evaluates false alarms only. Price
direction accuracy must explicitly distinguish flat prices.

## Promotion or rejection

The practical initial target is at least a 10% MAE reduction versus persistence
at the intended horizon, a dependence-aware uncertainty interval supporting an
improvement, useful and reasonably calibrated intervals, and robust performance
under target/timing sensitivities. Inspect severe losses and relevant recent
regimes as well as averages. A single numerical gate is not sufficient.

If the evidence is inconclusive, keep the candidate in research and continue the
unchanged trial. If it loses, publish that result and retire or redesign it for a
new trial. No model should be promoted merely because its historical level-price
fit is high or because a more complex input set sounds economically plausible.

## What this research still cannot identify

Even a successful forward forecast does not prove that a source causes Brisbane
price changes. Causal pass-through estimates require a separately specified
identification design and defensible assumptions. Nor does accurate city-price
forecasting automatically minimize a driver's total spend: that requires a
separate executable buying-policy evaluation including travel, tank and usage
constraints.
