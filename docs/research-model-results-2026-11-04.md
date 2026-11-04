# Brisbane U91 modelling experiments — 4 November 2026

No candidate has established reliable high accuracy across tomorrow, next week,
two weeks and four weeks. The useful research result is conditional: station
price distributions contain information in cycling history, but that advantage
must survive a quiet later regime before it can justify a forecast product.

## Target and evidence

The primary target is the equal-weight station daily **median reconstructed from
official reported events with a 14-day report expiry**, in cents per litre. It is
not a transaction-weighted price, an individual station forecast or a direct
observation of every station at midnight. The frozen archive covers 669 daily
dates, 1 January 2025–31 October 2026. Unavailable station prices are excluded.

The post-28 September assessment is a new **retrospective temporal assessment**,
not a prospective or preregistered trial. Previously inspected April–August 2026
dates are development evidence. Dates before 29 September whose forecast targets
fall on or after 29 September are labelled `embargo` and excluded from development
selection. Each horizon has its own origins; scores should not be averaged across
different horizon/date samples to claim a single accuracy percentage.

The source audit documents larger measurement risks: 65.8% of reconstructed
station-days carry a previous report, expiry is a modelling convention, and
monthly public files do not preserve real-time release/revision vintages. The
maximum reported-versus-collected median difference is 8 c/L across available
overlap days. A model error against this target is not automatically an error
against the true market-wide purchase price.

## Fixed candidates and walk-forward procedure

All fitted candidates predict the change from the current median directly and
add that change back to the observed origin median. Forecasts are made after the
Brisbane origin day ends. The current incomplete day is a different information
set and is not covered by this experiment.

| Candidate | Fixed rule |
| --- | --- |
| Persistence | Carry the origin price forward |
| Damped trend | Median daily change over the last seven prices, slope clipped to ±4 c/L/day, geometric damping 0.8 |
| History ridge | Direct change regression, ridge penalty 30, recent price level, 1/2/3/7/14/28-day changes, rolling mean gaps and volatility |
| Panel ridge | History plus station price quantiles relative to median, mean-minus-median, hi20/lo20, matched shares increasing/decreasing by at least 5 c/L, station counts and backward seven-day changes |
| Panel analog | Same panel state, 15 nearest previous states after training-only scaling, fixed inverse-distance weighting |
| Panel + context ridge | Panel plus lagged TGP changes and retail–TGP gap, AUD-converted Brent and Gulf gasoline changes, AUD/USD change |
| Boosted panel + context | Histogram gradient boosting: 100 iterations, seven maximum leaves, learning rate 0.05, L2 penalty 10, minimum leaf size 20, seed 20261104 |

No settings are tuned on the later assessment. Models refit daily using at most
365 calendar days of training origins and at least 90 usable examples. At forecast
origin t, a horizon-h training example is eligible only when its target date
is **no later than t**. Feature scaling is fitted on these eligible training
examples. Missing calendar days are never interpolated. Candidates in each run
use identical complete-case origins.

Upstream values are joined backward by assumed availability date, with a maximum
observation age of 14 days. AIP TGP and RBA FX have a one-calendar-day assumed
publication lag; EIA Brent and US Gulf gasoline have four days. These current
historical files are **not vintage data**. US Gulf gasoline is a different market
from Singapore Mogas 95; conversion to AUD c/L does not remove that difference.
No causal conclusion follows from the extra model features.

MAE is the main loss; RMSE, bias, exact direction including flat outcomes, sample
counts and event counts are retained. Development comparisons use moving-block
bootstrap differences against persistence (2,000 draws, blocks max(14,horizon)).
Intervals are withheld if there are fewer than four blocks or calendar gaps.
These are descriptive, unadjusted comparisons across several candidates, not
confirmatory significance tests.

## Primary results

Fresh origins start on 29 September 2026. The last origin depends on horizon:
30 October at h1 (32 forecasts), 24 October at h7 (26), 17 October at h14 (19),
and 3 October at h28 (five). These are overlapping forecasts over one short quiet
period, not 82 independent experiments. Every horizon has fewer than four of the
declared dependence blocks; fresh block confidence intervals are therefore
withheld. The five month-ahead targets are only 27–31 October.

Fresh MAE in c/L, lower is better:

| Model | h1 | h7 | h14 | h28 |
| --- | ---: | ---: | ---: | ---: |
| Persistence | **0.406** | **1.792** | **2.084** | **1.480** |
| Damped trend | 0.438 | 1.823 | 2.109 | **1.480** |
| Panel analog | 0.539 | 3.840 | 11.216 | 18.149 |
| History ridge | 0.515 | 2.977 | 4.146 | 27.638 |
| Panel ridge | 0.545 | 3.195 | 8.815 | 43.219 |
| Panel + context ridge | 0.912 | 6.024 | 13.394 | 55.462 |
| Boosted panel + context | 0.811 | 5.143 | 13.225 | 9.637 |

No fitted candidate beats persistence at any primary horizon. The damped trend
ties it at h28 because the median daily origin slope is zero for those five
origins. A nominally sophisticated model can extrapolate very badly: the direct
context ridge's 55.5 c/L h28 error is not hidden by clipping its predictions
after inspecting this test. It is evidence against using this specification.

The small baseline errors are **historical errors on quiet October dates**,
not promises about tomorrow, a revived retail cycle or a new supply shock.
In particular, the lower h28 error does not mean month-ahead forecasting is easier:
its five-origin period differs from every shorter-horizon assessment.

Development MAE, excluding embargo targets:

| Model | h1 | h7 | h14 | h28 |
| --- | ---: | ---: | ---: | ---: |
| Persistence | 1.966 | 13.412 | 22.597 | 25.615 |
| Damped trend | 1.914 | 12.459 | 22.003 | 28.483 |
| Panel analog | **1.705** | 9.112 | 18.599 | 25.096 |
| History ridge | 2.168 | 12.716 | 18.273 | **19.744** |
| Panel ridge | 1.829 | **8.358** | 16.883 | 21.040 |
| Panel + context ridge | 1.940 | 8.513 | **16.776** | 21.779 |
| Boosted panel + context | 1.776 | 8.931 | 18.017 | 22.037 |

Selecting the minimum development MAE independently at each horizon chooses panel
analog, panel ridge, panel + context ridge, and history ridge, respectively.
Every selected model subsequently loses to persistence. Choosing persistence
now describes this later period; it does not retrospectively validate a selection
rule that knew the outcome in advance.

The h7 panel ridge reduces development MAE by 37.7% (13.412 to 8.358 c/L), with a
descriptive block-bootstrap difference interval of −7.44 to −2.52 c/L, then raises
fresh MAE by 78.3% (1.792 to 3.195). Thus the local-distribution hypothesis has
useful development support but fails the crucial later-regime test. Adding
upstream variables or nonlinear boosting does not repair that failure. The next
research question is how to recognise when a cycling model should abstain; it is
not justified to deploy the development winner as a general price predictor.

## Target sensitivity: daily mean

The station mean replaces the station median as the target; all settings and
upstream assumptions remain fixed. This removes the median's abrupt jump when a
restoration crosses half the stations. Fresh MAE, c/L:

| Model | h1 (32 origins) | h7 (26 origins) |
| --- | ---: | ---: |
| Persistence | 0.447 | **2.098** |
| Damped trend | **0.433** | 2.614 |
| Panel analog | 0.511 | 4.047 |
| History ridge | 0.633 | 4.180 |
| Panel ridge | 0.522 | 4.096 |
| Panel + context ridge | 0.726 | 6.838 |
| Boosted panel + context | 0.696 | 4.327 |

The damped trend's one-day advantage is only 0.013 c/L (3.0%), below the research
design's practical 10% threshold, and its RMSE is slightly worse (0.647 versus
0.637). The sample is too short for the declared block confidence interval. The
development-selected models are boosting at h1 and panel + context ridge at h7;
both lose on the later assessment. On development, boosting has 39.6% lower h1 MAE
than persistence, illustrating how an encouraging fit can fail a regime change.
The failure of selected models therefore extends beyond the median target.

## Composition sensitivity: fixed station cohort

A 434-station cohort is selected using at least 90% coverage from 15 January
2025–28 September 2026 and then held fixed. Days need at least 90% cohort coverage;
all 33 fresh dates meet it. Development membership uses later information within
development, so its scores are diagnostic; membership is fixed before the fresh
period. This sensitivity reuses exactly the fixed candidate settings at h1/h7.

Fresh MAE, c/L, 32 one-day origins and 26 seven-day origins:

| Model | h1 | h7 |
| --- | ---: | ---: |
| Persistence | **0.506** | **1.815** |
| Damped trend | 0.528 | 2.012 |
| Panel analog | 0.610 | 3.462 |
| History ridge | 0.830 | 4.728 |
| Panel ridge | 0.745 | 3.248 |
| Panel + context ridge | 0.925 | 5.173 |
| Boosted panel + context | 0.939 | 5.590 |

Every fitted alternative loses to persistence in this composition-controlled
later period. Selecting on development chooses panel analog for h1 and panel
ridge for h7; neither selection survives the fresh period. This does not make
the reconstructed target exact, but changing station membership is insufficient
to explain the principal failure.

## Report-expiry sensitivity

The same six lightweight candidates are rerun at h1/h7 with seven-day and
30-day expiry; boosting is not added to these extra sensitivity runs. This tests
the target convention rather than searching for an expiry that makes a model
win. Targets differ across rows, so a lower baseline error is not evidence that
one expiry better represents actual pump prices.

| Expiry | Persistence h1 MAE | Best fitted h1 MAE | Persistence h7 MAE | Best fitted h7 MAE |
| --- | ---: | ---: | ---: | ---: |
| 7 days | 0.481 | 0.519 (damped) | 2.277 | 2.246 (damped) |
| 14 days, primary | 0.406 | 0.438 (damped) | 1.792 | 1.823 (damped) |
| 30 days | 0.450 | 0.464 (damped) | 1.781 | 1.899 (damped) |

No fitted alternative achieves the practical 10% improvement at either tested
horizon under these conventions. The seven-day-expiry damped trend's h7 edge is
only 1.35%, with worse RMSE (2.706 versus 2.654). All linear/analog machine-learning
candidates lose to persistence in both additional expiry runs. This supports
the absence of a robust later-period edge; it does not resolve which expiry
accurately represents unchanged real-world station prices.

## Uncertainty and large-rise warnings

For each candidate and horizon, symmetric 80% and 95% empirical error radii are
estimated from errors of forecasts originating within the preceding 180 days
whose target dates have already arrived. At least 60 such errors are required.
This is prequential historical calibration, not a guarantee of future coverage
under dependent observations or a regime change. Coverage and width are reported
together: a very wide band can cover nearly everything while being unhelpful.

For persistence, fresh 80% empirical bands cover 100% of outcomes, but their mean
full widths are 4.00, 25.20, 44.34 and 73.56 c/L at h1/7/14/28. This is substantial
overcoverage in the quiet period, not evidence of a precisely calibrated 80%
probability. Conversely, the context ridge's h28 80% band covers zero of five
outcomes and its 95% band only one of five. Neither a broad historical error
distribution nor a good pooled MAE guarantees useful uncertainty after a regime
shift. None of these bands warrants a guaranteed savings recommendation.

A separate fixed panel ridge probability model predicts whether the **endpoint
median is at least 10 c/L above the origin price**. Probabilities are clipped to
[0,1] and scored by Brier loss against a trailing historical event-frequency
baseline. This is neither the first retailer's restoration nor the largest
intrahorizon price rise. There are zero such positive events in the fresh period
at every horizon. Any lower Brier loss there measures fewer false alarms; it
cannot establish that the model detects restorations or rare shocks.

## Reproduction and safety checks

From the repository root:

```powershell
$env:OMP_NUM_THREADS='1'
$env:OPENBLAS_NUM_THREADS='1'
$env:MKL_NUM_THREADS='1'
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --output .local/research-2026-11-04/models-final
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --target mean --horizons 1 7 --output .local/research-2026-11-04/models-mean
.\.venv\Scripts\python.exe research/sensitivity_data.py
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported_fixed_cohort.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --horizons 1 7 --output .local/research-2026-11-04/models-fixed-cohort
.\.venv\Scripts\python.exe -m unittest tests.test_research_forecasts -v
```

Runs can take several minutes because models are refitted at every origin. The
application does not import the research dependencies or change its forecasting
gates. No operational database, collector process or user-modified CSV is changed.

The five research tests passed: matured training-label cut-off; unchanged earlier
forecasts after perturbing all future prices; upstream assumed-publication join
and expiry; exclusion of unmatured errors from interval calibration; and undefined
relative skill when the persistence baseline has exactly zero error. These
are evidence of implemented safeguards, not evidence of forecast accuracy.

Frozen primary daily SHA-256:
`8470ab2be2bad0b81186336c6421c81b81415caaa024d9bbe92b44251faff48a`.
Fixed-cohort daily SHA-256:
`5bd5b6a33eb3fbfc5f5c8a699054a4f9ada975fe6ed16f43c7ba76fd48a8e3b5`.

Environment: Python 3.12.14; NumPy 2.3.5; pandas 3.0.1; scikit-learn 1.9.1;
SciPy 1.18.1; threadpoolctl 3.7.0. Raw predictions, errors, interval radii, metrics,
regime summaries, development-only selections and manifests are under each output
directory in `.local/research-2026-11-04/`.

Queensland source attribution: © State of Queensland, CC BY 4.0.
