# Upstream costs: lead/lag diagnostics

4 November 2026. These results describe associations, not identified causal effects.
Model comparisons are in `research-model-results-2026-11-04.md`.

## Inputs and timing

The research downloaded official current histories into an isolated cache, using
the existing registered EIA credential without printing or storing it in outputs.
The production database and context collector were not changed.

| Source | Downloaded observations from 2023 onward | Latest observation | Assumed availability |
| --- | ---: | --- | --- |
| [EIA Brent](https://www.eia.gov/dnav/pet/hist/RBRTED.htm), RBRTE | 964 | 27 October 2026 | observation +4 calendar days |
| [EIA US Gulf conventional gasoline](https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=EER_EPMRU_PF4_RGC_DPG&f=D) | 952 | 27 October 2026 | +4 days |
| [RBA AUD/USD](https://www.rba.gov.au/statistics/tables/csv/f11.1-data.csv) | 964 | 3 November 2026 | +1 day |
| [AIP Brisbane ULP TGP](https://aip.com.au/resources/historical-ulp-and-diesel-tgp-data) | 1,000 | 30 October 2026 | +1 day |

The AIP file actually retrieved was `AIP_TGP_Data_30-Oct-2026.xlsx`. The manifest
records source URLs, SHA-256 content hashes and UTC retrieval times. The series
dates are not necessarily their release dates. In particular, EIA's latest value
is eight calendar days before retrieval; a four-day historical delay is an
assumption and can be optimistic. A second diagnostic doubles all delays to
AIP/RBA two days and EIA eight days. Neither reconstructs historical revisions
or proves historical availability. The modelling report qualifies context results
accordingly.

At each origin, join only observations whose assumed availability has arrived.
Carry a source value for at most 14 days of observation age. Compute trailing
seven-calendar-day changes in this available series. Brent and gasoline are
converted to AUD cents/litre using the FX value available at the same origin:

```
Brent AUD c/L    = USD/barrel / (USD/AUD) × 100 / 158.987
Gasoline AUD c/L = USD/US gallon / (USD/AUD) × 100 / 3.785411784
```

These are untaxed commodity-equivalent prices, not a pump-price estimate.
The two upstream series jointly contain crude/refined spread information, but
the US spread is not the Singapore refining margin. No Singapore Mogas feed was
verified or substituted with another series under that name.

## Following-week retail changes

Pearson correlation between the available trailing seven-day input change at t
and Brisbane reconstructed retail median change from t to t+7, using default
assumed lags:

| Input change | 2025, 358 origins | 2026, 297 origins | New period, 26 origins |
| --- | ---: | ---: | ---: |
| Brisbane TGP | +0.215 | +0.246 | −0.231 |
| AUD/USD | −0.079 | +0.068 | −0.308 |
| Brent in AUD c/L | −0.112 | +0.190 | +0.051 |
| Gulf gasoline in AUD c/L | −0.048 | +0.207 | −0.335 |

The 2026 column includes the new period and is not a separate independent test.
The new period is 29 September–24 October origins, with targets through 31 October.
Overlapping changes make these row counts much larger than the effective number
of independent episodes. Do not read small differences as statistically reliable.
Price-level correlation, which would be inflated by shared trends, is not used.

The direction and strength change with period. For example, available Gulf
gasoline pressure is positively associated with following-week wholesale changes
in 2025 and 2026 (+0.487 and +0.441), while the corresponding retail associations
are −0.048 and +0.207. That is consistent with a wholesale transmission mechanism
and intervening retail dynamics; it does not identify a causal coefficient or
show that the source improves a forecast already containing TGP.

Here the wholesale outcome is the change in the **lagged available TGP series**
from t to t+7, so it is an information-flow diagnostic, not a regression estimating
the structural response of same-day wholesale prices.

## Sensitivity to policy windows and publication delay

Exclude origins and forecast windows intersecting a ±14-day band around known
excise effective dates: 3 February and 4 August 2025; 2 February, 1 April, 1 July
and 3 August 2026. This removes policy-contaminated windows without claiming that
tax pass-through finishes exactly on day 14. Only 286 and 156 h7 origins remain
in 2025 and 2026 respectively.

| Retail h7 association | 2025 default | 2025 double delay | 2026 default | 2026 double delay |
| --- | ---: | ---: | ---: | ---: |
| TGP change, excise windows excluded | +0.176 | +0.180 | +0.343 | +0.296 |
| AUD/USD change, excluded | −0.074 | −0.096 | −0.116 | −0.058 |
| Brent AUD change, excluded | −0.122 | +0.080 | +0.256 | +0.192 |
| Gulf gasoline AUD change, excluded | −0.087 | +0.174 | +0.117 | +0.117 |

Some signs change solely by changing plausible publication delays. Tax exclusion
does not make all upstream relationships vanish, but does not produce a stable,
universal petrol-to-retail relation either. These diagnostics include 384
source/target/horizon/regime/delay/window combinations; they are disclosed as a
descriptive grid, with no selected p-values or claim of independent discoveries.

## Interpretation and remaining identification work

The [ACCC identifies refined Singapore prices and currency as major cost inputs](https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices).
This supports the mechanism, not a claim that our four proxy features can predict
an exact future pump price. Current input prices may already be reflected in TGP
or retail. The key forecasting test is incremental future loss reduction, not
whether two series move together.

The diagnostics do not identify causation. Shared demand, supply disruptions,
freight, FX, policy responses and retailer behaviour confound observational
relationships. TGP also mediates part of an upstream effect: controlling for it
changes the causal question. A causal project would need a prespecified
instrument or defensible event/control design, announcement-aware policy dates,
and sensitivity to coincident shocks. Another Australian petrol city is not an
untreated control for a nationwide excise change.

## Additional exploratory tax baseline

After the initial price-model results were visible, a separate diagnostic tested
`persistence + announced future change in embodied excise × 1.1 GST`. A fixed
seven-day linear inventory transition represents delayed retail pass-through;
the duration is an assumption, not an estimated causal response. The calendar
uses the verified primary documents in the literature report, with availability
assumed one day after each announcement/document date. April's 26.3 c/L cut is
visible from 31 March, and the additional 5.7 c/L only from 3 April. Scheduled
July/August reversions are superseded only after later announcements arrive.

| Horizon | Development origins | Persistence MAE | Tax baseline MAE | Origins where tax rule acts |
| --- | ---: | ---: | ---: | ---: |
| 1 | 635 | 1.806 | 1.812 | 42 |
| 7 | 629 | 12.171 | 12.053 | 62 |
| 14 | 622 | 21.047 | 20.870 | 76 |
| 28 | 608 | 24.863 | 24.793 | 103 |

Development targets end before 29 September; origins whose targets cross that
boundary are embargoed. This rule needs no fitted training period, so its
origin counts differ from those of the fitted models. Scores cannot be compared
across those different samples. At h7 the overall improvement is about 1%; on
the 62 origins where the adjustment is nonzero, MAE is 12.777 versus 11.578 c/L.
No tax adjustment applies in the fresh October slice, so its forecasts and errors
there are exactly persistence. The result supports retaining an announcement-aware
policy control, but does not establish a generally accurate forecasting model.

This late exploratory test is disclosed separately from the seven fixed main
candidates. There is no claim of independent confirmation, optimal transition
duration, or causal identification. Three tests verify that later announcements
and revisions cannot change an earlier forecast and that the July extension
supersedes the earlier reversion only once its announcement is available.

Reproduction: `python research/context_data.py`, then
`python research/context_experiments.py` after the read-only data export.
The separate tax diagnostic is `python research/excise_experiments.py`.
Full numerical diagnostics and acquisition manifest are in
`.local/research-2026-11-04/`; the underlying histories can change on redownload.
