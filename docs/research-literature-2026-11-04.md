# Australian fuel forecasting: literature and economic identification

Research date: 4 November 2026, Australia/Brisbane. This is a research review and a specification of hypotheses, not a claim that a new model has achieved reliable accuracy. Empirical results from this investigation belong in the companion model and data reports.

## Research question and distinct outcomes

Using information available at a specified Brisbane forecast cutoff, can we predict the Brisbane metropolitan U91 daily price at 1, 7, 14 and 28 calendar days ahead better than carrying the latest price forward, and can we give a useful warning of a large increase?

Keep three outcomes separate:

1. **Price prediction:** daily equally weighted station median, with mean and fixed-station-panel sensitivity checks. Report errors in cents per litre, by horizon.
2. **Jump warning:** probability that the city price increases substantially within a fixed future window. Define the jump threshold and whether this means a terminal increase or the maximum during the window before evaluating.
3. **Buying value:** net cost of an executable purchase policy subject to tank capacity, consumption and travel costs. A successful timing policy does not establish accurate forecasts of every future daily price.

The new contribution should be an honest local replication and extension: station-distribution signals, observable regime changes, wholesale/FX availability, separate tax events, and a genuinely later evaluation. It should not be a claim to improve the literature simply because a more flexible model fits the same history.

## Evidence reviewed

The table distinguishes original full texts from publisher previews. Where full text was restricted, only the accessible abstract/introduction/section snippets support the description; no unverified accuracy number, sample date or model equation is inferred.

| Original study and access | Verified finding | Consequence for Brisbane research |
| --- | --- | --- |
| Noel and Chu (2015), *Forecasting gasoline prices in the presence of Edgeworth Price Cycles*, Energy Economics 51, 204–214; [author manuscript, full text](https://www.noeleconomics.com/articles/NOEL_forecasting.pdf), [DOI](https://doi.org/10.1016/j.eneco.2015.06.017). | Daily city-average retail and terminal gate prices for five capitals including Brisbane, January 2007–June 2013. Simple purchase rules updated from prior periods could reduce prices paid. Full adjustment performed better than more elaborate updating; buying after a spike while some stations remained cheap was particularly effective. The loss from buying late was much larger than buying slightly early. | Replicate executable timing logic and asymmetric regret separately from city-price MAE. Updating a threshold and predicting price levels are different tasks. Its old cycling sample cannot establish performance during the 2026 Brisbane regime. |
| Clements and Otero (2025), *Forecasting retail fuel prices with spatial interdependencies*, Economics Letters 247, 112128; [original publisher preview](https://www.sciencedirect.com/science/article/pii/S0165176524006128), [DOI](https://doi.org/10.1016/j.econlet.2024.112128). Full text restricted. | Greater Perth FuelWatch station-level ULP forecasting. Accessible methods describe rolling estimation with 500 observations and iterative forecasts. Error-correction models incorporating cross-sectional dependence outperform the alternatives studied; autoregressions augmented with wholesale or spatial effects rank next. | Test lagged station/nearby-station and city-distribution features. This is evidence motivating a candidate, not a Brisbane replication or a verified percentage gain. Perth's regulated daily price-setting environment differs from Queensland. |
| Holt, Igami and Scheidegger (2024), *Detecting Edgeworth Cycles*, Journal of Law and Economics 67(1); [journal record](https://chicagounbound.uchicago.edu/jle/vol67/iss1/3/), [full author preprint](https://arxiv.org/html/2111.03434). | Compares ten cycle-detection methods using WA, NSW and German station data. Distinguishes cyclicality from simple asymmetry; Fourier/Lomb–Scargle, spline and machine-learning approaches can capture patterns missed by simple rules. Classification method affects conclusions about cycles and margins. | Detection of a cycle in an observed window is not a future-price accuracy result. Use trailing windows only when constructing forecast features; a full-quarter label would leak information into earlier dates. Start with transparent diagnostics before complex classifiers. |
| de Roos and Katayama (2013), *Gasoline Price Cycles Under Discrete Time Pricing*, Economic Record 89(285), 175–193; [original publisher abstract](https://onlinelibrary.wiley.com/doi/abs/10.1111/1475-4932.12036). | Characterises WA's discrete daily pricing using a Markov-switching regression, including cycle disruption and evolution. | A regime model is a plausible candidate. Estimated regimes must be filtered using past observations; full-sample smoothed states would give the model knowledge of subsequent prices. WA coefficients should not be copied into Brisbane. |
| Valadkhani (2013), *Modelling the terminal gate prices of unleaded petrol in Australia*, Economic Modelling 33, 233–243; [author university record](https://rune.une.edu.au/entities/publication/aff813f5-2f92-4287-952d-cf61c49bd3e0), [DOI](https://doi.org/10.1016/j.econmod.2013.04.012). Accessible abstract. | Across 18 Australian terminals, exchange-rate responses were asymmetric in ten; downward stickiness was found in eight. Singapore-price asymmetry appeared in only three terminals. | Test positive and negative FX/upstream changes separately as an ablation; do not assume one universal pass-through coefficient or uniform asymmetry. This is wholesale response evidence, not a demonstrated Brisbane retail forecast gain. |
| Valadkhani (2013), *Do petrol prices rise faster than they fall when the market shows significant disequilibria?*, Energy Economics 39, 66–80; [original publisher preview](https://www.sciencedirect.com/science/article/abs/pii/S014098831300073X), [DOI](https://doi.org/10.1016/j.eneco.2013.04.009). | Weekly data for 111 Australian locations, 2007–2012, found asymmetric adjustment in 28 locations, conditional on the size and sign of retail/wholesale disequilibrium. | A threshold error-correction candidate has an economic rationale, but conclusions must be checked against frequency and breaks. |
| Chua, De Silva and Suardi (2017), *Do petrol prices increase faster than they fall in market disequilibria?*, Energy Economics 61, 135–146; [original publisher preview](https://www.sciencedirect.com/science/article/abs/pii/S0140988316303097), [DOI](https://doi.org/10.1016/j.eneco.2016.10.024). | Daily Queensland data, structural-break-aware cointegration and threshold error correction challenge the earlier result. Asymmetry was found at only four of 28 stations; weekly aggregation and omitted regime shifts can mislead. | This directly argues against imposing rockets-and-feathers behaviour as a fact. Check relation stability and compare symmetric/asymmetric models out of sample. |
| Byrne, de Roos and Tiong (2014 working paper), *The Internet, Search, and Asymmetric Pricing: A Natural Experiment in Retail Gasoline*; [author paper/abstract](https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID2525085_code892730.pdf?abstractid=2525085&mirid=1&type=2). Accessible author abstract. | Exploits a policy introducing a price clearinghouse in some markets but not others, reporting changed asymmetric adjustment and faster pass-through of falling costs. | A comparison group and policy change offer identification beyond price correlation. Search/transparency and competition can change the process itself. This historical policy result is not evidence that any current Brisbane input has been causally identified. |

## Current official context and verified tax events

The [ACCC price-cycle page](https://www.accc.gov.au/consumers/petrol-and-fuel/petrol-price-cycles-in-the-5-largest-cities) reports an approximately 6.5-week Brisbane cycle in 2025, but says regular cycles mostly ceased in Brisbane, Sydney, Melbourne and Adelaide after late February 2026. Therefore a rigid weekly seasonal forecast or a fixed time-to-next-cycle model is a weak prior for this dataset. This is external evidence consistent with the repo's regime diagnosis, rather than an explanation estimated by our model.

The [ACCC benchmark explanation](https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices) identifies Singapore Mogas 95 for petrol, Singapore Gasoil 10 ppm for diesel, and AUD/USD as important transmission inputs. It describes roughly two weeks of international-to-domestic transmission and illustrates a ten-day lag of a seven-day benchmark average. These are descriptive guides, not a guaranteed forecasting lag for every station or regime. Crude oil is an input to refined fuel; it is not the complete import cost.

Tax changes are material confounders and sometimes known future inputs:

| Effective period | Petrol/diesel excise, c/L before GST | Change including 10% GST | Primary evidence / availability |
| --- | ---: | ---: | --- |
| 1 January–2 February 2025 | 50.6 | — | The preceding rate was set from 5 August 2024: [ABF notice 2024/29](https://www.abf.gov.au/help-and-support-subsite/CustomsNotices/2024-29.pdf). |
| 3 February–3 August 2025 | 50.8 | +0.22 | [ABF notice 2025/02](https://www.abf.gov.au/help-and-support-subsite/CustomsNotices/2025-02.pdf), dated 30 January 2025. |
| 4 August 2025–1 February 2026 | 51.6 | +0.88 | [ABF notice 2025/19](https://www.abf.gov.au/help-and-support-subsite/CustomsNotices/2025-19.pdf), dated 30 July 2025. |
| 2 February–31 March 2026 | 52.6 | +1.10 | [ABF notice 2026/04](https://www.abf.gov.au/help-and-support-subsite/CustomsNotices/2026-04.pdf), dated 28 January 2026. |
| 1 April–30 June 2026, retrospective effective rate | 20.6 | −35.2 | [Infrastructure department announcement, 2 April](https://www.infrastructure.gov.au/department/media/news/fuel-excise-and-heavy-vehicle-road-user-charge-relief-australian-drivers), confirms the full 32 c/L reduction effective 1 April. Full reduction was not all announced on 30 March; see below. |
| 1 July–2 August 2026 | 36.6 | +17.6 | [PM release, 21 June 2026](https://www.pm.gov.au/media/additional-fuel-excise-relief-month-july) confirms 16 c/L relief relative to the normal rate and these effective dates. 36.6 is arithmetic from 52.6−16.0. |
| From 3 August 2026 | 53.7 | +18.81, conventionally 18.8 | [ABF notice 2026/30](https://www.abf.gov.au/help-and-support-subsite/CustomsNotices/2026-30.pdf), dated 29 July 2026, gives the indexed rate; [ACCC 7 August report](https://www.accc.gov.au/system/files/weekly-fuel-price-monitoring-report-7-august-2026.pdf) confirms the restoration and total retail-tax effect. |

The ABF notices concern excise-equivalent import duty and explicitly state equivalent ATO excise changes. Their signed dates are documented availability proxies, not proof of an intraday retrieval timestamp. A conservative forecast can make such values available the following day and test a further delay.

The release date of the July decision is **21 June**, not an assumed 20 June. April requires special care: the [30 March PM announcement](https://www.pm.gov.au/media/fuel-excise-halved-three-months) announced halving, while the [2 April PM agreement with states](https://www.pm.gov.au/media/government-delivering-more-fuel-relief-through-deal-states) added a further 5.7 c/L, making the total cut 32 c/L. Thus the full 20.6 retrospective effective rate must not be exposed to a 30 March forecast. Split the announced components by known date or mark historical availability unresolved. An effective-date table built today is insufficient to prove historical availability.

Excise is embedded in terminal gate prices. Adding the whole tax change again on top of a TGP-driven forecast double counts it. A tax feature must model the part not already incorporated into the wholesale feature and retail inventory turnover. The retail-minus-TGP gap includes operating and distribution costs and is not retailer net profit.

## Inputs worth testing, in order

| Input | Mechanism and proposed feature | Limits |
| --- | --- | --- |
| Brisbane station price distribution and matched station changes | Share already substantially above the median, upper/lower quantiles, share hiking, cross-sectional spread, brand and nearby-station lagged changes. A partly completed restoration may predict subsequent diffusion. | A median can jump when just enough stations cross it, without an equivalent jump in the mean. Test both outcomes, stable station composition, stale-report expiry and plausible sample coverage. A feature computed at the target date would leak. |
| Brisbane AIP terminal gate ULP | Tax-inclusive wholesale cost and retail/wholesale disequilibrium; 1/7/14-day changes and train-estimated distributed lags. | [AIP provides historical spreadsheets](https://aip.com.au/resources/historical-ulp-and-diesel-tgp-data). Publication and revisions must be handled. Historical files downloaded today are not recorded vintages. |
| AUD/USD and overseas refined petrol | Convert upstream prices to AUD/litre before comparing; split positive/negative changes only as a prespecified candidate. | RBA quotes foreign currency per AUD and [publishes on Australian business days excluding NSW holidays](https://www.rba.gov.au/statistics/frequency/exchange-rates.html). Use the rate known by the forecast cutoff, never a later daily close. |
| Brent and EIA US gasoline proxies | Crude component, refined-product premium and its changes; ask whether they improve a model already containing retail dynamics and TGP. | [EIA Brent history](https://www.eia.gov/dnav/pet/hist/RBRTED.htm) is long and daily on trading days. US gasoline is a different physical market, specification and seasonality; neither is Mogas 95. US observation dates can precede Brisbane availability. |
| Freight/refinery/stock indicators | Shipping costs and local refinery disruptions could explain divergence of petrol from crude; monthly stocks may describe sustained supply stress. | Short and irregular live ship coverage, inferred ETAs, long publication lags and reverse causation make these difficult forecasting inputs. Preserve as dated exploratory context until matched history exists. Do not turn an unobserved stock estimate into a measured QLD stock variable. |
| Diesel, other cities and non-energy assets | Diesel/other cities can proxy a common shock; unrelated commodities may proxy global demand or AUD risk. | Begin with a mechanism and matched publication timing. A large correlation scan across assets and lags is a multiple-testing exercise; an apparent winner needs a new test period. A correlated proxy is not an independent causal driver. |

The [AIP international market page](https://aip.com.au/pricing/international-market-watch/) publishes recent Mogas 95 charts but says the underlying Argus data are proprietary and licensed. This review did **not** establish an openly licensed, long daily Mogas 95 download suitable for reproduction. Do not substitute Singapore retail pump prices or US gasoline and relabel them Mogas. AIP TGP is a practical domestic signal already incorporating regional costs; a licensed Mogas history may be useful later, but no paid acquisition is required or made by this review.

Unit conversions, with `fx` measured as USD per AUD:

```text
USD/barrel -> AUD cents/litre: 100 * price_usd_bbl / (158.987294928 * fx)
USD/US gallon -> AUD cents/litre: 100 * price_usd_gal / (3.785411784 * fx)
```

Apply GST only if needed for the comparison and account for the tax status of each source. A US gasoline-minus-Brent spread in common units is a **US refining proxy**, not an Asian crack spread. Algebraic currency conversion also creates mechanical relationships; a correlation after conversion is not new causal evidence.

## Hypotheses and falsification tests

1. **Retail diffusion:** lagged distribution and station-change features improve h1/h7 over price-history-only models. Test adding that feature group to the same estimator, and compare median versus mean and matched-station targets. If improvement disappears for the mean, investigate a median-crossing effect.
2. **Regime dependence:** a trailing/adaptive relation outperforms one fitted across 2025 cycles and the 2026 disruption together. Declare regimes from information available at the time; compare fixed rolling and expanding fits before considering a complex switching model.
3. **Upstream incremental information:** known upstream changes in AUD improve h7/h14/h28 beyond retail dynamics and TGP. Compare retail-only, retail+TGP, retail+upstream and retail+TGP+upstream on identical origins. Upstream predicting TGP does not guarantee it predicts retail after conditioning on the latest TGP.
4. **Asymmetric pass-through:** positive/negative cost changes and large disequilibria improve forecast losses beyond a symmetric model. Reject the added complexity if it cannot improve untouched outcomes. Do not infer asymmetry from upward retail cycle jumps alone.
5. **Known tax events:** announcement-aware tax inputs improve forecasts around effective dates. Score event and non-event windows separately; compare against an explicit tax-adjusted baseline. Report the small number of independent policy events.
6. **Decision value:** warning of a restoration reduces realised total fuel spending or regret even where city-price MAE gains are modest. Simulate an executable policy without future station information or assumed unlimited flexibility.

## What would support causation

A plausible economic graph is global oil supply/demand -> regional refined prices and freight -> AUD import cost -> TGP -> retail, alongside excise, local competition, demand and retail cycle decisions. AUD and crude can respond to shared macroeconomic news; TGP can be a mediator of upstream effects. The graph guides controls rather than proving coefficients causal.

Three evidence levels must be labelled separately:

- **Association:** lagged changes covary. Use differences/returns and inspect stability; correlated price levels and common trends can manufacture a strong-looking result.
- **Incremental predictive value:** a prespecified input lowers later forecast losses conditional on other known inputs. Granger-style tests concern additional prediction under a model, not structural causation.
- **Causal estimate:** an intervention or defensible quasi-experiment identifies a counterfactual response. Possible designs include event studies of announced tax changes, comparisons with unaffected products or jurisdictions where valid, and externally identified supply shocks with a credible instrument. Check anticipation, coincident shocks, spillovers, parallel trends and weak identification. Australian excise changes occur nationally, so another Australian petrol city alone is not an untreated tax control.

A single geopolitical episode is not a valid instrument merely because it precedes a price rise: it can alter FX, demand, freight and policy simultaneously. Likewise, neighbouring stations reacting together do not identify which caused the other without stronger timing or exogenous variation. The present repo can support descriptive and predictive evidence; a causal claim needs an explicit design and stronger assumptions than the forecast comparison.

## Evaluation requirements derived from the review

Freeze target, forecast cutoff, candidate list and selection rule before opening a later test. Existing April–August 2026 comparisons in the repo have already informed development and cannot serve as untouched evidence again. Post-28-September data are later evaluation data for this investigation, but recomputed forecasts today are still retrospective; only previously timestamped predictions establish genuine prospective performance.

For each horizon, fit using labels that have matured by the origin, purge overlapping validation labels, fit preprocessing only on training data and never interpolate missing targets. Split by time across all stations together; random station-row splitting leaks common market days. Reconstruct historical reported prices as their own provenance, distinct from prices actually captured by the collector.

Report MAE, RMSE, directional accuracy with an explicit flat-price policy, jump-event precision/recall or Brier score, and prediction-interval coverage **with width**. Compare each model to persistence and a simple trend. Preserve horizon-specific results rather than averaging away long-horizon failures. Dependence-aware blocks must be at least comparable to the longest overlapping target window, and the number of independent cycles/events should accompany nominal row counts. A handful of October origins cannot establish high-confidence 28-day skill or tail-risk calibration.

If no candidate passes, the research result is a measured failure to improve persistence under this design. That is useful evidence about where the predictive information is missing, not proof that fuel prices are unaffected by upstream markets or that no better model exists.
