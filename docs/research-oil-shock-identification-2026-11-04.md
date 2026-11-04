# Can externally identified oil news establish Brisbane pass-through?

4 November 2026. **The exercise does not identify a sufficiently precise or
credible Brisbane causal coefficient.** It goes beyond ordinary price
correlations by testing a published external-instrument candidate, and reports
the limitations rather than interpreting a noisy point estimate as an effect.

## Source and estimand

[Känzig (2021), American Economic Review](https://www.aeaweb.org/articles?id=10.1257/aer.20190964)
uses oil-supply news around OPEC announcements for identification. Its
[public appendix](https://www.aeaweb.org/articles/materials/14302) discusses
instrumental-variable local projections and their limited power. The main
published article was access restricted; the abstract, public appendix and
author data documentation were accessible. This is a small Brisbane extension,
not a replication of the full original VAR or its identifying assumptions.

The [author's data repository](https://github.com/dkaenzig/oilsupplynews) supplies
the `2025M12` vintage, including a daily PC surprise series. We downloaded that
published file and recorded its SHA-256. The daily series overlaps the extended
Brisbane history at **19 announcements, 4 June 2023–1 December 2025**. It has no
2026 announcement observations. These are 19 episodes, not 1,400 independent
instruments. Updated historical factors can also use later information.

The candidate estimand is the retail response, in c/L, per 1% announcement-window
Brent spot-price movement instrumented by the author's daily PC. PC units are
left as the file's factor units; they are not relabelled as a percentage oil-supply
change. We do not assume that all news effects operate only through Brent.

The shock data are released retrospectively with substantial delay. They are
**never included as a real-time forecasting input**. The economic-identification
exercise and the predictive-model comparison answer different questions.

## Event construction and inference

For each announcement, take the last EIA Brent quote strictly before its date
and the first quote on or after it, within five calendar days. Treatment is
100 times their log ratio. Map that first US trading-date close to the following
Brisbane day, with a further one-day alignment sensitivity. Weekend events thus
use the next genuine trading quote; missing quotes are not fabricated.

The retail baseline is the Brisbane close on the day before the announcement.
Outcomes are the reconstructed retail close at Brisbane anchor +1/+7/+14/+28,
minus that baseline. These are **event-response horizons**, not the exact daily
forecast horizons in the main model report. A mean-price sensitivity replaces
the median everywhere, including its historical control.

Controls are an intercept, pre-announcement seven-day retail change and
pre-announcement seven-day Brent change. A placebo uses the retail change from
14 to seven days before the baseline; it is not the same change already included
as a control. Controls do not include contemporaneous TGP or FX, which could
mediate news effects. This leaves potential omitted confounders and does not
establish an exclusion restriction.

Errors use HC3-adjusted Bartlett calendar HAC with bandwidth max(14,horizon)
days. Calendar distance matters because some announcements are only three days
apart. Instrumental-variable point ratios are retained for audit, but conclusions
use an approximate Anderson–Rubin-style inversion of the instrument-coefficient
test. The inversion can produce bounded, disjoint or unbounded sets; it does not
force a misleading finite Wald interval when the instrument is weak. Student
critical values and sparse-event HAC remain approximations, not exact small-sample
coverage guarantees. Multiple horizons and sensitivities are exploratory.

## Results

With the primary median target, all 19 events and the first alignment, approximate
95% sets for c/L response per 1% instrumented Brent movement are:

| Event-response horizon | Approximate set | Interpretation |
| --- | --- | --- |
| 1 | [−4.63, +3.46] | Too wide to determine a response |
| 7 | [−20.18, +15.87] | Very low precision relative to any useful forecast |
| 14 | [−25.99, +14.10] | Very low precision |
| 28 | [−5.45, +11.28] | Includes zero and economically large responses |

The seven-day point ratio is −0.315 c/L per 1% Brent, but the uncertainty makes
its sign uninformative. It is not evidence that higher crude costs lower pump
prices, or that crude costs do not matter. The median's cycle movements dominate
many of these few event windows. Replacing the median with the mean does not
resolve the identification problem; its seven-day set is [−20.88, +10.32].

## One large, potentially confounded episode drives relevance

The 3 April 2025 surprise contributes **47.7% of centred PC variation** in the
19-event sample. The [OPEC announcement](https://opec.org/pr-detail/557-03-april-2025.html)
confirmed a larger production increase that day. At almost the same time,
[DFAT dates the US reciprocal-tariff announcement to 2 April EDT / 3 April AEST](https://www.dfat.gov.au/news/news/us-tariff-announcements).
The [IEA April oil-market report](https://www.iea.org/reports/oil-market-report-april-2025)
describes both tariff news and the OPEC decision as important oil-market changes.
This gives a concrete reason to question a daily announcement-window exclusion
restriction in this particular event.

Dropping that episode reduces the median specification's calendar-HAC first-stage
F-style diagnostic from **19.42 to 6.00** at the shorter horizons. These numbers
are descriptive relevance diagnostics, not a universal strong/weak threshold or
proof of validity. The median h7 set expands to [−50.33, +16.84]. Under the mean
target, the first-stage diagnostic falls from 13.43 to 4.32 and the h7 set becomes
**(−infinity, 5.76] union [189.65, +infinity)**. Reporting only those two finite
endpoints as a conventional interval would be incorrect.

The alignment sensitivities and pre-event placebo are also imprecise. A placebo
set containing zero does not prove absence of anticipation or validate the
instrument. Dropping the April episode does not establish that every remaining
event is free of coincident news.

## What would improve this

More Brisbane history overlapping more announcement episodes would help power,
but not automatically solve exogeneity. Higher-frequency, well-dated treatment
windows, a documented check of coincident announcements, stronger pre-event
controls without overfitting the event count, and explicit treatment of FX/refining
channels are needed before claiming structural pass-through. Any future shock
instrument remains a retrospective research tool unless its genuine release and
revision timestamps support real-time use.

Forecast gains from station diffusion can still be useful without a causal
coefficient. Conversely, a credible economic mechanism does not guarantee useful
tomorrow/week-ahead predictive information after the current price is known.

## Reproduce

```powershell
.\.venv\Scripts\python.exe research/oil_shock_analysis.py --acquire
.\.venv\Scripts\python.exe research/oil_shock_analysis.py --daily .local/research-2026-11-04/extended/daily_reported.csv --target median
.\.venv\Scripts\python.exe research/oil_shock_analysis.py --daily .local/research-2026-11-04/extended/daily_reported.csv --target mean
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_research_oil_shocks.py -v
```

Event rows, all estimates and manifests are retained under
`.local/research-2026-11-04/oil-shocks/`. The three tests cover unbounded confidence
sets, an irrelevant instrument, and weekend/Brisbane alignment without using a
later oil quote. Source data attribution: Diego R. Känzig, CC BY 4.0 per author
repository; EIA public data; Queensland fuel reports © State of Queensland,
CC BY 4.0. The acquisition manifest retains the exact author-file content hash.
