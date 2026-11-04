# Research data audit — 4 November 2026

The current repository has considerably more historical evidence than the September
handoff describes. The operational database already contains 150,308 official
Brisbane U91 reports from 22 monthly files, including September and October 2026.
There is no need to import those files again. This audit read the database using
SQLite `mode=ro`, exported research copies, and did not alter the collector or its
database. The two pre-existing modified live CSVs were preserved.

The main target is **the daily median station price in the configured Brisbane
rectangle**, latitude −27.70 to −27.00, longitude 152.70 to 153.50. This rectangle
is not a council boundary or an official weighted Brisbane consumption market.
Station prices are equally weighted, rather than weighted by sales volume.

## Reproduce and inspect

```powershell
.venv/Scripts/python.exe research/prepare_data.py --as-of 2026-11-04
```

The default output directory is `.local/research-2026-11-04/`, which is ignored by
git. The script opens one read transaction over the database, exports its tables,
and combines a read of the live collection CSV for collected-day evidence. A
collector can append to that CSV after the database read; the export hashes
identify the actual research inputs. Re-running against a changing collector may
therefore produce a newer snapshot.

Important artifacts:

| File | Meaning |
| --- | --- |
| `reported_events.csv` | Original imported official Brisbane U91 report events, including unavailable-price sentinels |
| `import_sources.csv` | Source URLs, publication/import timestamps, checksums and row counts |
| `reported_panel.csv` | Date × station panel, retrospective as-of reports, 14-day expiry |
| `daily_reported.csv` | Uniform reconstructed daily target and cross-sectional features; primary modelling input |
| `collected_panel.csv`, `daily_collected.csv` | Actual final observations per station per collected day; collection gaps remain gaps |
| `daily_merged.csv` | Labelled app-compatible sensitivity series; collected days take precedence; not the primary uniform target |
| `context_observations.csv` | Latest stored contextual values, with first/last sighting dates |
| `context_vintages.csv` | Recorded contextual value vintages with actual `known_at` timestamps |
| `forward_predictions.csv` | Saved first daily candidate forecasts, without generating new ones |
| `forward_scored_collected.csv`, `forward_scoreboard.csv` | Saved forecasts scored only on completed, genuinely collected target days |
| `audit.json` | Coverage, tie policy, prospective counts and SHA-256 checksums |
| `expiry_sensitivity.json` | Effect of alternative report-expiry policies on the median |

Daily feature columns include `price_cpl` (median), `mean_cpl`, `stations`,
`q10`, `q25`, `q75`, `q90`, `std_cpl`, `hi20`, `lo20`,
`hiked5_share`, `cut5_share` and `matched_stations`. The rise/fall shares require
matched stations on the previous **calendar day**; a collection gap is not treated
as a one-day price movement. Features describe only data dated at or before their
row date. Today’s collected row is explicitly provisional.

Frozen primary input hashes:

```text
daily_reported.csv  8470ab2be2bad0b81186336c6421c81b81415caaa024d9bbe92b44251faff48a
reported_panel.csv  d5b1c48c96ffaa97831a90b564781888648fce1252b80657ecad0cf8ecce3dc2
```

## Historical coverage

| Evidence | Coverage |
| --- | --- |
| Official reported events | 150,308 events; 509 distinct stations; 22 monthly source files |
| Daily reconstruction | 669 calendar days, 1 January 2025–31 October 2026; no missing calendar days |
| Stations included each day | Minimum 270, median 486, maximum 502 |
| Carried-forward station-days | 65.84% of included station-days |
| Explicit unavailable reports | 1,157, stored as zero and masked after forward carrying |
| Collected daily observations | 38 dates, 3 December 2025–4 November 2026; 37 completed dates |
| Collected observations after 28 September | 12 completed dates |
| New reported observations after 28 September | 33 dates, 29 September–31 October |
| Reported/collected overlapping dates | 37; median absolute difference of medians 0 c/L; maximum 8 c/L |

The largest reported-versus-collected differences were 4 c/L on 15 December 2025,
2 c/L on 21 December 2025, and −8 c/L on 4 January 2026 (collected minus reported).
Those captures ended at 02:45, 00:00 and 10:15 respectively, whereas the reported
reconstruction includes reports through the end of each Brisbane day. Different
station coverage and the expiry rule also matter. Treat them as different measured
targets; a collection date alone does not establish a complete daily close.

The legacy `brisbane_fuel_history_clean.csv` lacks collection timestamps and is
excluded. Its rows cannot be relabelled as locally observed daily snapshots.
The database's 216,296 Western Australian snapshot rows cover only 3–12 July 2026;
they do not establish national historical coverage or Brisbane predictive skill.

## As-of construction and data risks

The official files are retrospective **change-event logs**, not daily snapshots.
An event is assigned to its Brisbane day using its UTC timestamp. The final report
per station per day is carried forward, for at most 14 days, and an unavailable
report replaces the prior price. No backward fill or interpolation is used.

Source publication and retailer report timestamps answer different questions.
For example, the September and October monthly files have recorded publication
metadata on 3 November and import timestamps on 4 November. A retrospective
backtest can simulate public report information at its report timestamp, assuming
the historical record and timestamps are faithful. It cannot claim the app itself
had the monthly file at that time. Historical corrections and reporting delays
cannot be ruled out by these files. This is simulated historical evidence, distinct
from an archived forward forecast.

There are **75 site/timestamp groups with conflicting prices**. A database primary
key containing price allows these groups. No actual sequence within those ties is
recoverable. The research export resolves them deterministically to the maximum
price; this is a reproducibility convention, not a claim that the higher price is
correct. Choosing the minimum instead changes the daily median on only four days:
7 January 2025 (0.3 c/L), 22 October 2025 (1.0), 11 November 2025 (0.6), and
21 July 2026 (0.1). None lies in the fresh September/October evaluation period.
The original events and conflicting rows are retained in the exports.

The **14-day expiry is a repository assumption**, not an observed station closure.
A station whose unchanged price lasts longer may disappear from the reconstructed
median. Price-cycle timing can consequently depend on that assumption:

| Expiry | Median station count | Days differing by >1 c/L from 14 days | Mean absolute median difference | Maximum difference |
| --- | ---: | ---: | ---: | ---: |
| 7 days | 448 | 74 | 0.766 c/L | 37.5 c/L |
| 14 days | 486 | 0 | 0 | 0 |
| 30 days | 496 | 32 | 0.279 c/L | 17.0 c/L |
| No expiry | 500 | 35 | 0.321 c/L | 19.0 c/L |

These are target-definition sensitivities, not forecast errors. A model should
retain its advantage on alternative expiry policies and collected targets before
production use. Full-history reconstruction matched reconstruction from only
events available through 22 October 2025, 21 July 2026 and 28 September 2026 in
explicit prefix checks. A separate sparse-calendar check confirmed that a
two-day observation gap cannot become a one-day matched-station change.

`research/sensitivity_data.py` exports alternative 7-day, 30-day and unlimited
expiry daily features from the frozen events, without modifying the primary input.
It also creates `daily_reported_fixed_cohort.csv`: 434 stations selected for at
least 90% coverage during 15 January 2025–28 September 2026, with membership then
frozen. A target is retained only where at least 90% of that cohort has a price.
There are 629 such days, including all 33 fresh dates. Cohort selection uses all
development dates, so development scores on this series are diagnostic; October
membership uses no October information. This is a fixed-membership sensitivity,
not a guarantee of exactly identical participating stations on every target date.
The cohort IDs, criteria and output hashes are in `sensitivity_data_manifest.json`.

```powershell
.venv/Scripts/python.exe research/sensitivity_data.py
```

## Contextual data actually recorded locally

| Source | Stored observations | Observation dates | Recorded vintages |
| --- | ---: | --- | ---: |
| AIP Brisbane wholesale ULP | 31 | 11 September–4 November 2026 | 27 |
| RBA AUD/USD | 14 | 15 September–3 November 2026 | 11 |
| EIA Brent | 90 | 23 June–27 October 2026 | 30 |
| EIA US Gulf Coast gasoline | 90 | 22 June–27 October 2026 | 30 |

The 98 recorded vintages begin on 28 September. Earlier observation dates inside
these tables were first fetched later; joining on observation date alone would
leak information into a purported real-time historical forecast. For a strict
local-information test, use `known_at` and the latest vintage actually available
by the forecast cutoff. These timestamps are stored without an offset by the
local app and should be interpreted using its Brisbane runtime convention.

The research lead separately acquired the publishers' longer AIP/RBA/EIA histories
in `upstream_history.csv` with `upstream_manifest.json`. Those current-history files
are not archived vintages. Conservative publication delays can support an
exploratory historical backtest, with explicit revision/availability assumptions;
they cannot remove that limitation.

## Saved forward evidence

`local_data.py::_prediction_log` writes only when `forecast.as_of` equals the
current local date. `INSERT OR IGNORE` preserves the first recorded path for each
issue date, target date and model. This is useful prospective evidence, conditional
on the integrity of the database. The log stores **dates, not exact issue times**,
and has no code version or input snapshot hash. The origin median may be an
intraday observation. It does not prove an end-of-day research protocol was used.

There are 392 saved rows: 98 each for persistence, damped trend, weekly and analog,
across issue dates 28 September–4 November. Only 43 per model have completed,
actually collected outcomes: 11 unique issue dates and 11 unique target dates.
Missing target days are not filled from the official backfill. Today is excluded.

Mean absolute errors, in c/L:

| Horizon | Forecasts per model | Persistence | Damped trend | Analog | Weekly |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 day | 10 | 0.400 | 0.400 | 0.420 | 0.560 |
| 2 days | 8 | 0.825 | 0.825 | 0.825 | 0.825 |
| 3 days | 7 | 0.943 | 0.943 | 0.943 | 0.800 |
| 4 days | 6 | 0.933 | 0.933 | 0.933 | 0.767 |
| 5 days | 5 | 0.920 | 0.920 | 0.920 | 0.720 |
| 6 days | 4 | 0.900 | 0.900 | 0.900 | 0.900 |
| 7 days | 3 | 0.667 | 0.667 | 0.667 | 0.667 |

These are sparse, dependent, quiet-market outcomes. Repeated forecasts of the
same target day are not independent evidence. The pooled MAE is 0.767 c/L for
persistence/damped trend, 0.772 for analog and 0.735 for weekly, but mixing horizons
does not establish an edge. This evidence does not validate high accuracy through
restorations, wholesale shocks, or 14/28-day horizons, which were never logged.

## Longer official history: metadata-only verification

The [2024 official archive](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2024)
lists 12 monthly CSV resources; all also advertise an active CKAN datastore.
The [2023 official archive](https://www.data.qld.gov.au/dataset/fuel-price-reporting)
also lists 12 monthly CSV resources. Its slug is `fuel-price-reporting`, **not**
`fuel-price-reporting-2023`; the current importer's generic year template would
return an HTTP error for 2023. January–March 2023 advertise no active datastore,
but CSV download URLs are listed.

Zero-record datastore schema requests for December 2023 and January 2024 showed
the required `SiteId`, `Site_State`, `Site_Latitude`, `Site_Longitude`, `Fuel_Type`,
`Price` and `TransactionDateutc` fields, compatible with the importer's column-name
normalisation. This does not verify the raw CSV timestamp strings, units or every
month's schema. Those require bounded sample inspection before bulk parsing.

The 11 stated 2023 file sizes total 73.5 MiB, plus an unspecified December file.
2024 lists 5.5 MiB for February and 6 MiB for March; most other sizes are omitted,
so an exact combined download volume cannot be claimed. Acquisition would involve
24 monthly CSVs, plausibly of order hundreds of megabytes, filtered into a much
smaller Brisbane U91 event table. No old monthly CSVs were downloaded in this audit.
The package metadata and sampled schemas are saved in the isolated research folder.

Older data can strengthen cycle estimation and regime comparisons. It does not
create more genuinely fresh post-September outcomes or solve vintage availability.
