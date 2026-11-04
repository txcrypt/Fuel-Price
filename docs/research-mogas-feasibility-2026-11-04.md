# WATC Mogas95 feasibility — 4 November 2026

**A public PDF quote source is verified. A complete, freely reusable historical
daily feed is not.** WATC reports contain an explicitly identified Singapore
Mogas95 benchmark, and a bounded extractor can recover its numeric quote reliably
when the report supplies the required identifying fields. The archive is sparse,
the daily quote clock and vendor instrument remain unverified, and public PDF
access does not establish a redistribution licence. Nothing from this investigation
enters a production forecast or the current model comparisons.

This updates the earlier repository statement that no free daily Mogas source had
been verified: **public daily PDF quotations now are verified**, but an API, full
historical coverage and rights to redistribute a daily data product remain
unverified. The EIA Gulf Coast series remains a different regional benchmark.

## What was checked

The [WATC daily-updates page](https://www.watc.wa.gov.au/economic-insights/market-updates/daily-updates/)
describes reports as discussing the previous trading day's developments. The
4 November HTML contained six latest daily PDF links, through 28 October. Its
load-more JavaScript only reveals existing DOM items; it did not expose a
historical-data endpoint. The public sitemap lists publication landing pages,
without an inventory of historical media PDFs. A search through WATC's public
search form returned no result for `Mogas95`; searching for old daily reports
returned general site pages, not a usable historical PDF index.

Older publisher PDFs remain directly accessible when their observed URLs are
known. Their randomly assigned media directory names prevent reliable URL
construction from dates alone. Public indexed search found examples; a bounded
Internet Archive CDX query supplied a more systematic discovery mechanism.

The first CDX path-glob request returned an empty result. An explicit media-prefix
query with an original-URL regular-expression filter worked:

```text
https://web.archive.org/cdx/search/cdx
url=www.watc.wa.gov.au/media/
matchType=prefix
output=json
filter=statuscode:200
filter=original:.*daily-report[.]pdf$
collapse=original
from=2023
to=2026
limit=500
```

It returned **148 distinct observed publisher PDF URLs**, below the 500 limit.
Only metadata was requested from Internet Archive; sampled PDFs were fetched from
their original public WATC URLs. No archive content replay, login, hidden endpoint
or access restriction was bypassed.

| Report year | Discovered URLs/dates | First report date | Last report date | Largest calendar gap | Weekdays without a discovered report between endpoints |
| --- | ---: | --- | --- | ---: | ---: |
| 2023 | 73 | 21 February | 26 October | 95 days | 105 |
| 2024 | 37 | 1 February | 15 October | 207 days | 147 |
| 2025 | 20 | 7 March | 25 November | 140 days | 168 |
| 2026 | 18 | 30 April | 9 July | 28 days | 33 |

These are **archive-discovery gaps**, not proof that WATC failed to publish on
those dates. The weekday counts do not remove public holidays. CDX capture dates
range from 15 March 2023 to 13 July 2026; capture date and document report date are
distinct. No complete coverage claim is justified. The raw response, exact request,
date-parsed URL CSV and coverage summary are retained in the ignored research cache.

## Bounded extraction results

Six report dates evenly spaced across each recovered year's dates were selected,
plus six already inspected daily reports: **30 daily PDFs in total**. A separate
weekly PDF supplied contextual unit evidence. This was a bounded consistency test,
not bulk archive acquisition. All 30 observed publisher URLs were accessible.

The strict extractor accepted 24 reports: seven from 2023, eight from 2024, eight
from 2025 and one from 2026. Each accepted report had:

- A document report date agreeing with its six- or eight-digit filename.
- An explicit US-dollar commodity-table heading.
- A footnote identifying Mogas95 as the Singapore petrol benchmark.
- Exactly one numeric Mogas95 price row and a parseable displayed change.

The six sampled April–July 2026 reports contained the numeric row and the Singapore
footnote but omitted the currency from the commodity-table heading. They were
**rejected for missing direct currency verification**, rather than silently assigned
USD. Their original PDFs and individual rejection reasons are retained. Visual
inspection of 30 April confirmed that the heading actually omits the currency;
this is not just a text-extraction omission. Thus numeric-row availability was
30/30 in this sample, but the stricter accepted extraction rate was 24/30.

PDF layouts from 2023, early 2024, 2025 and 2026 were inspected visually, alongside
the weekly unit chart. The numeric quotes recovered in the initial samples included
95.97 in the [17 March 2023 report](https://www.watc.wa.gov.au/media/054jckkw/170323_daily-report.pdf),
91.39 in the [2 January 2024 report](https://www.watc.wa.gov.au/media/pztgdsmc/020124_daily-report.pdf),
79.36 in the [4 April 2025 report](https://www.watc.wa.gov.au/media/zupdzxut/04042025_daily-report.pdf),
and 74.27 in the [10 April 2025 report](https://www.watc.wa.gov.au/media/k0yhao2g/10042025_daily-report.pdf).
Those values are source quotations, not Australian pump prices or inferred missing
observations.

## Units, vendor and time: verified versus unresolved

**Currency:** the accepted daily tables explicitly state USD. The six omitted
headers demonstrate why a dollar sign alone should not determine the currency.

**Quantity denominator:** the daily tables do not explicitly print per barrel.
However, page 2 of the [13 March 2026 WATC weekly report](https://www.watc.wa.gov.au/media/k3nofq1d/market-watch-weekly-13032026.pdf)
contains a global-oil-price chart with a Singapore Mogas benchmark series and an
explicit USD-per-barrel axis. The chart credits Bloomberg. This supplies strong
within-publisher context for a USD/barrel interpretation, while falling short of
an explicit instrument specification for every historical daily-table quote. The
sample CSV preserves both `daily_table_denominator=not_stated` and a contextual
unit field with that evidence; it performs no AUD-per-litre conversion.

**Vendor and instrument:** the weekly chart's Bloomberg attribution is visible,
but the sampled daily quote tables do not identify a vendor ticker, exact spot
assessment, bid/offer/mid convention, close versus live observation, delivery
specification or roll treatment. It would overstate the evidence to relabel every
daily quote as a particular Argus/Platts series or assert that the daily table uses
the exact instrument plotted by the weekly chart. These fields remain blank.

**Quote date and publication clock:** report dates are explicit. PDF metadata in
the initial examples records creation around 08:17–08:55 AWST, but creation is not
proof of the time the file became publicly available. The introduction's
previous-trading-day description is not a timestamp for the individual Mogas row.
The source quote observation date/time and historical publication timestamp
therefore remain blank. The actual extraction/download recording time is preserved
separately. Every sample row is marked `verified_point_in_time_ready=False`.

**Displayed change:** it must not be treated automatically as a one-day return.
The 2 January 2024 PDF explicitly identifies the comparison as the previous report
on 15 December 2023. Holidays and publication breaks can lengthen the comparison
window. The extractor retains this note and the displayed change independently.
It never back-calculates missing historical quotes from those changes.

**Vintages:** an old CDX timestamp proves that a URL was observed in the archive,
not that the currently downloaded PDF is byte-identical to that historical capture.
The present publisher PDF SHA-256 is retained. No verified historical content-vintage
comparison was performed. A current PDF creation date or archive URL alone cannot
establish a leakage-safe historical `known_at` timestamp.

## Access and reuse scope

The [WATC terms of use](https://www.watc.wa.gov.au/terms-of-use/) reserve copyright
and distinguish permitted private-study/research fair dealing from broader reuse
requiring permission. They do not provide an open-data licence or establish rights
to redistribute an extracted daily feed. The underlying vendor instrument may also
carry rights that WATC's public PDF access does not resolve. This investigation
retains its small raw sample and extracted quotes only inside the ignored local
research cache; it does not publish a redistributed price dataset or install an
ongoing scraper.

The publisher's robots file permits ordinary public paths while disallowing certain
application/configuration directories. Those directories were not accessed.
Robots access is separate from licensing. No contact or permission request was sent.

## Artifacts and reusable extraction design

All raw quotes remain in `.local/research-2026-11-04/mogas/`:

| Artifact | Contents |
| --- | --- |
| `mogas_sample.csv` | 24 accepted observed quotes; no filled dates, conversions or fabricated quote timestamps |
| `mogas_sample.audit.json` | Every requested source URL, PDF SHA-256, metadata, result and rejection reason |
| `sample_reports.json` | Explicit bounded sample list and discovery provenance |
| `cdx_discovered_report_urls.csv` | 148 discovered original publisher URLs and archive metadata |
| `cdx_coverage_summary.json` | Coverage by report year and discovery gaps |
| Original PDFs and local PNGs | Source evidence and visual checks, retained privately |

Tracked code is `research/watc_mogas.py`; `pypdf` is an optional research-only
dependency. The extractor requires an explicit manifest of at most 40 official
publisher media URLs. Downloads require an explicit flag, existing local PDFs are
reused, and a pinned checksum can reject changed content. It rejects missing or
contradictory dates, missing USD identification, missing Singapore identification,
duplicate numeric rows and implausible values. Source failures remain failures in
the audit. Repeated report dates require explicit revision review.

```powershell
# Re-extract the already cached bounded sample without network access:
.venv/Scripts/python.exe research/watc_mogas.py --manifest .local/research-2026-11-04/mogas/sample_reports.json --output .local/research-2026-11-04/mogas/mogas_sample.csv

# Add --download only for an explicitly reviewed bounded manifest.
.venv/Scripts/python.exe -m unittest discover -s tests -p test_research_mogas.py -v
```

Five regression tests pass: holiday change-window preservation, date disagreement
rejection, missing-row rejection, multiple-row rejection, and currency/benchmark
identity checks. Python compilation also passes. These checks establish extractor
behaviour; they do not resolve missing market metadata or licensing.

## Feasibility conclusion

WATC is a credible **public-report lead for the relevant regional benchmark**, with
technically workable private sample extraction. It is not yet a verified complete
historical daily dataset, documented API, content-vintage archive or licensed feed.
Sparse discovery and unresolved quote metadata prevent a meaningful replacement
of the current upstream series in a daily forecasting experiment.

The next useful step would be obtaining a complete permitted report inventory or
structured series, the exact Mogas instrument/unit convention, its observation and
publication times, and the allowed research/reuse scope. Only then should a separate
predeclared, lagged incremental-value experiment compare this regional benchmark
with EIA gasoline and Brisbane wholesale inputs. Until those questions are resolved,
these quotes remain feasibility evidence only.
