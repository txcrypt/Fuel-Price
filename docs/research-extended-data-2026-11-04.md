# Extended historical data — 4 November 2026

The isolated research archive now covers **1 January 2023–31 October 2026**:
1,400 consecutive daily reconstructions, 311,960 Brisbane U91 report events and
515 distinct stations. This adds two years for blocked-year cycle and mechanism
tests. It does not add new October outcomes or make October a fresh confirmation
after it has already been inspected in the first research pass.

No operational database or existing primary research export was modified. The
combined datasets live under `.local/research-2026-11-04/extended/`, which is ignored
by git. Original monthly CSVs, filtered monthly records, contradictory reports,
source metadata and checksums are retained there.

## Acquisition and parser verification

Sources are the official [2023 Queensland fuel-price archive](https://www.data.qld.gov.au/dataset/fuel-price-reporting)
and [2024 archive](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2024).
Their 24 monthly CSVs total **172,338,603 bytes** (about 164 MiB), containing
1,224,490 raw rows across fuel grades and locations. Filtering retained 161,652
additional Brisbane U91 events. Attribution: © State of Queensland, CC BY 4.0.

Before bulk acquisition, bounded 16 KiB HTTP range samples of January 2023 and
January 2024 were inspected and retained as `raw_sample_2023.txt` and
`raw_sample_2024.txt`. The samples established:

- The fuel label is `Unleaded`; fields identify Queensland, coordinates and UTC
  report time. Raw prices are tenths of one cent: 1759 becomes 175.9 c/L.
- January 2023 has an extra `_id` field and ISO timestamps such as
  `2023-01-06T23:45:00`.
- January 2024 has day-first timestamps such as `11/01/2024 05:28`.

The repository's existing strict day-first parser would silently discard ISO
timestamp rows. `research/extend_history.py` therefore uses an isolated exact-format
adapter while calling the existing `read_events` filter. It explicitly recognises
ISO and day-first formats, assumes the publisher's UTC convention, and **raises
on any unrecognised nonempty timestamp** rather than quietly dropping it. The
adapter is restored after acquisition and changes no production module on disk.

The existing geographic bounds, `Unleaded` label, raw `9999` unavailable sentinel,
price divisor 10 and accepted 80–350 c/L range remain constant across years.
Invalid/out-of-range prices are subject to the existing import filter. Source row
counts and checksums are recorded per file in `acquisition_manifest.json`.

## Reconstruction and outputs

The original 2025–2026 frozen `reported_events.csv` is combined with the new filtered
events. All source records are retained in `reported_events_with_sources.csv`.
Exact event identity is station, fuel type, report timestamp and price. There were
no duplicate identities across the combined inputs. There are 120 station/report
timestamp groups with contradictory prices; they remain inspectable in
`ambiguous_report_timestamps.csv`.

The primary panel uses the same deterministic maximum-price tie convention and
14-day expiry as the first research pass. A tie convention supplies reproducible
ordering, not evidence that the selected price is correct. Daily last reports are
assigned using Brisbane time; only prior reports are carried, and unavailable
sentinels are masked after carrying. There is no backward filling or interpolation.

| Year | Days | Median stations/day | Minimum daily median | Maximum daily median |
| --- | ---: | ---: | ---: | ---: |
| 2023 | 365 | 479 | 165.7 c/L | 237.9 c/L |
| 2024 | 366 | 480 | 163.9 c/L | 235.9 c/L |
| 2025 | 365 | 480 | 164.9 c/L | 219.9 c/L |
| 2026 through October | 304 | 493 | 151.9 c/L | 259.9 c/L |

The full panel has a median 482 stations/day, minimum 278, and 66.62% of included
station-days are carried reports. Early-January cold-start coverage remains a
limitation at the beginning of 2023; do not treat the first days as a fully observed
market. The alternative expiry-policy concerns in the first data audit still apply.

Combining December 2024 reports legitimately changes four daily medians relative
to the shorter archive, which began with no prior reports on 1 January 2025:
1 January −2 c/L, 2 January −2, 7 January −0.3, and 8 January −1. All later daily
medians, including every 2026 evaluation target, are unchanged. The original
primary files remain intact.

The modelling inputs are:

- `extended/reported_panel.csv`: date × station, identical panel contract to the
  original export.
- `extended/daily_reported.csv`: daily median, mean, quantiles, high/low-price shares,
  matched-station rise/fall shares, station count and carried-report count; the
  same feature column names used in the first pass.
- `extended/reported_events.csv`: all unique events and retained source IDs.
- `extended/extended_summary.json`: coverage, overlap changes, checksums and
  reconstruction-check dates.

Frozen hashes:

```text
daily_reported.csv  2babbe0562867244fc97b8ed7986dbc7935f6288f044b541c26e821e649f130a
reported_panel.csv  02a8a4d69279d14d211b9ee940c6e74ce40261face7950181d67fb9e6250f1a8
reported_events.csv cac6b46583e70fc9e6f258d8a9f4fc25e631244a6e9f5274792d35d6cc4d1929
```

Reconstruction from only reports available through 15 June 2024, 1 January 2025
and 28 September 2026 exactly matched the corresponding full-panel prefixes.
Dedicated tests cover ISO/day-first equivalence, Brisbane day conversion and
rejection of unrecognised timestamp formats.

## Reproduction and interpretation

```powershell
# Public network acquisition, resumable using preserved raw-file checksums:
.venv/Scripts/python.exe research/extend_history.py

# Rebuild from the retained filtered monthly data; no network access:
.venv/Scripts/python.exe research/extend_history.py --prepare-only

# Timestamp regression checks:
.venv/Scripts/python.exe -m unittest discover -s tests -p test_research_history.py -v
```

The script never opens the operational database. It reuses the saved official
package metadata, handles the special 2023 dataset slug explicitly, and retains
the original CSV bytes. Wider historical inputs can reveal whether a mechanism
survives different years and regimes. They cannot demonstrate causal identification
or a deployable accuracy level by themselves. The files are retrospective public
report reconstructions, not locally collected historical snapshots or archived
publisher vintages.
