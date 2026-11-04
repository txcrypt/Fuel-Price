# Prospective shadow trial infrastructure

Implemented 4 November 2026, Australia/Brisbane. This is an isolated research ledger. It does not install a scheduler, run a service, refresh a provider, alter application forecasts or send notifications. Code alone does not establish a completed prospective trial.

The first real attempt, at **2026-11-04 15:38:45.224444 AEST** (05:38:45.224444 UTC), withheld all forecasts. Yesterday's completed origin was **3 November**; the latest completed collected day was **23 October**. The current partial 4 November snapshot cannot be substituted for a completed daily close. The retained exclusion is in `.local/research-shadow-trial/exclusions/`, with its content-addressed frozen inputs under `inputs/`.

## Run manually

From the repository root:

```powershell
.\.venv\Scripts\python.exe research/shadow_trial.py issue
.\.venv\Scripts\python.exe research/shadow_trial.py score
.\.venv\Scripts\python.exe -m unittest tests.test_research_shadow -v
```

By default, the ledger is `.local/research-shadow-trial`. `--trial <directory>` selects another ledger. Do not create a fresh directory to replace an unfavourable prediction: retain the originally chosen trial directory throughout the evaluation.

Issuance uses the actual timezone-aware system clock; the CLI offers no historical issuance override. A library clock argument exists for deterministic tests. Exact UTC and Australia/Brisbane issuance are recorded. This is an auditable local record, not an externally notarised timestamp or a tamper-proof storage system.

## What is issued and withheld

The default candidates are persistence and a fixed damped seven-day trend, at **1, 7, 14 and 28 calendar days** after yesterday's completed Brisbane day. Thus a morning forecast's h1 target is the current day, whose daily outcome has not completed. There is no synthetic extension of the target series to make a retrospective evaluator produce a forward prediction.

The fixed initial eligibility policy requires:

- Yesterday exists in actual collected data.
- At least 200 Brisbane stations have a valid 80–350 c/L quote that day.
- The median timestamp of stations' final daily observations is at least 23:00 Brisbane time. One late station cannot establish coverage of the whole market close.
- The same checks hold for the full seven-calendar-day input window.

These conservative capture and sample thresholds are operational research choices, not an empirically established optimal policy. Freeze them before a future trial; changing them creates a changed trial protocol. A final collection at 23:30 is still an observed closing proxy, not proof every station's midnight price was captured. Days with early or missing final captures are unscorable under this policy.

Missing or ineligible observations create an append-only exclusion record with the exact attempted issuance time, requested origin, reasons, code identity and frozen input hash. They do not occupy the successful daily forecast key, so a later attempt can succeed once genuine data are available. They never fabricate a price path or relabel monthly reported events as local collection.

## Frozen evidence and first-forecast retention

`research/shadow_trial.py` reads `snapshots` and `context_vintages` through a read-only SQLite transaction and freezes the collection CSV bytes in memory before parsing. It never opens the production database for writing. Station rows are filtered to the configured Brisbane bounds. Invalid prices are excluded and the last valid selected row per station/day determines the median. For equal collection timestamps, database rows take deterministic precedence over collection CSV rows.

Every attempt retains a compressed canonical JSON input bundle with:

- Actual collected station rows, their collection timestamps and source labels.
- Context vintage rows whose recorded `known_at` is no later than issuance. Naive collector timestamps are explicitly interpreted as Brisbane time; explicit offsets are respected.
- Derived completed daily features, configured bounds, source hashes and eligibility policy.
- Optional historical-training-corpus hash and availability manifest.

The uncompressed canonical bundle's SHA-256 identifies it; the saved compressed file is checked after writing. Code files, callback identity, Python/numpy/pandas versions and per-model state hashes accompany forecasts. The forecast input hash and optional training hash bind model outputs to frozen data.

Successful predictions are retained as **one file per origin date**, containing the complete candidate/horizon batch. A second issuance for that day returns the first record before rerunning a model. No revision or later-added winner replaces it. Complete files are published using an atomic operation that refuses an existing destination (Windows no-replace rename; POSIX no-replace hard link). Scored outcomes are separate files and cannot alter forecast records.

The first actual **issued** batch also registers immutable `trial_definition.json`: callback identity, code and environment identity, eligibility policy, geographic bounds and horizons. Withheld attempts do not lock the definition. A later incompatible issuance is rejected and requires a new trial directory while retaining the old trial's results; switching algorithms inside one ledger is prohibited. Per-issuance fitted coefficients and state may legitimately change and are not compared as fixed algorithm identity. Forecasts and scores carry both the definition and code hashes. The selector receives prior losses only when **both** hashes match its trial, excluding unversioned, mismatched and future scores even if their model names are identical.

## Optional frozen historical training corpus

Official history already acquired before issuance may legitimately train a forecast made now. Requiring 120 newly collected days merely to use historical training evidence is unnecessary. The optional corpus is kept separate from current observations:

```powershell
.\.venv\Scripts\python.exe research/shadow_trial.py issue --training-corpus .local/research-2026-11-04/daily_reported.csv
```

The CSV requires unique `date` values, `price_cpl` or `median`, and explicit `provenance` (`reported` or `collected`). Raw bytes are retained under `training/<sha256>.csv`, with an issuance-time availability record. Future dates are excluded from model inputs. Historical rows can fill training dates, while actual local collection always wins on overlapping dates. The historical corpus **cannot fill yesterday or the seven-day current eligibility window**: those gates are checked against collection alone, before merging.

This does not imply that this app possessed a monthly archive at each historical event time. It records that the archive was held before the new forecast. A model using historical training values still needs a defensible within-training feature chronology and publication/revision treatment. The logger does not certify arbitrary callback methodology.

## Optional challenger protocol

```powershell
.\.venv\Scripts\python.exe research/shadow_trial.py issue --challenger research.mechanism_models:predict_candidates --training-corpus .local/research-2026-11-04/daily_reported.csv
```

The local callback interface is:

```python
predict_candidates(frame, upstream, origin, horizon, prior_losses=None)
# -> {model_name: {"prediction": float, "state": JSON_serializable_dict}}
```

`frame` contains dates through the origin only. Columns include `median`, `mean`, `q10/q25/q75/q90`, `hi20/lo20`, `share_rising/share_falling`, `station_count`, `provenance` and `input_role`. The current shares follow the existing daily feature convention: matched stations changing by at least 5 c/L. Historical training rows retain their own provenance and `input_role=historical_training`; actual collected rows have `input_role=live_collected`.

`upstream` contains actual archived `source, observed_date, value, known_at` vintage rows. The callback must select the revision available at each feature date, not join the newest revision onto every past training row. No downloaded current-history file is silently substituted. A callback should omit an ineligible challenger and explain the missing-input reason in the state of its retained selector/baseline output. Empty or short vintage coverage can therefore leave only simpler candidates eligible.

`prior_losses` contains separately saved outcome scores whose scoring timestamps precede issuance. A selector can use those matured losses, without examining unscored future targets. It must declare its update rule and make the selected model inspectable. Tuning on losses then advertising those same losses as untouched evidence remains invalid.

## Separate outcome scoring

`score` waits until each target Brisbane day has ended, then reads actual collection only and applies that forecast's frozen coverage policy. Missing days, early closes, low station counts and monthly backfill do not become valid collected outcomes. The first eligible outcome is retained with its own frozen raw input hash, actual median, final capture time, absolute error and exact scoring timestamps.

Running the scorer again preserves first scores. If later ingestion changes an old daily close, the original score remains; a correction requires a distinct, explicit audit rather than replacement. No confidence interval or claimed future coverage is issued: prospective calibration evidence does not exist yet.

Fourteen focused tests pass, covering exact time/origin handling, all four horizons, frozen input integrity, first-forecast retention after revised inputs, incomplete/stale origin exclusion, late-station coverage, future-vintage exclusion, historical-training separation, read-only database access, outcome maturity and score retention. Additional tests reject changed callbacks, policies and code without replacing the trial definition, allow legitimately changing fitted state, exclude losses from missing/wrong code or trial versions, and verify that scoring uses the registered geographic bounds even if the app's current configuration changes. An integration test runs the real mechanism callback on sparse inputs and verifies that it retains baseline/selector predictions while explicitly recording unavailable challengers. The production attempt confirmed the expected abstention rather than issuing a forecast against stale data.
