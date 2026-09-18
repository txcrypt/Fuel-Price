# Fuel Observatory: feature audit and implementation handoff

Audited 18 September 2026, Australia/Brisbane. This is a point-in-time audit, not a promise that external APIs remain available. Read current endpoint status before starting work.

## Task for the next agent

Continue the local Brisbane unleaded 91 information console. The user wants dense, inspectable information in a dark “God’s Eye” style, accurate price evidence, and decisions that minimise total fuel spend. Prioritise collection continuity, trustworthy historical coverage, and validated forecasts before adding more speculative signals. The default vehicle is 50 L, but all personal inputs remain editable.

Start with items 1–3 below. Preserve existing data and working features. Each item describes an independently reviewable change; do not treat the entire backlog as permission to publish the service, create accounts, buy services, or send notifications.

Workspace: `C:\Users\sarke\OneDrive\Documents\GitHub\Fuel-Price`.

Local dashboard: `http://127.0.0.1:8000/`. Buying workspace: `/planner`. Python: `.venv\Scripts\python.exe`. Launch: `start-local.ps1`. The current hidden server is recorded in `.local/server.pid`; verify its command line and workspace before stopping it. Binding to `127.0.0.1` is intentional. Computer sleep and a stopped process prevent collection.

## Verified working / fixes completed

- Queensland fuel API, CSV append, SQLite snapshots and extended API evidence recovered during this audit. A manual refresh succeeded at 15:21 AEST after earlier network failures. Last good data was retained during the failures. CSV is hourly; SQLite/live snapshot collection is normally every 30 minutes.
- `brisbane_fuel_live_collection.csv` is the larger file the user identified as their Excel spreadsheet. It is already used. It contains actual collection gaps; it is not a complete continuous series.
- Observatory map, station selection, recorded history, source-row inspection and buying-plan calculations are implemented. Observations, retrospective reported events and an unvalidated baseline are distinguished. Missing collection days must remain gaps.
- openrouteservice key was verified with real driving routes, including a dedicated return trip and a journey detour. A repeat audit test returned a 7.801 km / 16.75 minute return route for public central-Brisbane coordinates and Liberty Highgate Hill. The key is server-side in ignored `.env`.
- QLDTraffic initially returned HTTP 429, then recovered. At 15:18 AEST it returned 532 events, 62 overlapping the Brisbane analysis area. The real overlay, Flooding filter, closure popup, directions/impact and provider details were checked in the browser. This is no longer a completely broken integration.
- AIP and RBA fetches had failed earlier; a deliberate audit refresh recovered both. AIP source date was 18 September; RBA source date was 17 September. A source date need not equal today's date.
- Fixed context freshness: a failed source fetch now marks retained observations stale even when their publication date is recent (`context_sources.py`).
- Fixed collection status: API evidence is reset to `not_checked` at the start of an attempt so a failed fuel request cannot leave it claiming that evidence is currently recording (`local_data.py`).
- Fixed traffic UI freshness after a local request failure so moving the map cannot restore an unjustified current-status label. Malformed geometry collections are handled defensively.
- Validation after these changes: **43 Python tests and 6 device-location JavaScript tests passed**, plus JavaScript syntax checks. Real device positioning remains unverified; mock tests are not evidence of a working device fix.

## Remaining failures, limitations and missing integrations

### 1. P1 — Continuous collection is not reliable across sleep/network outages

**State/evidence:** Intermittent operational failure, recovered. Server logs show failed collection attempts at 07:07 and 15:07 after a successful 00:10 capture. Manual refresh recovered at 15:21. The logs alone do not establish whether sleep, connectivity or a provider outage caused the full gap. A failed attempt currently waits the normal 30-minute interval. No startup/login service is configured.

**Change:** Add bounded failure retry/backoff independently of the normal successful cadence; preserve upstream cache limits and avoid overlapping workers. Record outage duration, failure category and last successful write for each output. Detect resume/missed collection windows and perform one catch-up fetch, never fabricate snapshots for missed times. Offer a documented local startup mechanism; user-controlled login/startup installation is a separate decision.

**Files:** `fuel_dashboard.py` (`collect`, `refresh_and_build`), `local_data.py`, `fuel_engine.py`, `start-local.ps1`, `tests/test_local.py`.

**Done when:** Simulated transient failures recover before the normal 30-minute delay without duplicate CSV rows or parallel requests; shutdown/sleep/resume behaviour is documented and tested where possible; a stale feed can never appear healthy.

### 2. P1 — Useful price prediction is blocked by sparse validated history

**State/evidence:** Capability is limited, not a graph rendering defect. `/api/forecast` returned `limited`, zero validated origins and three recent consecutive days. Its seven flat values are persistence (“today's price”), not a demonstrated prediction of the next cycle. Automatic wait recommendations are deliberately withheld when evidence is insufficient.

**Change:** Improve data coverage first (item 3), then run chronological model selection and a purged held-out comparison against persistence. Evaluate each 1–7 day horizon, recent/regime-specific performance and uncertainty calibration. Do not loosen validation thresholds merely to make a forecast appear. Keep manual scenarios clearly separate.

**Files:** `forecasting.py`, `local_data.py`, `buying_plan.py`, `static/observatory.js`, `static/app.js`, `tests/test_local.py`, `tests/test_buying_plan.py`.

**Done when:** A dated, reproducible report demonstrates whether the chosen model improves on the baseline using information available at prediction time. If it does not, the UI must continue to say so. Future records cannot alter earlier predictions in leakage tests.

### 3. P1 — Official monthly historical backfill is not imported

**State/evidence:** Unimplemented. The larger local CSV is included, but no importer for the Queensland monthly datasets fills the historical evidence gap. These datasets contain reported price events, not observations collected by this app at the time.

**Change:** Build an idempotent importer for [Queensland fuel history](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2026), storing source URL, source file checksum, publication/import times, station/fuel identity and report timestamps. Audit station coverage, unavailable prices and duplicates. Reconstruct an explicitly labelled reported-price series with a documented as-of policy; keep actual collected observations untouched and separate.

**Files:** `observatory.py`, `data_store.py`, `forecasting.py`, `docs/data-sources.md`; add an importer and provenance tests.

**Done when:** Importing twice creates no duplicates; daily station coverage reconciles to raw records; unit conversions/timezones are tested; no backfill is relabelled as a historical local capture; retrospective-only data cannot silently enter a point-in-time backtest.

### 4. P1 — Actual device location did not return a fix in the in-app browser

**State/evidence:** Previously observed host/browser failure; still unverified on real hardware. Tracking UI, accuracy circle, follow mode, cancellation and stale-fix handling are implemented. The in-app browser timed out without coordinates; a reload was needed after a pending permission interaction. No real location was fabricated.

**Change:** Diagnose browser and Windows location availability with the user’s permitted location workflow. Verify in a normal supported browser as well as the in-app host; give a direct recovery path if that host cannot supply a fix. Retain map-picked/manual coordinates. Do not auto-send location to routing: the user must explicitly calculate.

**Files:** `static/device-location.js`, `static/observatory.js`, `static/decisions.js`, `static/road-routing.js`, `tests/device-location.test.cjs`.

**Done when:** A real consented device fix appears with accuracy/time; movement updates it; Stop prevents late callbacks and removes the marker. Denied, unavailable and timeout paths remain usable. If host support is absent, document that limitation rather than declaring the feature fixed.

### 5. P2 — Route costs and the buying planner remain separate

**State/evidence:** Missing integration. The Road-cost lab calculates real extra driving, but does not populate or optimise the buying planner. The user currently enters extra km/minutes manually. Nearby-list radius distances are still straight-line by design.

**Change:** Add an explicit route-to-plan action with a visible explanation of what extra trip is being priced. Distinguish a normal commute, a dedicated fuel trip, the current purchase and a later refill. Preserve equal driving service and equal ending fuel; prevent double-counting consumption/time. Invalidate adopted costs when station, origin, destination, volume or price changes.

**Files:** `routing.py`, `buying_plan.py`, `static/road-routing.js`, `static/decisions.js`.

**Done when:** A route-backed plan can be traced through its assumptions, and tests prove equivalence of ending fuel and trip costs across buy-now/bridge scenarios. Coordinates remain unsaved unless the user deliberately opts in.

### 6. P2 — Automatic cheapest-total-trip station ranking is not built

**State/evidence:** Unimplemented. A station can be quoted individually, but the app does not compare multiple candidates for the same journey automatically. Estimated travel times are not live traffic times. Toll fees and queue/stop time are not priced.

**Change:** Rank a bounded shortlist using identical origin, destination, purchase volume, consumption and time value. Use quota-aware requests and caching; distinguish pump-price rank from total-trip-cost rank. Default to avoiding toll roads until actual toll costs or user-entered costs are included. Never call this globally optimal if only a shortlist was evaluated.

**Files:** `routing.py`, `static/road-routing.js`, `static/decisions.js`.

**Done when:** Ranking can reverse a pump-price bargain once travel is included; baseline trips match; candidates/timeouts/quota failures are visible; no background location upload or unbounded all-station API burst occurs.

### 7. P2 — Traffic events do not affect route recommendations

**State/evidence:** Missing integration, not a broken overlay. Traffic is a separate layer. ORS routes, ETA and fuel costs are unchanged by the displayed closures, incidents or roadworks. Current geometry filtering is bounding-box overlap, and date-window membership does not establish that a recurring restriction is active now.

**Change:** First add route-corridor event matching with exact geometry and explicit direction/schedule uncertainty. Show potentially relevant disruptions alongside each route. Treat area-alert polygons and bridges/parallel roads carefully. Only add avoidance/rerouting if supported and verified; do not invent numeric delays from labels such as “Long delays expected.”

**Files:** `traffic.py`, `routing.py`, `static/traffic.js`, `static/road-routing.js`.

**Done when:** Tests cover crossing segments, enclosing polygons, parallel roads, scheduled/expired events, recurrence uncertainty and stale feeds. A nearby unrelated event is not declared a road closure on the route. No “road is clear” assurance is inferred from an empty feed.

### 8. P2 — Shared traffic quota can make the overlay unavailable

**State/evidence:** Intermittent external limitation, recovered during audit. Public key is globally limited to 100 requests/minute. Five-minute upstream caching/cooldown, persisted across restarts, is implemented; failures retain dated data. Polling happens while a page is open/visible, not as an independent traffic archive worker.

**Change:** Retain conservative limits, honour provider Retry-After when supplied, and add source-availability history. A registered TMR key can be supplied through `QLD_TRAFFIC_API_KEY` if the user obtains one. A dedicated traffic history collector is a separate feature if trend/history analysis is desired.

**Files:** `traffic.py`, `config.py`, `static/traffic.js`, `tests/test_traffic.py`.

**Done when:** Repeated HTTP 429s do not create request storms; restart cannot bypass cooldown; recovery replaces stale status automatically. Registered-key configuration requires no frontend changes and never exposes private keys.

### 9. P2 — Wholesale/currency data is context, not a validated predictor

**State/evidence:** Sources recovered; predictive integration is unimplemented. AIP/RBA observations are displayed and dated, but not used by the model or recommendations. Failed context fetches retry hourly; fuel refresh can therefore succeed while a context retry remains pending. Current observation rows update revised values rather than preserving every revision vintage.

**Change:** Surface next retry and sanitized HTTP/parse failure categories. If modelling these feeds, first preserve revision vintages and known-at timestamps, then test lagged features against the baseline. Never treat the retail–wholesale gap as station profit. Brent is not a substitute for the refined-petrol benchmark.

**Files:** `context_sources.py`, `data_store.py`, `forecasting.py`, `static/source-intelligence.js`.

**Done when:** Source health is understandable independently of fuel health; replaying a past forecast cannot use a later revision; predictive inclusion requires measured held-out improvement.

### 10. P2 — Buying workspace still has the older consumer-style design

**State/evidence:** Incomplete product/design migration. `/` is the dark dense observatory; `/planner` retains its cream consumer layout, with shared map/routing/traffic controls inserted. Two frontend shells duplicate some station and health presentation.

**Change:** Bring the buying workspace into the same dense observatory design and shared navigation, preserving accessible forms and explicit assumptions. Consolidate reusable station, status and map components without rewriting data logic unnecessarily.

**Files:** `static/index.html`, `static/style.css`, `static/app.js`, `static/decisions.js`, `static/observatory.html`, `static/observatory.css`.

**Done when:** Desktop and narrow layouts provide the same information hierarchy; station deep links work; no chart, form, map or status functionality is lost; external-script errors cannot blank the whole page.

## Not implemented: product backlog, not current regressions

| Capability | Proposed next change | Acceptance condition |
| --- | --- | --- |
| Favourite price/target alerts | Local observed-price rules, minimum saving threshold, stale suppression and deduplication; notification delivery only when explicitly authorised | One event per qualifying transition; no stale/duplicate alerts |
| Refuelling journal / measured consumption | Local fill records, odometer validation and measured usage | Partial fills and missing odometers do not create false consumption estimates |
| Discount / voucher / price-lock comparison | Model eligibility, cap, expiry and user-entered offers | Net price reflects constraints and never assumes an unverified discount |
| Household / named vehicle profiles | Multiple local profiles and explicitly generated comparison summaries | Inputs cannot silently leak between vehicles; sharing is opt-in |
| Observed cycle/regional leadership signals | Matched-station rise share, temporal clustering and lag analysis | Observed signal remains separate from an unvalidated prediction |
| Traffic/flood camera views | Connect official camera metadata and clearly dated still images if useful | Attribution and source timestamp visible; never labelled live video |
| EIA / petroleum stocks context | Verify current access, series definitions, cadence and publication lag before importing | Provenance and release time recorded; no automatic model inclusion |
| Reliable free daily Singapore Mogas 95 benchmark | Research and verify a lawful, reproducible source | Do not fabricate a feed or substitute crude prices under the same label |

Public internet hosting, multi-user accounts and collection while the computer is off are **not currently supported**. The user asked for a local app; this is intentional scope, not an accidental outage.

## Implementation constraints and verification

- Preserve `.env`, the large CSV, SQLite data, `.local` evidence and the retired modules under `archive/`. Existing large data diffs and prior code changes are not disposable. Never print or commit private API keys.
- Tests: `.venv\Scripts\python.exe -m unittest discover -s tests -v`; Node `--test tests/device-location.test.cjs`.
- Use mocked tests for failure paths and at most a small number of real-provider checks. Do not spend shared quota on loops or pretend test fixtures are live data.
- Check `/api/health`, `/api/forecast`, `/api/source-evidence`, `/api/market-context`, `/api/traffic` and `/api/routing/status`; status HTTP 200 alone is insufficient—inspect provider/freshness fields.
- Browser verification should cover both `/` and `/planner`, real map selection, stale/unavailable states, narrow layout, route mode and source dates. Test ORS with public sample coordinates rather than silently sending the user's device location.
- Update this handoff with observed results and remaining blockers after each item. Record the distinction between fixed, recovered externally, unverified and not built.
