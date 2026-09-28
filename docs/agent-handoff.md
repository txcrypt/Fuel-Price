# Fuel Observatory: feature audit and implementation handoff

Audited 18 September 2026, Australia/Brisbane. This is a point-in-time audit, not a promise that external APIs remain available. Read current endpoint status before starting work.

## Task for the next agent

Continue the local Brisbane unleaded 91 information console. The user wants dense, inspectable information in a dark “God’s Eye” style, accurate price evidence, and decisions that minimise total fuel spend. Prioritise collection continuity, trustworthy historical coverage, and validated forecasts before adding more speculative signals. The default vehicle is 50 L, but all personal inputs remain editable.

Items 1, 2 and 3 have status notes; read them before reopening any of the three. Item 2's answer is a published negative result, not a working forecast, and item 3's backfill is still not in the tracked database. Preserve existing data and working features. Each item describes an independently reviewable change; do not treat the entire backlog as permission to publish the service, create accounts, buy services, or send notifications.

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

### Source keys connected 18 September 2026 (before starting items 1-3)

- **RBA AUD/USD moved to the official RSS-CB feed** (`https://www.rba.gov.au/rss/rss-cb-exchange-rates.xml`), parsed with the standard library instead of the HTML rate table. The feed carries one 4.00 pm observation per publication rather than the table's few days, so first-run backfill is smaller; accumulated history in `context_observations` is unaffected. The parser rejects a feed that is not base AUD / target USD rather than silently inverting the rate. Verified live: 0.7108 for 2026-09-17, matching the HTML table it replaces.
- **US EIA connected** with a user-supplied registered key in ignored `.env` (`EIA_API_KEY`). Two daily `/v2/petroleum/pri/spt/` series: `EER_EPMRU_PF4_RGC_DPG` (US Gulf Coast conventional regular petrol, $/gal) and `RBRTE` (Europe Brent, $/bbl). Verified live through the running server: both returned 60 observations, latest 2026-09-15 for a 2026-09-18 fetch, so a multi-day publication lag is normal and staleness is judged at eight days. **Displayed as dated context only; neither series enters the forecast or any recommendation.** The Gulf Coast series is a refined-petrol benchmark from a different market and is never relabelled as Singapore Mogas 95.
- **Australian Petroleum Statistics dropped as an acquisition target** at the user's direction: the publisher releases monthly documents with no API.
- Key handling: a missing key reports `not_configured` and attempts no unauthenticated request; request parameters are stripped from `/api/market-context`; provider exception text is replaced with fixed wording because it can echo the request URL and its key. Tested.
- Fixed while adding sources: `ContextSources.payload` paired wholesale prices from `sources[0]` positionally, which would have paired retail against an EIA series once more sources were added. It now selects the AIP source by id.
- Validation: **47 Python tests and 6 device-location JavaScript tests passed**. All four context sources verified `connected` through the running server at `/api/market-context`, and all four cards confirmed rendering in the browser with no console errors.

## Remaining failures, limitations and missing integrations

### 1. P1 — Continuous collection is not reliable across sleep/network outages

**State/evidence:** Intermittent operational failure, recovered. Server logs show failed collection attempts at 07:07 and 15:07 after a successful 00:10 capture. Manual refresh recovered at 15:21. The logs alone do not establish whether sleep, connectivity or a provider outage caused the full gap. A failed attempt currently waits the normal 30-minute interval. No startup/login service is configured.

**Change:** Add bounded failure retry/backoff independently of the normal successful cadence; preserve upstream cache limits and avoid overlapping workers. Record outage duration, failure category and last successful write for each output. Detect resume/missed collection windows and perform one catch-up fetch, never fabricate snapshots for missed times. Offer a documented local startup mechanism; user-controlled login/startup installation is a separate decision.

**Files:** `fuel_dashboard.py` (`collect`, `refresh_and_build`), `local_data.py`, `fuel_engine.py`, `start-local.ps1`, `tests/test_local.py`.

**Done when:** Simulated transient failures recover before the normal 30-minute delay without duplicate CSV rows or parallel requests; shutdown/sleep/resume behaviour is documented and tested where possible; a stale feed can never appear healthy.

**Status 18 September 2026 — implemented.** Root cause was `refresh()`'s `finally` block setting `next_refresh = now + REFRESH_SECONDS` on every outcome. Failure now schedules `max(60, min(REFRESH_SECONDS, 60 * 2**(n-1)))`; verified empirically as 60/120/240/480/960/1800, capped, resetting to 1800 on success. The 60 s floor is deliberate: `fuel_engine._cached('prices')` has a 60 s TTL, so a faster retry would only re-serve cache. Write-stage failures count as failures, not just fetch failures. `fuel_engine.FetchError(RuntimeError)` carries `.category` (`http_<code>`/`parse`/`network`/`empty`/`not_configured`); `ValueError` is caught before `requests.RequestException` because `JSONDecodeError` subclasses both. `_record_startup_gap()` appends one bounded entry (last 20) when a persisted gap exceeds 1.5x the interval; no snapshot rows are fabricated. `fuel_dashboard.py` needed no change — health already spreads the status dict. Confirmed live after restart: all of `consecutive_failures`, `outages` and the per-output `last_*_write` timestamps are present at `/api/health`, and `health.fresh` still derives only from snapshot age.

**Still unverified:** the `-InstallStartupTask` / `-RemoveStartupTask` switches in `start-local.ps1` are parse-checked only and were deliberately NOT run — installation remains the user's decision, and whether non-elevated `Register-ScheduledTask` succeeds here is unobserved. Failure categories are proven against mocked exceptions only; no live 4xx/5xx or DNS failure was induced. Actual machine sleep was not tested, only a simulated stale `collector_status.json`. Repeated restarts during one long outage append one entry each rather than merging — deliberate.

### 2. P1 — Useful price prediction is blocked by sparse validated history

**State/evidence:** Capability is limited, not a graph rendering defect. `/api/forecast` returned `limited`, zero validated origins and three recent consecutive days. Its seven flat values are persistence (“today's price”), not a demonstrated prediction of the next cycle. Automatic wait recommendations are deliberately withheld when evidence is insufficient.

**Change:** Improve data coverage first (item 3), then run chronological model selection and a purged held-out comparison against persistence. Evaluate each 1–7 day horizon, recent/regime-specific performance and uncertainty calibration. Do not loosen validation thresholds merely to make a forecast appear. Keep manual scenarios clearly separate.

**Files:** `forecasting.py`, `local_data.py`, `buying_plan.py`, `static/observatory.js`, `static/app.js`, `tests/test_local.py`, `tests/test_buying_plan.py`.

**Done when:** A dated, reproducible report demonstrates whether the chosen model improves on the baseline using information available at prediction time. If it does not, the UI must continue to say so. Future records cannot alter earlier predictions in leakage tests.

**Status 18 September 2026 — evaluated; the answer is negative and it stands.** Full evidence in `docs/forecast-evaluation-2026-09-18.md`; regenerate with `.venv\Scripts\python.exe -m forecasting --db <db holding the backfill> --root . --today 2026-09-18`. `build_forecast(daily, today=None, reported=None)` now merges the official reported series under the collected one through `merge_history`, keeping per-day provenance; collected always wins a shared day. Evaluated on 611 merged days (587 reported, 24 collected), 400 origins, 260 selection / 133 purged held-out (2026-04-14 to 2026-08-24), each horizon scored separately.

**Result: persistence still wins.** The selection winner `analog` came last on held-out data (h1 2.60 / h7 13.66 cpl vs the baseline's 1.26 / 7.77) — overfitting caught exactly where it should be. `damped_trend` beat the baseline at all seven horizons but was not selected, wins on only ~32% of origins, and every moving-block bootstrap interval on its edge straddles zero; it was deliberately **not** adopted post hoc. The 80% band is miscalibrated both ways: 93.8% coverage for persistence (too wide), 76.4% for analog (too narrow). No threshold was loosened. `/api/forecast` now carries `provenance`, per-model `horizon_mae` and `band_coverage`, `beats_baseline`, `recent` (90-day held-out slice) and `calibration`, and its `reason` states in words both that retrospective reported days were used and that the selected model did not beat the baseline.

Guards added: staleness and `recent_consecutive_days` now derive from **collected** days only, so a month-old backfill can never make a stopped collector look current. `buying_plan.compare_plan` additionally requires the selected model to have beaten persistence on held-out data **at the horizon being priced**. Three new `ForecastTests` cover the backfilled series, the provenance flag and the freshness guard; 73 Python tests pass.

**Still unverified:** the backfill lives only in a scratch database — the tracked `fuel_data.db` still holds no reported events, so the running server still serves the pre-backfill `limited` state. The server was **not restarted**, so it is still running pre-change code; the new path was verified by building `LocalData` against a temporary workspace holding the backfill. Every evaluated origin is a reported origin; nothing here measures behaviour on collected days. An uncached dashboard build costs ~7 s with the backfill present (`forecasting.MAX_ORIGINS`). Frontend files were untouched — `static/observatory.js` still renders whatever `/api/forecast` returns and does not yet show the new per-horizon or calibration fields.

### 3. P1 — Official monthly historical backfill is not imported

**State/evidence:** Unimplemented. The larger local CSV is included, but no importer for the Queensland monthly datasets fills the historical evidence gap. These datasets contain reported price events, not observations collected by this app at the time.

**Change:** Build an idempotent importer for [Queensland fuel history](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2026), storing source URL, source file checksum, publication/import times, station/fuel identity and report timestamps. Audit station coverage, unavailable prices and duplicates. Reconstruct an explicitly labelled reported-price series with a documented as-of policy; keep actual collected observations untouched and separate.

**Files:** `observatory.py`, `data_store.py`, `forecasting.py`, `docs/data-sources.md`; add an importer and provenance tests.

**Done when:** Importing twice creates no duplicates; daily station coverage reconciles to raw records; unit conversions/timezones are tested; no backfill is relabelled as a historical local capture; retrospective-only data cannot silently enter a point-in-time backtest.

**Status 18 September 2026 — importer built and smoke-tested; the backfill itself is NOT yet imported.** `qld_history_import.py` (manual CLI only, never on the server path) resolves CKAN resources, streams each ~13 MB CSV to gitignored `.local/qld-history/`, checksums while streaming, imports filtered rows, then deletes the raw file — the sha256 is the evidence. Filters: `Site_State == 'QLD'`, `Fuel_Type == 'Unleaded'`, `config.BOUNDS`, 80-350 c/L. Dates parse with an explicit `%d/%m/%Y %H:%M` + `utc=True`; `format='mixed'` is forbidden here because it silently mis-reads DD/MM as MM/DD. New `reported_price_events` and `import_sources` tables are strictly separate from `snapshots`.

Real smoke test, August 2026, into a scratch DB: 92,244 rows read, 8,887 kept, 93 unavailable, 31 days reconstructed, reconciled 8,887 == 8,887. Published 2026-09-02 for August events, confirming the ~1 month lag — which is why `reported_series()` and `coverage()` always return `retrospective: True` and `origin: 'reported'`.

**Design decision:** an unavailable `9999` is stored as `price_cpl = 0.0`, not NULL, because SQLite treats NULL key parts as distinct and a NULL price would have silently broken the `INSERT OR IGNORE` idempotency the primary key exists for. 0 is unambiguous against 80-350, is masked out of the series, and correctly cancels a held price rather than quoting it stale. As-of expiry is `ffill(limit=14)` over a `date_range`-reindexed daily index, so the limit counts calendar days, not present rows; day+14 is carried, day+15 is not.

**Remaining before item 2 can use this:** the multi-year import has deliberately not been run, because `fuel_data.db` is tracked in git at 71 MB and growing it is the user's decision. `fuel_data.db` did gain the two new tables as empty schema (`_ensure_schema` runs on every construction); `snapshots` still holds 345,826 rows. Only the August 2026 file has been parsed — other months are assumed to share the header. Now wired into `forecasting.py` via `LocalData._reported()` (see item 2): with no imported events it returns no days and the forecast is unchanged. `/api/observatory/coverage` is tested by calling the handler directly plus asserting route registration, not through a live TestClient; it was confirmed by hand against the running server.

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

**Status 18 September 2026 — implemented.** `RoadRouter.compare` now returns an `adoption` block alongside the existing figures, and `RouteRequest` gained `leg`. The block carries **only kilometres, minutes, consumption and time value — never a dollar amount** — because `routing.py` values the detour at the station price and `buying_plan.compare_plan` values it again from `extra_km * consumption / 100`; passing a cost across as well would charge the same detour twice. A test asserts no key in the block contains "cost".

Two legs are offered and no more, because `compare_plan` charges extra travel to the deferred plan only: `later_refill` (the deferred refill alone drives it) and `both_purchases` (both options drive it, so it cancels to exactly 0 km / 0 min). A third case — a detour only the immediate fill needs — would require a negative `extra_km`; that is deliberately refused rather than clamped, as is any detour past the planner's 300 km / 300 min limits, and a time value above $200/hour is reported as a note instead of being silently dropped. Values are rounded server-side to the planner's own input steps (0.1 km, 1 minute) so the number shown is the number priced.

**Equivalence proof** (`AdoptionTests.test_routed_detour_is_charged_exactly_once_in_the_plan`), 12 km / 18 min adopted at 200.0 c/L, 8 L/100 km, $30/hour, tank 50 L, remaining 10 L, reserve 5 L, 35 km/day, 3-day wait, zero price change: fill now buys 40.00 L for $80.00; the deferred plan buys 4.40 L now plus 36.56 L later for $90.92. Litres bought differ by exactly the detour's 0.96 L; cost differs by exactly $1.92 of fuel plus $9.00 of time; both finish with 41.60 L (10 + 4.40 + 36.56 − 8.40 driving − 0.96 detour = 50 − 8.40). The bridge round-up to 0.1 L cancels out of the total because `later_l` is derived from the rounded `bridge_l`. On the cancelling leg the deferred cost equals the fill-now cost to the cent.

Invalidation is by fingerprint: `routing.fingerprint()` hashes station, price, origin, destination, litres, consumption, time value, tolls and leg. `static/road-routing.js` calls `onInvalidate` from its existing `invalidate()`, which already fires on every route-form edit, map pick and station change; `static/decisions.js` then zeroes the adopted km/minutes and says why. Editing `extra_km`, `extra_minutes`, `consumption`, `hourly_value` or the station by hand keeps the number but drops the provenance label. `BuyingProfile.extra_travel_basis` (160 chars, echoed into `warnings`) makes the trace survive into the server result; a non-zero extra with no basis is labelled "typed in by hand".

**Still unverified:** the server was **not restarted**, so `/planner` is serving the new JavaScript against the old API — the adopt block is absent from the old `/api/routing/compare` response and the UI degrades to its previous behaviour. Nothing in the browser flow (adopt button, invalidation note, form population) has been exercised against the new endpoint. `.road-adopt` and `.road-rank-table` styling is a small inline `<style>` inside the routing panel because `static/style.css` and `static/observatory.css` belong to other agents; it is functional, not designed. The equal-driving-service invariant holds in the sense the module already used: everyday driving (`daily_km * wait_days`) is identical in both branches and the detour is charged as a cost of deferring, not as service.

### 6. P2 — Automatic cheapest-total-trip station ranking is not built

**State/evidence:** Unimplemented. A station can be quoted individually, but the app does not compare multiple candidates for the same journey automatically. Estimated travel times are not live traffic times. Toll fees and queue/stop time are not priced.

**Change:** Rank a bounded shortlist using identical origin, destination, purchase volume, consumption and time value. Use quota-aware requests and caching; distinguish pump-price rank from total-trip-cost rank. Default to avoiding toll roads until actual toll costs or user-entered costs are included. Never call this globally optimal if only a shortlist was evaluated.

**Files:** `routing.py`, `static/road-routing.js`, `static/decisions.js`.

**Done when:** Ranking can reverse a pump-price bargain once travel is included; baseline trips match; candidates/timeouts/quota failures are visible; no background location upload or unbounded all-station API burst occurs.

**Status 18 September 2026 — implemented.** `RoadRouter.rank(RankRequest, dashboard)` and `POST /api/routing/rank`. Each candidate reuses `compare()`, so the money arithmetic is not duplicated anywhere. The shortlist is hard-bounded at `MAX_CANDIDATES = 8`: either stations named explicitly (list capped at 8, each id length-checked) or the `limit` (2-8, default 5) lowest pump prices within `radius_km` straight-line of the start. `shortlist_rule` states the selection in words with all three counts, and `headline` always reads "Cheapest of the N stations evaluated — not the cheapest in Brisbane." Worst case is N+1 provider calls against the existing 30-per-minute limiter, because the origin→destination baseline leg is one request shared by every candidate through the 300-second cache; a test asserts 3 candidates with a destination cost exactly 4 calls, and `baseline_consistent` reports whether every candidate really did share one baseline.

A `422` candidate (no route) is recorded in `failures` and the loop continues; a `429` or `503` **stops the loop** rather than spending the rest of the allowance, and sets `quota_stopped`. `partial` is true whenever anything was dropped, including station ids that are not in the current priced snapshot, and the frontend prints "Partial ranking. N of M candidates were priced" with the reasons. If nothing survives, the quota status is re-raised as 429 rather than masked as a generic failure. `avoid_tolls` still defaults true and the note repeats that toll charges and queue time are not priced and driving times are not live traffic.

**Rank reversal, synthetic (unit test, time valued at $0 so the reversal is pure distance):** 40 L at 10 L/100 km. Far bargain 190.0 c/L, 40 km return → $76.00 + 4.00 L × $1.90 = **$83.60** (209.0 c/L effective). Near corner 196.0 c/L, 6 km → $78.40 + 0.60 L × $1.96 = **$79.58** (198.9 c/L). Closest 202.0 c/L, 2 km → **$81.20**. Pump order A-B-C reverses to B-C-A on total.

**Rank reversal, real provider, 18 September 2026**, 4 openrouteservice calls from public King George Square (-27.4705, 153.0260) to public Highgate Hill (-27.4990, 153.0130), 40 L, 8 L/100 km, $30/hour, 6 km radius, shortlist 3 of 57 stations in radius / 500 priced: baseline direct journey 9.278 km / 21.44 min, consistent across all three. Liberty Annerley has the lowest pump price at 219.5 c/L but 9.08 km / 14.1 min of detour → $96.42; 7-Eleven Greenslopes at 222.5 c/L needs only 8.13 km / 11.6 min → **$96.25**, so pump rank 3 wins on total. Caltex Holland Park, pump rank 2 at 221.9 c/L, is last at $101.78 on 16.72 km of detour.

**Be careful with that live example:** the margin is 17 cents and it is driven by the time value. At $0/hour the same three routes put Liberty Annerley back in front ($89.40 against $90.45). The synthetic test is the robust demonstration; the live one shows the mechanism is real but marginal at these distances.

**Still unverified:** the server was **not restarted**, so `/api/routing/rank` does not exist on the running instance and the ranking UI was never exercised in the browser — it was verified by calling `RoadRouter.rank` directly against the live `/api/dashboard` snapshot and the real provider. Only one live shortlist of three was routed; provider behaviour under a real 429 is still mocked-only. The 30-calls-per-minute limiter is not pre-checked before a ranking starts, so a shortlist begun with little allowance left simply returns a partial result instead of refusing up front. Ranking compares `cost_including_travel` across candidates, which is only meaningful because purchase volume is identical for all of them; it says nothing about buying different volumes at different stations.

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

**Status 18 September 2026 — implemented.** No dependency added; state lives in the existing `.local/traffic/latest.json`. `Retry-After` is honoured only to wait longer: the parsed value is clamped to `[INTERVAL, MAX_BACKOFF]` (300 s to 3600 s), so a provider asking for 30 s still waits 300 s and one asking for 999999 s waits 3600 s; a missing or unparseable header falls back to the fixed cooldown. Source-availability history is a bounded list under `availability`, one entry per attempt that actually ran (`at`, `outcome`, `http`, `age_seconds` of the retained feed), capped at `HISTORY_LIMIT = 288` (~24 h of five-minute attempts) with the oldest dropped. Outcome categories: `ok`, `rate_limited`, `rejected`, `http_error`, `unreachable`, `bad_payload`, `not_configured`. A `requests.exceptions.JSONDecodeError` now reports "returned invalid JSON" instead of being mislabelled as unreachable. Exposed through `/api/traffic` with `availability_limit`, `availability_outcomes` and an `availability_note` stating that a gap is a period nobody asked, not a period without incidents.

`static/traffic.js` draws it with `window.FuelPlot.plot` (guarded, since `/planner` does not load `observatory.js`): a per-attempt outcome strip plus a dated line of retained-feed age in minutes, which climbs during an outage and returns to zero on recovery. Tests added for Retry-After clamping, one request per cooldown across 60 polls with a fresh `TrafficFeed` each time, history contents/bounding, and restart persistence. Suite 73 → 77.

This unblocks item 11's deliberately-unbuilt feed-availability-over-time visual; the comment naming that dependency is gone.

### 9. P2 — Wholesale/currency data is context, not a validated predictor

**State/evidence:** Sources recovered; predictive integration is unimplemented. AIP/RBA observations are displayed and dated, but not used by the model or recommendations. Failed context fetches retry hourly; fuel refresh can therefore succeed while a context retry remains pending. Current observation rows update revised values rather than preserving every revision vintage.

**Change:** Surface next retry and sanitized HTTP/parse failure categories. If modelling these feeds, first preserve revision vintages and known-at timestamps, then test lagged features against the baseline. Never treat the retail–wholesale gap as station profit. Brent is not a substitute for the refined-petrol benchmark.

**Files:** `context_sources.py`, `data_store.py`, `forecasting.py`, `static/source-intelligence.js`.

**Done when:** Source health is understandable independently of fuel health; replaying a past forecast cannot use a later revision; predictive inclusion requires measured held-out improvement.

**Status 18 September 2026 — implemented. Verdict: no feed earned inclusion.** Full numbers in `docs/context-feature-evaluation-2026-09-18.md`.

Source health now rides on `/api/market-context` as its own `health` block and its own UI strip, separate from the QLD capture line; each card shows next retry, failure category and consecutive failures. Categories derive from exception **type and HTTP status only** — `failure_category()` never reads `str(error)`, because provider text echoes the request URL with `api_key` in it. The 6h/1h retry split was duplicated between `refresh` and the display and is now one `retry_seconds()` both call, so the displayed time is the real one.

Vintages: new append-only `context_vintages(source, observed_date, value, known_at)`, written before the upsert overwrites, and only when the value actually differs so unchanged republication does not grow the archive. `observations_as_of()` reads the latest vintage recorded at or before a moment. Two bugs the tests caught: second-precision `known_at` collided on the PK so `INSERT OR IGNORE` silently discarded revisions (now full precision), and replay metadata leaked later revisions through a row count (the whole vintage frame is now filtered to `known_at <= today`).

Result, 133 held-out origins on the same purged split as item 2 — persistence reproduces that report's baseline row exactly, which is the cross-check that this scores the same thing. **At h1-h2 both EIA feeds are significantly WORSE than persistence**, bootstrap intervals entirely above zero — a measured loss, not a null. Where either is nominally ahead the interval straddles zero. Both beat a drift control at long horizons and still lose to persistence, so the apparent long-horizon gain is the intercept being less bad than unconstrained drift, not the feed carrying information. Four feature windows (3/7/14/28 days) were searched and the report says so, because searching silently is how a null becomes a false positive. Nothing was adopted; `build_forecast` and `buying_plan` are untouched.

`aip` (6 stored observations) and `rba` (3) are **untestable, not disproven** — zero usable origins. That is a statement about stored history, not about the relationship; the vintage store makes an honest test possible in about a year.

**Unverified, and it matters:** the EIA `known_at` values used in this evaluation are **reconstructed, not recorded**. This app has held those series only since 2026-09-18, so a one-off extended pull back to 2024-10-01 was dated `known_at = observed_date + 4 days`. **That assumes EIA does not revise these spot series, which was not checked against their revision policy.** It is exactly the assumption `context_vintages` exists to retire, and any future rerun should prefer recorded vintages over reconstructed ones. Also: one feature shape per feed (lagged change, linear); levels, ratios, AUD-converted series and cross-feed interactions are untested. All usable origins are reported origins.

**Side effect to know about:** an empty `context_vintages` table was created in the tracked `fuel_data.db` during analysis, because constructing `LocalData` against the workspace runs the schema migration. Zero rows, and `reported_price_events` is still empty, so no backfill went in.

### 10. P2 — Buying workspace still has the older consumer-style design

**State/evidence:** Incomplete product/design migration. `/` is the dark dense observatory; `/planner` retains its cream consumer layout, with shared map/routing/traffic controls inserted. Two frontend shells duplicate some station and health presentation.

**Change:** Bring the buying workspace into the same dense observatory design and shared navigation, preserving accessible forms and explicit assumptions. Consolidate reusable station, status and map components without rewriting data logic unnecessarily.

**Files:** `static/index.html`, `static/style.css`, `static/app.js`, `static/decisions.js`, `static/observatory.html`, `static/observatory.css`.

**Done when:** Desktop and narrow layouts provide the same information hierarchy; station deep links work; no chart, form, map or status functionality is lost; external-script errors cannot blank the whole page.

### 11. P2 — Connected sources are listed but barely visualised

**State/evidence:** Missing presentation, not a broken feed. Every connected source now returns dated series through `/api/market-context` and `/api/traffic`, but the console shows almost none of it as a picture. Context sources render as four single-value cards: the AIP series holds a handful of observations, RBA accumulates one per publication, and each EIA series returns 60 daily points, none of which are plotted. QLDTraffic returns a full GeoJSON feed with event types, impacts, schedules and provider details, and the only visualisation is the map overlay plus a text list; the returned `counts` breakdown, feed freshness over time and the metro/total/unlocated split are numbers in a sentence. The observatory already has a working inline SVG plotter (`plot()` in `static/observatory.js`) that nothing outside the price chart reuses.

**Change:** Give every connected source a visual reading, reusing the existing plotter rather than adding a charting dependency. Context: a sparkline per card and a shared dated multi-series panel, each series on its own axis with its own units, showing collection gaps as gaps. Wholesale-vs-retail already has a paired table and should gain the matching plot. Traffic: event-type composition, a metro-versus-feed count, and feed availability over time once item 8's source-availability history exists. Keep units, source dates and staleness visible on every visual; a chart must not imply an observation on a day the source did not publish. Do not plot any series against price in a way that implies a tested relationship — these remain descriptive until item 9's held-out evaluation says otherwise.

**Files:** `static/source-intelligence.js`, `static/traffic.js`, `static/observatory.js` (export the existing plotter), `static/observatory.css`, `traffic.py`.

**Done when:** Each connected source has at least one dated visual with correct units and explicit gaps; publication lag and staleness are readable from the visual itself; narrow layouts keep the same information; no chart asserts a predictive relationship that has not been measured.

**Status 18 September 2026 — implemented.** No charting dependency added. `static/observatory.js` now exports its existing inline-SVG plotter as `window.FuelPlot`; the hardcoded 5 c/L gridline step became a parameter, because a 0.7108-0.7133 USD/AUD series renders as a flat line on a 0-5 axis. Verified after the change that the price chart is unaltered: gridlines are still 150/165/180/195/210.

Added: a sparkline per context card, a `#context-multiples` small-multiples grid over one shared date window with a per-source 1-2-5 axis step (c/L, USD/AUD, USD/gal and USD/bbl never share a scale), and retail-vs-wholesale plus gap charts fed directly from the server's `paired_history` with `gap_note` printed verbatim so the "not station profit" wording survives. Traffic gained an event-type composition and a feed-coverage group; confirmed live at 58 metro events of 533 returned, typed Roadworks 44 / Special event 5 / Congestion 4 / Hazard 3 / Flooding 1 / Crash 1, which sums to 58.

Gap handling confirmed empirically: AIP 6 points render as 2 segments, both EIA series' 60 points as 13 segments each (weekends), collected history as 10. Nothing is interpolated across a non-publication date. Fewer than two points renders an explicit "not enough observations yet" state rather than a misleading line. No context series is plotted against retail price anywhere.

**Deliberately not built:** feed-availability-over-time, which needs item 8's source-availability history. A comment in `static/traffic.js` names that dependency instead of inventing a history.

**Unverified:** the browser pane reports `document.hidden: true` and does not paint, so layout was confirmed by geometry queries and DOM inspection rather than by eye. That same pre-existing behaviour suppresses `traffic.js`'s fetch while hidden; the property had to be stubbed to exercise the traffic bars at all. Narrow-layout behaviour at 375px is geometry-verified, not visually reviewed.

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
