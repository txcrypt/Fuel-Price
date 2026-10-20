# Extra dashboard information: research, recommendations and implementation plans

Researched 29 September 2026 by four parallel research passes: price and market
sources, station-level enrichment, calendar and environmental context, and data
already collected but not surfaced. Endpoints marked **verified** returned live data
on 28–29 September 2026. This is a point-in-time audit; re-check an endpoint before
building on it.

Every recommendation is graded against the product's three questions: **where do I
buy, how much do I buy, when should I buy again?** The project's rules still apply:
new inputs are display-only until a lagged, leakage-safe, held-out evaluation beats
persistence (see `docs/context-feature-evaluation-2026-09-18.md`).

## Headline findings

1. **Fuel excise steps sit inside the forecast held-out window and are not
   documented anywhere.** Petrol excise was 20.6 c/L from 1 Apr 2026, 36.6 c/L
   from 1 Jul, 53.7 c/L from 3 Aug ([ATO](https://www.ato.gov.au/businesses-and-organisations/gst-excise-and-indirect-taxes/excise-on-fuel-and-petroleum-products/excise-duty-rates-for-fuel-and-petroleum-products),
   verified). With GST that is roughly −35, +18 and +19 c/L at the pump, passed
   through over about 7–10 days. The held-out window in
   `docs/forecast-evaluation-2026-09-18.md` is 2026-04-14 to 2026-08-24, so it
   contains both increases. The "cost-driven climb July–August" in
   `docs/cycle-signal-evaluation-2026-09-28.md` is very probably excise
   pass-through. Both evaluations should be re-read with these dates marked.
2. **The ACCC says regular Brisbane price cycles have mostly not occurred since
   late February 2026.** Cycle-based signals will have little recent data to
   work with until cycling resumes.
3. **Much of the most useful new information needs no new source.** `api_price_events`
   holds 19,568 events (875 real Brisbane U91 price changes since 17 Sep), but the
   only code that reads it is a `SELECT COUNT(*)` (`api_evidence.py:141`).
4. **Two correctness problems affect "where to buy" today:**
   - **Costco.** Costco sites are in the feed and are members-only, but nothing marks them.
   - **Brand ranking.** The Sources brand table filters with `config.BOUNDS`, which
     has been statewide since 28 Sep (`api_evidence.py:42`), so it now ranks all of
     Queensland, not Brisbane.
5. **The official monthly backfill is still not in the tracked DB** (`reported_price_events` = 0 rows).
   Items 5, 6, 10 and 16 below only become statistically meaningful once it is.
   Importing it grows the git-tracked 81 MB database, so that is your decision (see *Decisions for you*).

## Ranked recommendations

Effort: S ≈ under a day, M ≈ 1–3 days, L = more. "New network" means a new upstream host.

### Tier 1: do first (correctness, high value, no new network dependency)

| # | Item | Answers | Value | Effort |
|---|---|---|---|---|
| 1 | Excise step calendar, plus flags in the evaluations | how much, when | High | S |
| 2 | Costco members-only flag | where | High (correctness) | S |
| 3 | Brand ranking area fix, plus a brand field and filter on stations | where | Med (bug) | S |
| 4 | Discount programs → effective price (user toggles) | how much, where | High | S |
| 5 | "Reliably cheap" station score | where | High | M |
| 6 | Cheap stations lagging a rise | where, when | High | S–M |
| 7 | Show collection outages and missing days | trust | Med | S |

### Tier 2: new free sources and derived views

| # | Item | Answers | Value | Effort | New network |
|---|---|---|---|---|---|
| 8 | AIP historical TGP backfill (daily since 2004) → margin history, plus a margin-as-hike-signal test | when | High (evaluation) | S | aip.com.au (already used) |
| 9 | 7-Eleven Fuel Price Lock simulation | where, how much, when | High | S | none |
| 10 | Restoration-signal history sparkline and price spread band | when, how much | Med–High | S | none |
| 11 | QLD public holidays, school holidays, Ekka (labels) | when | Med | S | date.nager.at |
| 12 | 7-day weather strip (extend existing Open-Meteo call) | when | Low–Med | S | already used |
| 13 | OSM fuel-station amenities and hours, plus EV chargers layer | where | Med | M | Overpass |
| 14 | Brisbane toll table in road-cost lab | where | Med | M | none (hand-kept) |

### Tier 3: evaluate first, or optional display

| # | Item | Why it waits |
|---|---|---|
| 15 | NSW FuelCheck history → Sydney/Brisbane lead-lag test | Worth a live API key only if the offline test shows a lead |
| 16 | Time of day prices change | About 2,800 events in one rising regime; descriptive only |
| 17 | Forecast scoreboard by horizon | Only 4 forward predictions scored so far |
| 18 | E10 net-cost note (−2.0 c/L vs about 3% more consumption) | One line in the brief, not a chart |
| 19 | ABS CPI Brisbane fuel, BCC road occupancies, Lytton refinery news query, RBOB futures | Display context; low marginal value |

### Not recommended (dead ends)

| Source | Reason |
|---|---|
| Google Places ratings/hours | ToS forbid use with a non-Google map; ratings can't be cached; paid. The only allowed use is a no-API "Open in Google Maps" link from the stored Place ID, which is worth adding (S). |
| Linkt toll page scraping | Imperva bot wall; use a hand-kept table (#14). |
| Singapore Mogas 95 | No free daily source (AIP's chart is Argus-licensed). Keep the "Gulf Coast is not Mogas" label. |
| VIC Servo Saver | Prices are delayed 24 h, so no leading value; approval is backlogged. |
| SA scheme, NT MyFuel | SA: low value, publisher registration needed. NT: stale since 2024. |
| WA FuelWatch day-ahead | Works (verified), but Perth runs its own cycle. Cross-market display only if wanted. |
| Qships, Port of Brisbane vessels, Ampol ASX JSON | Undocumented or 404, unclear rights. |
| data.gov.au holidays dataset | Stale after 2025; use Nager.Date. |
| Per-station cheapest weekday | 26 days across three regimes would mislead. |
| `station_metadata/profiles/ratings.csv` | Orphaned (read only by `archive/`), stale, collapse brands. Candidates for `archive/`. |

## Implementation plans

Each plan is an independently reviewable change. Each one leaves one small test
behind, following the style of the existing `tests/`.

### 1. Excise step calendar

- **What:** a dated table of petrol excise changes. Show an "upcoming known step"
  banner, and mark the steps on the history chart.
- **Data:** hand-kept, since it changes about three times a year and scraping isn't
  worth it. Add `EXCISE_STEPS` to `config.py` as a list of
  `(effective_date, cpl_ex_gst, announced_date, source_url)`, holding the 2026 rows
  above. Look up and record the pre-April rate and all **announcement** dates by hand
  from the ATO page and press releases. The next indexation is early February 2027,
  and its factor is published beforehand.
- **Backend:** `dashboard()` in `local_data.py` returns
  `excise: {current, next_step, steps}`. A step is shown as "upcoming" only when
  `announced_date <= today`.
- **Frontend:** a banner in `static/brief.js`; vertical markers on the history plot in
  `static/observatory.js` (the same mechanism as gap shading, #7).
- **Evaluation:** add an `--excise` flag to the `forecasting` CLI. It re-runs the
  held-out comparison (a) with step windows (effective date to +10 days) excluded,
  and (b) with a "persistence + 1.1 × Δexcise × GST" candidate whose step is
  visible only from `announced_date`. Publish the result next to the existing
  evaluations, even if it is negative.
- **Test:** `tests/test_excise.py`. An unannounced step is not exposed. The pump
  delta is 1.1 × the excise delta.

### 2. Costco members-only flag

- **Backend:** in `local_data.py` / `fuel_engine.py`, set `members_only=True` for
  brand name `Costco` (resolved via `.local/qld-api/latest.json` brands). Exclude
  these sites from "cheapest" lists and headline minimums unless the request passes
  `include_members=1`. They stay in the median, because they are real prices.
- **Frontend:** a "Costco member" toggle, saved in localStorage next to the vehicle
  settings (`static/decisions.js`), and a badge in popups.
- **Test:** a Costco site at the lowest price is not ranked first unless the toggle is on.

### 3. Brand ranking fix, plus brand on stations

- `api_evidence.py`: `summary()` uses `_within(site, config.ANALYSIS_BOUNDS)` for the
  brand table. Label it "Brisbane metro" via `config.ANALYSIS_AREA_NAME`.
- `LocalData.dashboard()`: add `brand` to each station dict (`brand_id` from
  `live_snapshot.csv` → name from the cached brands lookup). The `brand` column in
  `snapshots` is 100% NULL today, so populate it on write as well.
- `static/app.js`: add a brand filter chip row beside the below-median filter.
- **Test:** a statewide site outside `ANALYSIS_BOUNDS` does not appear in the brand table.

### 4. Discount programs → effective price

- **Data:** `discounts.json` (a dated, hand-kept table like `algo_config.json`):

  | Program | Brand(s) | c/L | Condition | Stackable |
  |---|---|---|---|---|
  | Coles Flybuys docket | Reddy Express | 4 | $30 Coles spend, 28 days | no |
  | Woolworths Everyday Rewards | Ampol (Foodary sites) | 4 | $30 Woolworths spend, 28 days | no |
  | EG in-store | EG Ampol | 4 | $5 in-store spend | unverified |
  | RACQ member | Caltex (incl. Puma) | 4 | membership, ≤120 L/day | no |

  Each row carries `checked: 2026-09-29`. Re-verify the RACQ end date and the EG
  docket acceptance before shipping.
- **Frontend:** user toggles ("I have a docket / RACQ") in `static/decisions.js`. The
  ranking and planner use `effective = pump − best applicable discount`. The pump
  price is always shown and never overwritten.
- Rows older than 90 days are shown as "unverified" and not applied by default.
- **Test:** non-stackable programs apply one discount only; an expired row is not applied.

### 5. "Reliably cheap" station score

- **Backend:** `station_stats()` in `observatory.py`, built from the combined frame
  `LocalData._history()` already builds (keep the per-station rows before the
  median). Per station:
  - share of observed days in the cheapest quartile;
  - mean c/L vs the day's median;
  - number of days observed.

  Expose it on `/api/observatory/station/{id}` and as a field in `dashboard()['stations']`.
- **Evidence it is stable:** a station's position vs the median has Spearman
  correlation 0.51–0.62 across Dec/Jul/Sep windows (495 stations). 54 of 503
  stations sat in the cheapest quartile on ≥80% of 15+ observed days.
- **Frontend:** a line in the inspector in `static/observatory.js`, plus a "consistently
  cheap" sort in `static/app.js`. Always show the day count; suppress the score
  below 10 days.
- **Test:** a synthetic three-station, five-day history gives the expected quartile shares.

### 6. Cheap stations lagging a rise

- **Backend:** add a `lagging` flag next to `price_flag()` (`local_data.py:51`). It is
  set when the price is ≤ median − 5 c/L, the last report is ≥ 3 days old, **and** the
  market is rising (`restoration.level != 'quiet'` or the 7-day median change is at
  least +3 c/L). The `stale` flag (>30 days) takes priority.
- **Today:** 159 stations are ≥5 c/L under the median, and 41 of them last reported
  4+ days ago.
- **Frontend:** a badge and filter reading "observed: hasn't followed the rise yet".
  Never "will rise".
- **Test:** the flag is off in a flat market and on for an old, cheap report in a rising one.

### 7. Collection outages and missing days

- **Current state:** `health.outages` (e.g. 18 Sep 17:27 → 28 Sep 12:59) and
  `coverage.missing_days` are already in the payload but not rendered.
- **Frontend:** shaded bands on the history plot and a list in the audit block
  (`static/observatory.js`, about line 141). JS only; no backend change.

### 8. AIP historical TGP backfill

- **Source:** [AIP historical TGP](https://aip.com.au/resources/historical-ulp-and-diesel-tgp-data)
  (verified: 5,932 daily rows from 2004, Brisbane 232.4 on 25 Sep 2026). The file
  name is dated, so find the `.xlsx` link on the page each time.
- **Implementation:** a manual CLI module like `qld_history_import.py`
  (`python -m aip_tgp_import`). It reads the XLSX with the stdlib (`zipfile` +
  `xml.etree`), because openpyxl is not in `requirements.txt` and one sheet doesn't
  justify adding it. It writes Brisbane ULP into
  `context_observations` under the existing AIP source id. It is idempotent on
  source/date.
- **Payoff:**
  - `paired_history` (`context_sources.py:303`) gets a margin point for every collected day.
  - The undocumented `tgp_history` table becomes redundant.
  - With the QLD backfill imported, you can test **"retail − TGP margin below X
    precedes a rise within N days"**, the classic when-to-buy indicator, using the
    existing evaluation harness.
- **Test:** parse a trimmed fixture XLSX; a re-import is a no-op.

### 9. 7-Eleven Fuel Price Lock simulation

- **Rule** ([T&Cs](https://www.7eleven.com.au/mobile/my-7-eleven-app-fuel-price-lock-terms-and-conditions.html)):
  the locked price is the cheapest of the up to 5 closest 7-Eleven stores. The lock
  lasts 7 days, covers up to 150 L, and gives at most 25 c/L off.
- **Implementation:** `buying_plan.py` gains `lock_price(origin, stations)`. The
  planner in `static/decisions.js` shows "Lock now at X, valid to date" as an
  option beside fill-now and wait.
- Label it as an estimate from our own snapshot; the app's real rule may differ.
- **Test:** the five-nearest selection and the 25 c/L cap.

### 10. Restoration history and price spread

- `LocalData._history()`: add a per-day hi20 share (share of stations ≥ median + 20)
  and the P25/P75 (and P10/P90) prices to the `groupby('day').agg`.
- **Frontend:** a sparkline with the 10% and 37% thresholds next to `data.restoration`
  (`static/app.js:67`), and a P25–P75 band on the history plot.
- Hide days with fewer than 100 stations. Leave gaps open.

### 11. Holidays and school terms

- **Source:** `https://date.nager.at/api/v3/PublicHolidays/{year}/AU` (verified), filtered
  to national + `AU-QLD`, and cached yearly in `public_feeds.py`. Add Ekka show day
  and school term dates (from education.qld.gov.au, HTML only) by hand to the same
  config.
- **Display:** "Long weekend in N days". Before implying any price effect, run one
  backfill test: is the median change over the 3 days before a QLD long weekend
  higher than on matched days? Power is low (about 10 holidays a year).

### 12. Weather strip

`static/area.js` already calls Open-Meteo for today. Add
`daily=precipitation_probability_max,weather_code` for 7 days. Display only; there
is no price hypothesis.

### 13. OSM amenities and EV chargers

- **Source:** Overpass `nwr["amenity"="fuel"](bbox);out center tags;`
  - **Verified:** `z.overpass-api.de` returned 485 stations.
  - The main instance returned 504, and the kumi and private.coffee mirrors timed out.
- **Coverage:** 164 stations have `opening_hours` (103 "24/7"), 158 compressed air
  and 40 LPG.
- **Implementation:** a weekly fetch in the `public_feeds.py` pattern with a mirror list.
  Match to QLD sites at ≤50 m, or ≤100 m with an agreeing normalised brand
  (402/497 matched within 50 m). Store the join locally only, since it is an ODbL
  derivative. Popups show "24/7 · air · LPG"; hours only where tagged.
- **EV chargers:** the same code with `amenity=charging_station` (103 found) as a map layer.
- **Attribution:** "© OpenStreetMap contributors" is already on the map. Choose the
  User-Agent contact string yourself (see *Decisions for you*).

### 14. Toll table

- **Data:** a hand-kept `tolls.json` with class-2 car prices (Gateway $6.02, Clem7
  $6.77, Legacy Way $7.30, AirportlinkM7 $7.16/$5.36, Go Between $4.22, Logan
  $2.28–3.76). Rates change 1 July (CPI); AirportlinkM7 changes 1 January.
- **Implementation:** when the lab's `avoid_tolls=False`, `routing.py` requests ORS
  `extra_info:["tollways"]` and maps toll segments to the nearest gantry. The lab
  adds "tolls (approx.)" to the detour cost.

### 15. NSW lead-lag test

- **Source:** [data.nsw FuelCheck](https://data.nsw.gov.au/data/dataset/fuel-check),
  verified with 125 monthly resources. CC BY-SA.
- **Implementation:** clone `qld_history_import.py`, filtering to Sydney by postcode.
  Test whether Sydney restorations lead Brisbane's by 1–3 days, using the 2025 data,
  when both cycled.
- Register for the free API key (2,500 calls/month) only if a lead shows up.

## Decisions for you

1. **Import the official QLD backfill into `fuel_data.db`?** It unlocks the
   statistics behind items 5, 6, 10 and 16, and the tests in 8, 11 and 15, but it
   grows the tracked DB.
2. **Remove the 216k WA snapshot rows** (57% of `snapshots`, unused since July)?
3. **Discounts that apply to you** (Flybuys, Everyday Rewards, RACQ, Costco
   membership). These set the defaults for #2 and #4.
4. **OSM User-Agent contact.** OSM asks clients to identify themselves.
   - **Privacy note:** one research request to `z.overpass-api.de` during this
     session sent your email address in its User-Agent header. All later requests
     used a generic UA.
   - Choose what the app sends, or leave it generic.

## Suggested order

Excise calendar and evaluation (1) → Costco and brand fixes (2, 3) → outages (7) →
discounts and fuel lock (4, 9) → station score and laggards (5, 6) → AIP backfill (8)
→ the rest as wanted. Items 1–7 and 9 touch no new upstream host.
