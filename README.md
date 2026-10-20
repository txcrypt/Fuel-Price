# Brisbane Fuel / Local

A local market observatory for **Brisbane unleaded 91**: a dense station map,
price distribution and movement, source timelines, raw records, model diagnostics,
and visible API / CSV / SQLite recording status. The buying workspace is at `/planner`.
The app code and Leaflet library are served locally. Internet access is needed
for the Queensland fuel API and OpenStreetMap street tiles. AI chat, news, routing and supply tracking have been retired to
`archive/`; they are not loaded by the app.

## Start

The local environment is already configured on this computer. From this folder:

```powershell
.\start-local.ps1
```

Open **http://127.0.0.1:8000**. The server binds to this computer only.
Keep the process running and the computer awake for collection to continue.
Closing the browser does not stop collection. Ctrl+C stops a foreground server.
The background server started during setup can be stopped with `./stop-local.ps1`.
Use `start-local.ps1 -Port 8001` if another application occupies port 8000.
Run only one collector against these files.

### Use it from your phone, anywhere

Keep the server bound to this computer and reach it privately through
[Tailscale](https://tailscale.com) (free for personal use). Nothing is exposed to the
public internet, and `tailscale serve` adds a real HTTPS certificate, which iPhone
Safari requires before it allows **Use my location**.

1. Install Tailscale on this computer and on the iPhone; sign in to the same account on both.
2. On this computer, with the dashboard running: `tailscale serve --bg 8000`
3. On the iPhone, open the `https://<computer-name>.<tailnet>.ts.net` address it prints,
   then Share → **Add to Home Screen**. It opens full screen on the buying planner.

Collection still needs this computer on and awake. For an always-on server instead,
run it on a free cloud VM (below). Do not publish it openly: it has no login, and the
Queensland fuel API's terms should be checked before its data is shared.

### Always on: Oracle Cloud Always Free VM

1. Create an Oracle Cloud account with home region **Australia East (Sydney)**. Create an
   instance: Ubuntu 24.04, shape `VM.Standard.A1.Flex` (1–2 OCPU, 6–12 GB; Always Free),
   or `VM.Standard.E2.1.Micro` if ARM capacity is unavailable. Save the SSH private key.
2. On the VM: `git clone https://github.com/txcrypt/Fuel-Price.git` (sign in with
   `gh auth login` first if the repository is private).
3. From this folder on the computer, copy the secrets and the latest recorded data
   (`fuel_data.db` is too large to push to GitHub):
   ```powershell
   scp -i <key> .env fuel_data.db brisbane_fuel_history_clean.csv brisbane_fuel_live_collection.csv live_snapshot.csv station_metadata.csv station_profiles.csv station_ratings.csv ubuntu@<vm-ip>:~/Fuel-Price/
   ```
4. On the VM: `bash ~/Fuel-Price/deploy/setup-vm.sh`. It installs Python packages and
   Tailscale, runs the dashboard as the `fuel-dashboard` systemd service (restarts on crash
   and reboot), and serves it over HTTPS on your tailnet. Open the login link it prints.
5. Stop the collector on this computer (`./stop-local.ps1`); run only one.
6. On the iPhone (Tailscale signed in): open `https://<vm-name>.<tailnet>.ts.net`, then
   Share → **Add to Home Screen**.

Logs: `journalctl -u fuel-dashboard -f`. After `git pull`, `sudo systemctl restart fuel-dashboard`.
If `tailscale serve` says HTTPS is off, enable MagicDNS and HTTPS certificates in the Tailscale
admin console (DNS page) and re-run it.

On phones, one finger scrolls the page and two fingers move the map; **⤢** opens a
full-screen map with normal one-finger dragging.

For a fresh installation (Python 3.11+):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
# Set FUEL_API_TOKEN in .env, then:
.\start-local.ps1
```

The existing API token has been moved into the ignored local `.env` file.
It is never returned to the browser. `REFRESH_SECONDS` defaults to 1800.

## Recording

The observatory's **07 Sources** panel shows API field coverage, all-grade station
prices, explicit U91 unavailability, brand comparisons, and dated AIP wholesale /
RBA currency context. See [the source audit](docs/data-sources.md) for integration
status, free-source priorities, caching and model limitations. All five documented
Queensland subscriber endpoints are retained; metadata uses a daily cache.

Both maps have **Use my location**, accuracy circles, follow and stop controls.
This requires browser/device permission. Coordinates remain in page memory;
the app does not save them or send them to the fuel API.

The default console provides four distinct history views: observed daily prices
(CSV + SQLite), individual CSV captures, retrospective station report events from
the live collection CSV, and the legacy report archive. Report timestamps are
parsed with mixed-format support and displayed in Brisbane time. Report-event
medians cover only reporting stations, not the whole market; they never enter
forecast training. The raw explorer supports source, state, station and pagination.
Price movement compares only matched stations against the most recent earlier CSV
capture; its timestamp is shown. A calendar makes missing collection days explicit.
Unvalidated persistence is hidden from the chart by default and can be inspected
with the model / baseline toggle. It is never described as a validated forecast.

Read-only evidence APIs: `/api/observatory`, `/api/observatory/station/{site_id}`,
and `/api/observatory/rows?source=live&state=QLD&offset=0&limit=50`.

- Fetches actual Brisbane U91 station prices at startup and every 30 minutes.
- Saves each successful snapshot to SQLite and atomically replaces `live_snapshot.csv`.
- Appends to `brisbane_fuel_live_collection.csv` at most once per hour, checking
  its last Brisbane timestamp even after a restart. Original CSV rows and database
  records, including historical WA data, are retained.
- Each destination reports success or failure independently. API failures preserve
  the saved observations, visibly mark the failed fetch, and retry next interval.
- The Refresh button requests a real API fetch; it respects the hourly CSV cadence.
- Data health and API responses are never served from an offline cache.
- Snapshot freshness and the station's last *price change report* are distinct:
  a station can report an unchanged price for several days.

All collection timestamps follow this computer's Brisbane timezone. The metro
filter is latitude -27.70 to -27.00 and longitude 152.70 to 153.50. North/South
labels are latitude bands split at -27.470, not river-boundary classifications.

## Forecasting honestly

Daily medians use each station's final observation for that day, equally weighted.
Training combines the recorded collection CSV with SQLite snapshots and removes
station/day duplicates. Legacy event-only history without collection timestamps
is excluded; missing days are not interpolated.

Four lightweight candidates are available: persistence (today's median), damped
trend, a weekly pattern, and similar historical weeks. Selection uses earlier
chronological origins; reported mean absolute error uses later held-out origins,
separated by seven days. All models see only data available at each origin.
Overlapping validation origins are not independent trials. Error bands describe
historical errors and do not guarantee future coverage.

When continuous data is insufficient, the app clearly labels a **baseline** and
withholds accuracy claims and error bands. Stale historical data does not generate
a current forecast. The current archive has large collection gaps; recording
consistently is the next requirement for credible accuracy comparisons.

Seven forward predictions are archived automatically each day (first forecast
of the day is retained), then scored against recorded medians after target dates
pass. Today's median is provisional until the day ends. Future improvements can
be compared against these saved forecasts rather than just an in-sample fit.

## Code and API

- `fuel_dashboard.py`: local HTTP server, scheduling, routes.
- `fuel_engine.py`: Queensland API requests and Brisbane U91 filtering.
- `local_data.py`: collection, CSV writes, data health, observed history.
- `forecasting.py`: transparent models and chronological validation.
- `public_feeds.py`: news headlines, BOM warnings and QFD bushfires (display only, cached).
- `traffic.py`: QLDTraffic events and the road-camera list.
- `data_store.py`: SQLite persistence; retains compatibility with existing tables.
- `static/`: responsive dashboard, no frontend build or CDN dependency.

API reference: http://127.0.0.1:8000/docs

`GET /api/dashboard`, `/api/health`, `/api/stations`, `/api/forecast`,
`/api/export/history`, `/api/export/snapshot`; `POST /api/refresh` and
`POST /api/buying-plan`. Map and news context: `GET /api/cameras`, `/api/bushfires`,
`/api/news`.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node --check static/app.js
```

Tests use temporary files and a fake API, covering storage failures, hourly CSV
cadence across restarts, API failure preservation, daily aggregation, HTTP routes,
chronological validation, and future-data leakage. Live API and browser checks
are separate from this repeatable test suite.

## Map and buying decisions

Use the map to select a station, save favourites, or set a starting point and a
search radius. Nearby-list distances are straight-line estimates. Open the map's
**Road-cost lab** for actual road routes from openrouteservice: enter/pick a start,
optionally an onward destination, then select **Calculate road cost**. With no
destination it calculates a dedicated return trip; otherwise it subtracts the
direct journey from the route through the selected station. It shows extra
kilometres/minutes, fuel and optional time cost, and effective purchase price.
Use identical inputs when comparing stations. This does not automatically change
the buying planner's assumptions. Driving times exclude live traffic and queues;
toll charges are not included.

Set `ORS_API_KEY` in the ignored local `.env` and restart to enable road routing.
The key stays on the server. Coordinates are sent to openrouteservice only after
Calculate, held in a bounded memory cache for reuse within five minutes, and are
not saved to disk. The route origin can copy a recent device fix or use a manually
chosen point. Device movement does not trigger routing calls.

The selected station price is passed to the buying planner. Vehicle settings
and favourites are saved in this browser only.

The planner compares filling now with a minimum top-up and another purchase
after 1–7 days, preserving the same everyday driving and ending fuel balance.
It accounts for a reserve, optional current-purchase budget, discount, extra
travel fuel and optional time value. The later purchase does not necessarily
fill the tank: its quantity is chosen to keep the comparison equivalent.

The minimum bridge quantity depends on fuel remaining, use and reserve. Tank
capacity affects the volume that can be postponed and the dollar saving.
The seven-day comparison only suggests a wait date when supported by validated
recent data and the historical-error stress check. Manual scenarios are clearly
labelled and are not extended to other dates.

Both maps: pinch or Ctrl+scroll to zoom (plain scrolling still scrolls the page);
press Enter in a map search box to jump to an Australian place; the top-right menu
switches between stations, 📷 road cameras (shown from street zoom, or all of them in
*Cameras only*), 🔥 bushfire incidents and 🌧 rain radar.

**Layers** (map menu): bushfires, rain radar, 🌡 price heat (cyan cheaper, red dearer than the
rest of the view), ⚡ EV chargers (needs a free `OCM_API_KEY` from openchargemap.org in
`.env`) and 🛢 fuel supply sites. The observatory map also has a **price-jump replay** that
steps through how a past jump spread across Brisbane stations, day by day.

**Insights** (both pages): today's U91 median, cheapest station and diesel for every
Queensland city with five or more stations, the weekday pattern (Brisbane currently has
none worth timing a fill-up by) and the hours stations raise prices. Selecting a station
on the planner shows its price history against its city and how often it was among the
cheapest quarter. Saved favourites appear in a **watchlist** with the change since the
previous day.

**Phone alerts** (planner): push notifications through the free ntfy app when a price jump
starts building, or a favourite reaches your target price. Checked after every collection,
so they work with no browser open. Off until you save a topic.

**History.** The official Queensland reports for the Brisbane area (Jan 2025 onwards) are
loaded into `fuel_data.db` (`python -m qld_history_import --years 2025 2026`), so charts,
backtests and the replay cover about 20 months. The forecast is chosen on the most recent
90 selection days and must beat today's price there; over the whole period the old rule
picked the model that did worst on later dates.

**Forward test.** Every forecasting method's seven-day path is saved daily
(`forward_predictions` table) and graded only against days this app collected.
The scoreboard appears under *A forecast has to earn its place*.

See [fuel hub ideas](docs/fuel-hub-roadmap.md) for the next features and the
mapping library/tile references.

## QLD Traffic

Both station maps include a **QLDTraffic** panel for road events, works and area
alerts, with source schedules and impact details. The server uses TMR’s published
public key by default; an optional `QLD_TRAFFIC_API_KEY` in `.env` can override it.
The shared public quota can be busy. All browser requests share a five-minute
upstream cache/cooldown, persisted across restarts. The page retries while open
and visible; fuel collection continues independently. Old events are marked stale
after failures, and a missing feed is labelled unavailable. This layer provides
context only: it does not automatically avoid closures or adjust route time/cost.
