# Source audit and acquisition priorities

Verified 17 September 2026 against the live subscriber API and official publisher documentation.

## Queensland subscriber API

The old collector consumed only site details and prices, filtered immediately to usable Brisbane U91 prices, and discarded other fields. The current integration reads all five documented endpoints:

| Endpoint | Treatment |
| --- | --- |
| GetFullSiteDetails | Daily cache; address, coordinates, brand and region IDs, postcode, modification time and Google Place ID retained. All additional fields preserved verbatim. |
| GetSitesPrices | Existing 30-minute collection; a persistent one-minute minimum between upstream requests. All grades retained. Raw 9999 means unavailable. |
| GetCountryBrands | Daily lookup for readable brand names and descriptive brand comparisons. |
| GetCountryFuelTypes | Daily lookup for grade names; other grades remain separate from U91 analysis. |
| GetCountryGeographicRegions | Daily lookup for actual suburb and city names; existing North/South bands remain approximate. |

The audit returned 1,808 Queensland sites and 7,004 price rows. Within the configured metro bounds: 541 sites, 2,450 all-grade rows, 500 usable U91 prices and four explicit U91 unavailability reports. These are audit counts, not constants. Extra site fields beyond the published PDF are retained without inferred semantics; their Brisbane values were empty at audit time.

Complete latest responses live in `.local/qld-api/latest.json`, with retrieval and reference-cache timestamps. API tokens and request headers are never serialized. The source explorer exposes returned fields and their population counts. Deduplicated metro price events, including other grades and unavailability, accumulate in `api_price_events` with first-seen and last-seen times. The original collection CSV schema stays compatible. First-seen timestamps record when this integration saw an event, not when the retailer first published it.

`TransactionDateUtc` is UTC even without a timezone suffix. Dashboard responses now normalize that field to an explicit `Z` timestamp, preventing the browser from interpreting it as Brisbane local time. Site-modification dates remain labelled as API values.

Metadata failures fall back to the dated cached metadata or a visible unavailable lookup. A failed price fetch does not re-label old prices as a fresh fetch. The price API and public context have separate failure handling.

Source: [Fuel Prices Queensland subscriber API v1.6](https://www.fuelpricesqld.com.au/documents/FuelPricesQLDDirectAPI(OUT)v1.6.pdf).

## Connected free context

| Source | Data and use | Limits |
| --- | --- | --- |
| [AIP terminal gate prices](https://aip.com.au/pricing/terminal-gate-prices) | Brisbane wholesale ULP, GST-inclusive c/L; compare dated changes and same-date retail–wholesale gaps. | Public HTML table, not a guaranteed JSON API. Parser validates dates and columns. Current page exposes several observations; history accumulates locally. |
| [RBA exchange rates (RSS-CB feed)](https://www.rba.gov.au/rss/rss-cb-exchange-rates.xml) | USD per AUD; imported-fuel currency context. | Official machine-readable feed, parsed with the standard library. Carries one 4.00 pm observation per publication, so history accumulates locally rather than arriving in batches. Read as USD per AUD only while the feed states base AUD / target USD; a re-based feed is rejected rather than silently inverted. |
| [EIA US Gulf Coast conventional petrol spot](https://www.eia.gov/opendata/documentation.php) | USD per US gallon; refined-petrol cost pressure. | EIA v2 API, series `EER_EPMRU_PF4_RGC_DPG`, registered key in ignored `.env` (`EIA_API_KEY`). Daily on US business days with a publication lag of several days. **This is the US Gulf Coast market, not Singapore Mogas 95, and is never relabelled as such.** |
| [EIA Brent crude spot](https://www.eia.gov/opendata/documentation.php) | USD per barrel; broader crude-cost pressure. | EIA v2 API, series `RBRTE`, same key. Crude is not a substitute for a refined-petrol benchmark. |

All four are fetched at most every six hours; failures retry hourly and retain visibly dated data. Raw source HTML, XML and JSON bodies and fetch status are kept in `.local/context`. A missing API key is reported as `not_configured` rather than a provider error, and no unauthenticated request is attempted. Request parameters carrying a key are never persisted, returned by `/api/market-context`, or included in an error message; provider exception text is replaced with fixed wording because it can echo the request URL. `context_observations` preserves observation dates, retrieval dates, units and source IDs separately from the old experimental tables. It stores the latest revision for a source/date; it is not a complete vintage archive.

Only equal-date retail and wholesale values are paired. Retail is our metro median; wholesale is the publisher's company average. Their difference includes costs and margins and must not be labelled station profit. These sources are descriptive context only and do not alter forecasts or buying recommendations yet.

The [ACCC's fuel-price explanation](https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices) identifies Singapore Mogas 95 as the relevant refined-petrol benchmark, with currency and supply-chain lags affecting domestic prices. No reliable free daily Mogas API was verified. Brent is not an interchangeable substitute, and neither is the EIA Gulf Coast petrol series: it is a refined-petrol price from a different market, carried as a closer proxy than crude and labelled accordingly.

## Queensland monthly fuel history (official backfill)

**Connected 18 September 2026**, manually only. Source: [fuel price reporting](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2026) on data.qld.gov.au, resolved through the CKAN `package_show` action per year. Licence CC BY 4.0: **Queensland fuel price reporting, © State of Queensland, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)**. That attribution must appear wherever this series is shown.

Run by hand; nothing imports it on server start:

```
.venv\Scripts\python.exe -m qld_history_import --years 2025 2026
```

`--list` shows the resources without importing, `--match August` selects one month, `--db` writes somewhere other than `fuel_data.db`. Each monthly CSV is streamed to gitignored `.local/qld-history/`, hashed while streaming, parsed in pandas chunks, then **deleted**: the files are about 13 MB each and only the sha256, byte count and row counts are retained, in `import_sources`.

- **These are reported price-change events, not daily snapshots, and not observations this app collected.** They land in `reported_price_events` and never in `snapshots`. A backfilled day is never described as a local capture.
- Kept rows: `Site_State` QLD, `Fuel_Type` `Unleaded` (U91 as the publisher spells it), coordinates inside the configured Brisbane bounds, price within 80–350 c/L.
- `Price` is **tenths of a cent** (`2099` = 209.9 c/L). Raw `9999` means the retailer reported the grade unavailable; those rows are counted (`rows_unavailable`) and stored as an explicit unavailability marker, not dropped and not treated as a price.
- `TransactionDateutc` is UTC written `DD/MM/YYYY HH:MM`. It is parsed with that exact format and converted to Australia/Brisbane for daily grouping. It is never parsed with `format='mixed'`, which would silently read 04/08 as 8 April.
- `INSERT OR IGNORE` on `(site_id, fuel_type, reported_at_utc, price_cpl)` makes re-importing a file a no-op.

**As-of policy.** A station's last report holds until its next one and **expires 14 days after it** (`AS_OF_EXPIRY_DAYS`): day + 14 is still carried, day + 15 is not. An expired station leaves that day's median rather than being quoted at a stale price. An explicit unavailability report replaces the held price and expires the same way. Every reconstructed day is labelled `origin='reported'`.

**Publication lag — read before using this in any evaluation.** The monthly file is published about a month after the events it contains: the August 2026 resource carried a publication date of 2 September 2026. Nothing reconstructed here was knowable on the day it describes. `reported_series()` and `coverage()` therefore return `retrospective=True` alongside the data, and a point-in-time backtest must not treat these days as real-time observations.

**How the forecaster uses them (18 September 2026).** `forecasting.merge_history` places the reported series *under* the collected one and keeps a `provenance` label on every day; a collected day always wins a day both cover. Reported days may be selected on and scored against, because each day's value is built only from reports timestamped on or before it — but they never count as collection: `recent_consecutive_days` and staleness derive from collected days alone, and `forecast.provenance` reports how many selection and held-out origins depended on reported data. See `docs/forecast-evaluation-2026-09-18.md` for the resulting held-out comparison, which is a negative result.

`/api/observatory/coverage` reports, per day, raw event count, stations reporting, stations priced, stations carried forward and explicit unavailability, plus the provenance rows and a `reconciled` flag that checks the per-day event counts sum back to the raw imported rows.

First import verified on August 2026 alone: 92,244 rows read, 8,887 kept, 93 unavailable, 31 reconstructed days, reconciled. It was written to a scratch database, not to the tracked `fuel_data.db`.

## Map and news context (display only)

Added 28 September 2026. None of these enters a forecast, a buying recommendation or a route cost.

| Source | Shown as | Access and limits |
| --- | --- | --- |
| [QLDTraffic webcams](https://qldtraffic.qld.gov.au/more/Developers-and-Data/index.html) `/v1/webcams` | 📷 map icons; popup shows the latest still, refreshed each minute while open | Shared public key, served via local `/api/cameras` (the API sends no CORS header). List cached one hour; the last good list is kept on failure. Only images on `cameras.qldtraffic.qld.gov.au` are passed to the browser. |
| [QFD current bushfire incidents](https://www.qfd.qld.gov.au/) | 🔥 map layer: warning areas outlined, incidents as dots | Public GeoJSON on S3, via local `/api/bushfires` (no CORS). Ten-minute cache. |
| [BOM Queensland warnings](https://www.bom.gov.au/qld/warnings/) `IDZ00056` | List in the News panel | Public RSS, no geometry. Ten-minute cache. |
| Google News search RSS; [OilPrice.com](https://oilprice.com/rss/main) | News panel, three topics | Via local `/api/news`, 30-minute cache. Per-feed cap, duplicate titles and social-media sources removed. Headlines only; no sentiment score is derived. |
| [RainViewer](https://www.rainviewer.com/api.html) | 🌧 optional radar overlay | Browser fetch, no key. Free tiles stop at zoom 7 and are scaled up; frame refreshed every ten minutes while on. |
| [OpenStreetMap Nominatim](https://nominatim.org/release-docs/latest/api/Search/) | Place search on Enter | Browser request only when Enter is pressed, limited to Australia. The typed text is sent to OpenStreetMap. |

| [aisstream.io](https://aisstream.io) AIS | 🛳 live tankers and 🚢 other ships on the map; live tankers in Fuel security | Free key `AISSTREAM_API_KEY`. One server-side WebSocket for the Queensland coast (stdlib client, `ships.py`); vessel identities saved to `.local/ships` so tankers stay labelled across restarts; ships unheard for 3 h dropped. Coverage is patchy offshore. AIS ETA carries no year. Type 84 is mostly LNG/gas carriers. |
| [Maritime Safety Queensland Qships](https://qships.tmr.qld.gov.au/webx/) | Tanker arrivals due in 7 days and tankers at berth (Fuel security) | Keyless JSON behind the public page; undocumented, so it fails soft. Refreshed in the background every 30 min. |
| [Australian Petroleum Statistics](https://www.energy.gov.au/topics/energy-data/australian-petroleum-statistics) (DCCEEW) | Fuel security: month-end stocks and days of cover, IEA days, imports by product and country, refining, national and QLD sales | Monthly xlsx, ~2-month lag, CC BY 4.0. Newest file found by scraping the yearly page; checked twice a day. |
| [MSO weekly snapshot](https://www.dcceew.gov.au/energy/security/australias-fuel-security/minimum-stockholding-obligation/statistics) (DCCEEW) | Fuel security: stock held vs the legal minimum, weekly | xlsx overwritten weekly (Tuesday stock, Saturday publication); DCCEEW calls it temporary. |
| [Port of Brisbane trade statistics](https://www.portbris.com.au/operations-and-trade/trade-development) | Fuel security: crude and refined fuel imports, tonnes | Monthly xlsx, ~1-month lag. Tonnes to ML uses ~0.8 t/kL, an approximation. |

No public source reports Queensland fuel stocks or days of cover; the page says so rather than apportioning national stock.

Checked and not connected: OpenStreetMap Overpass for EV chargers (HTTP 406/504 from the public servers at the time).

Also derived from data already collected: the **price-jump signal** (share of stations 20 c/L or more above the median, see `docs/cycle-signal-evaluation-2026-09-28.md`), **per-grade medians** (diesel, E10, P95, P98) from the full API capture over the analysis area, and **price flags**: `stale` when a station's last report is over 30 days old, `suspect` when a fresh price is more than 60 c/L below the statewide median (below wholesale cost, so almost certainly an entry error). Flagged prices are marked and ranked last in lowest-price lists.

## Next sources, in order

1. **[openrouteservice](https://openrouteservice.org/plans/):** free Standard account/key with quotas; directions and matrices for extra road distance/time. More directly useful to fuel-spend decisions than adding loosely related economic feeds. Connected using the user-provided key in ignored `.env` (`ORS_API_KEY`). The map’s Road-cost lab calls the local `/api/routing/compare` endpoint on explicit Calculate only. It compares origin → station → destination against the direct journey, or a dedicated return trip against no driving. Displays extra distance, estimated driving time, fuel/time cost and effective purchase price. Coordinates and up to 64 routes are held in memory, reused for five minutes, never written to disk; only the provider receives route coordinates. No automatic tracking uploads. No live traffic, queue time or toll fees. Local limit: 30 upstream calls/minute; provider quotas also apply. This is a station comparison tool, not an automatic buying-plan recommendation.
2. **[QLDTraffic](https://qldtraffic.qld.gov.au/more/Developers-and-Data/index.html):** official GeoJSON events, public or registered API key, attribution required. Integrated `/v2/events` (including area alerts) through local `/api/traffic`. Both maps have an optional event overlay, event/time filters and full schedule/impact/source inspection. Complete successful feeds and rights are retained in `.local/traffic/latest.json`; `/api/traffic/raw` exposes the cached evidence. Display uses geometry-bound overlap with the Brisbane area, so broad area alerts can be included. Date windows do not establish active hours; recurrence descriptions, direction and provider advice are shown. Traffic events do not alter route geometry, ETA, fuel-cost arithmetic or price forecasts.

   The default key is the public key in [TMR specification v1.10](https://qldtraffic.qld.gov.au/media/moreDevelopers-and-Data/qldtraffic-website-api-specification-v1-10.pdf?lang=en-AU), globally limited to 100 requests/minute. Optional `QLD_TRAFFIC_API_KEY` overrides it with a registered key. Browser reads occur every minute while visible; all callers share at most one upstream attempt per five minutes in this server. Cooldown and previous successful response survive restart. Failed refreshes retain visibly stale data; before any success the panel says unavailable, never clear roads. Initial live checks on 18 September 2026 returned HTTP 429. The feed subsequently recovered: at 15:18 AEST it returned 532 events, including 62 with bounds overlapping the Brisbane area; the real overlay, event filter and closure popup were verified. Shared-quota availability remains intermittent. The `/v1/webcams` camera list is connected (see below); the flood-camera and rest-area endpoints refuse the shared key (HTTP 403) and are not connected. No device coordinates are sent to TMR.
3. **[US EIA API](https://www.eia.gov/opendata/documentation.php):** **connected 18 September 2026** with a user-supplied registered key held in ignored `.env`. Two daily spot series are read from `/v2/petroleum/pri/spt/data/`: `EER_EPMRU_PF4_RGC_DPG` (US Gulf Coast conventional regular petrol, $/gal) and `RBRTE` (Europe Brent, $/bbl). Verified live: both returned 60 recent observations with the latest dated 2026-09-15 against a fetch on 2026-09-18, so a three-day publication lag plus weekend gaps is normal and staleness is judged at eight days. Displayed as dated context only. Neither series is in the forecast or any buying recommendation, and promoting either requires the lagged-feature evaluation described below.
4. **Australian Petroleum Statistics:** **dropped as an acquisition target.** The publisher releases monthly documents with no API, so there is nothing to integrate on a schedule. Reinstate only if a machine-readable release appears.

Before promoting any extra input into a forecast, evaluate lagged features on matched dates, avoid future publication leakage, compare against persistence and existing models, and hold out later periods. Test each addition separately so more inputs do not merely increase apparent fit.

## Device location

Both maps provide an explicit **Use my location** control, backed by `navigator.geolocation.watchPosition`. Successful fixes update a blue marker and an accuracy circle. Follow mode can be toggled; dragging the map pauses following. Stop clears the watch and marker. Permission failures and stale fixes are labelled. In the buying workspace, device location also supplies the starting point for straight-line distance and radius filtering.

Device fixes stay in page memory and are not written to browser storage or sent to the local fuel API. The maps do remember their last view (centre and zoom, key `fuel.mapView` in this browser's localStorage) so they reopen where you left them; while following your location, that view is roughly where you were. Clear site data to remove it. On load, a map centres on the device only if location permission was already granted; it never prompts. Ordinary map tiles are still loaded from OpenStreetMap for the viewed area. Browser/device location permission and location services are required; desktops may return a broad network-derived location. No synthetic device positions are displayed.
