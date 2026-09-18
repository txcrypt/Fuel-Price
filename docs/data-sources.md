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
| [RBA exchange rates](https://www.rba.gov.au/statistics/frequency/exchange-rates.html) | USD per AUD; imported-fuel currency context. | Public table, normally business-day publication. Not a tick-by-tick FX quote. |

Both are fetched at most every six hours; failures retry hourly and retain visibly dated data. Raw source HTML and fetch status are kept in `.local/context`. `context_observations` preserves observation dates, retrieval dates, units and source IDs separately from the old experimental tables. It stores the latest revision for a source/date; it is not a complete vintage archive.

Only equal-date retail and wholesale values are paired. Retail is our metro median; wholesale is the publisher's company average. Their difference includes costs and margins and must not be labelled station profit. These sources are descriptive context only and do not alter forecasts or buying recommendations yet.

The [ACCC's fuel-price explanation](https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices) identifies Singapore Mogas 95 as the relevant refined-petrol benchmark, with currency and supply-chain lags affecting domestic prices. No reliable free daily Mogas API was verified. Brent is not an interchangeable substitute.

## Next sources, in order

1. **[Queensland monthly fuel history](https://www.data.qld.gov.au/dataset/fuel-price-reporting-2026):** free CSV/CKAN resources, CC BY 4.0. Most directly useful for the historical gaps. The 2026 catalogue currently runs January–August. These are report events: import separately, validate units and station IDs, reconstruct last-known state with explicit missing/availability handling, and retain the release date. Do not describe reconstructed days as locally collected snapshots. Not imported by this change.
2. **[openrouteservice](https://openrouteservice.org/plans/):** free Standard account/key with quotas; directions and matrices for extra road distance/time. More directly useful to fuel-spend decisions than adding loosely related economic feeds. Connected using the user-provided key in ignored `.env` (`ORS_API_KEY`). The map’s Road-cost lab calls the local `/api/routing/compare` endpoint on explicit Calculate only. It compares origin → station → destination against the direct journey, or a dedicated return trip against no driving. Displays extra distance, estimated driving time, fuel/time cost and effective purchase price. Coordinates and up to 64 routes are held in memory, reused for five minutes, never written to disk; only the provider receives route coordinates. No automatic tracking uploads. No live traffic, queue time or toll fees. Local limit: 30 upstream calls/minute; provider quotas also apply. This is a station comparison tool, not an automatic buying-plan recommendation.
3. **[QLDTraffic](https://qldtraffic.qld.gov.au/more/Developers-and-Data/index.html):** official GeoJSON events, public or registered API key, attribution required. Integrated `/v2/events` (including area alerts) through local `/api/traffic`. Both maps have an optional event overlay, event/time filters and full schedule/impact/source inspection. Complete successful feeds and rights are retained in `.local/traffic/latest.json`; `/api/traffic/raw` exposes the cached evidence. Display uses geometry-bound overlap with the Brisbane area, so broad area alerts can be included. Date windows do not establish active hours; recurrence descriptions, direction and provider advice are shown. Traffic events do not alter route geometry, ETA, fuel-cost arithmetic or price forecasts.

   The default key is the public key in [TMR specification v1.10](https://qldtraffic.qld.gov.au/media/moreDevelopers-and-Data/qldtraffic-website-api-specification-v1-10.pdf?lang=en-AU), globally limited to 100 requests/minute. Optional `QLD_TRAFFIC_API_KEY` overrides it with a registered key. Browser reads occur every minute while visible; all callers share at most one upstream attempt per five minutes in this server. Cooldown and previous successful response survive restart. Failed refreshes retain visibly stale data; before any success the panel says unavailable, never clear roads. Initial live checks on 18 September 2026 returned HTTP 429. The feed subsequently recovered: at 15:18 AEST it returned 532 events, including 62 with bounds overlapping the Brisbane area; the real overlay, event filter and closure popup were verified. Shared-quota availability remains intermittent. Cameras/flood-camera endpoints are not connected. No device coordinates are sent to TMR.
4. **[US EIA API](https://www.eia.gov/opendata/documentation.php):** free registration key, Brent and energy series. Useful as a broader cost-pressure covariate; lower priority than Australian wholesale/refined-fuel data. Not connected; no account created.
5. **[Australian Petroleum Statistics](https://www.energy.gov.au/energy-data/australian-petroleum-statistics):** free monthly extracts for stocks, imports and refinery activity. Useful for slow supply context, with release lag. Avoid presenting monthly observations as current local stock. Not connected.

Before promoting any extra input into a forecast, evaluate lagged features on matched dates, avoid future publication leakage, compare against persistence and existing models, and hold out later periods. Test each addition separately so more inputs do not merely increase apparent fit.

## Device location

Both maps provide an explicit **Use my location** control, backed by `navigator.geolocation.watchPosition`. Successful fixes update a blue marker and an accuracy circle. Follow mode can be toggled; dragging the map pauses following. Stop clears the watch and marker. Permission failures and stale fixes are labelled. In the buying workspace, device location also supplies the starting point for straight-line distance and radius filtering.

Coordinates stay in page memory and are not written to browser storage or sent to the local fuel API. Ordinary map tiles are still loaded from OpenStreetMap for the viewed area. Browser/device location permission and location services are required; desktops may return a broad network-derived location. No synthetic device positions are displayed.
