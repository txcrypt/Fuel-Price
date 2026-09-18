# Fuel hub: decisions worth building next

The product should answer three questions: **where do I buy, how much do I buy,
and when should I buy again?** Keep collection status and evidence visible so
people can distinguish an observed bargain from a speculative forecast.

## Delivered

- Interactive Brisbane U91 map, below-median filter, station search, favourites,
  manual starting point and radius filtering. Distances are straight-line; route
  distance is not inferred from them.
- A 50 L default profile with editable remaining fuel, consumption, driving and
  reserve. Settings belong to each browser, not a global shared driver profile.
- Equal-service comparison: fill now versus minimum bridge purchase and later
  purchase, covering the same everyday driving with equal ending fuel.
- Seven candidate wait dates, budget constraints, discount, additional travel
  fuel and optional time valuation. A date is suggested only when the model is
  validated on recent data and its upper historical-error price still saves money.
- Explicit user price scenarios, break-even future price, and rise/fall stress
  tests. A scenario is not a forecast and is only used at the selected date.
- Detour break-even calculator, priced using the alternative station. Both
  purchase volume and additional driving distance are explicit.

## Highest-value next steps

1. **Favourite-station price alerts.** Alert on an observed drop or a saved
   target price, with a minimum dollar saving for the person's intended purchase.
   Include stale-data suppression and duplicate-alert suppression. A local
   collector can detect changes while running; notification channels need a
   deliberate choice before anything is sent.
2. **Stations along a normal commute.** Rank by extra road distance and minutes,
   not distance from home. Compare fuel, toll and optional time costs. Let people
   keep routes local and explicitly open external directions when needed.
3. **A refuelling journal.** Record actual litres, price, odometer and station.
   Learn the vehicle's measured consumption and report realised savings against
   a clearly defined comparison, rather than crediting every predicted saving.
4. **Observed cycle changes.** Show the share of nearby stations raising prices,
   speed of the change and lagging cheaper stations. Label these observed signals;
   calibrate whether they predict a useful buy-now action before making claims.
5. **A discount comparison.** Compare net prices after applicable fuel vouchers,
   memberships or price locks, with caps, expiry, and any required spending.
   Users enter offers first; avoid presenting an expired or unverified deal.
6. **Household and shared plans.** Multiple named vehicle profiles, a plain-language
   shareable summary of the current station/price/assumptions, and collection
   freshness shown prominently. Decide hosting and privacy separately before
   opening this local server to other people over a network.

Prioritise alerts and commute-aware costs for immediate value. Keep collecting
continuous observations and evaluating forecasts; more elaborate models alone
cannot compensate for large gaps in the recorded data.

## Mapping implementation references

Leaflet 1.9.4 is vendored under static/vendor/leaflet with its licence:
https://leafletjs.com/examples/quick-start/

Street tiles are requested directly by the browser, with visible attribution,
normal browser caching, and no prefetch/offline download:
https://operations.osmfoundation.org/policies/tiles/
