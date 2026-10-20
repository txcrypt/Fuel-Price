/* Station discovery and personal cost comparisons. Settings stay in this browser. */
(() => {
  'use strict';
  const byId = id => document.getElementById(id);
  const form = byId('plan-form');
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => (
    {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]
  ));
  const money = value => value == null ? '—' : '$' + Number(value).toFixed(2);
  const FLAGS = {suspect:'<span class="price-flag" title="Far below every other station and below wholesale cost: probably an entry error. Check before driving there.">⚠ check</span>',stale:'<span class="price-flag" title="This station has not reported a price for over a month; it may have changed.">old report</span>'};
  const fixed = value => value == null ? '—' : Number(value).toFixed(1);
  const read = (key, fallback) => {
    try { return JSON.parse(localStorage.getItem(key)) ?? fallback; }
    catch { return fallback; }
  };
  const save = (key, value) => {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* Private browsing can deny storage. */ }
  };
  const savedFavourites = read('fuel.favourites', []);
  const favourites = new Set(Array.isArray(savedFavourites) ? savedFavourites.map(String) : []);
  const MAX_LABELS = 80;
  const markers = new Map();
  let dashboard, map, markerLayer, labelLayer, originMarker, origin = null, choosingOrigin = false;
  let selectedId = '', version = '', dirty = false, requestNumber = 0;
  let detourEdited = false;
  let deviceTracker, roadRouter, deviceFix, deviceOrigin=false, adopted=null, adoptedNote=null;

  // Extra travel adopted from a measured road route. Only distance and minutes are
  // carried in: the server prices them from this form's consumption and time value,
  // exactly once. Any change to the route or to these fields drops the provenance.
  const ADOPTION_FIELDS = ['extra_km', 'extra_minutes', 'consumption', 'hourly_value', 'station_id'];
  function showAdoption(text) {
    if (!adoptedNote) {
      adoptedNote = document.createElement('p');
      adoptedNote.className = 'method-note';
      adoptedNote.id = 'adopted-travel-note';
      form.elements.extra_minutes.closest('label').after(adoptedNote);
    }
    adoptedNote.textContent = text;
  }
  function adoptRoute(adoption) {
    if (!adoption?.adoptable) return;
    // Select the routed station first: that path can itself invalidate an adoption.
    if (adoption.station_id && String(adoption.station_id) !== selectedId) selectStation(adoption.station_id, false);
    adopted = adoption;
    form.elements.extra_km.value = adoption.extra_km;
    form.elements.extra_minutes.value = adoption.extra_minutes;
    if (adoption.consumption != null) form.elements.consumption.value = adoption.consumption;
    if (adoption.hourly_value != null) form.elements.hourly_value.value = adoption.hourly_value;
    showAdoption(adoption.basis + ' ' + adoption.applies_to + ' ' + adoption.trip);
    compare();
  }
  function dropAdoption(reason) {
    if (!adopted) return;
    adopted = null;
    showAdoption(reason);
  }
  function clearAdopted(reason) {
    dropAdoption(reason);
    form.elements.extra_km.value = 0;
    form.elements.extra_minutes.value = 0;
    dirty = true; ++requestNumber;
    byId('plan-result').innerHTML = '<h3>The adopted route no longer applies.</h3><p>Choose “Compare my options” to recalculate without it.</p>';
  }
  const profile = read('fuel.profile', {});
  if (profile && typeof profile === 'object') {
    for (const [name, value] of Object.entries(profile)) {
      if (form.elements[name] && name !== 'station_id') form.elements[name].value = value ?? '';
    }
    selectedId = String(profile.station_id || '');
    if (Object.keys(profile).length) byId('profile-note').textContent = 'Saved inputs from this browser. Check fuel remaining and daily driving before each new plan.';
  }

  // Carry a station selected in the observatory into this comparison.
  const linkedStation = new URLSearchParams(location.search).get('station');
  if (linkedStation) selectedId = linkedStation;

  function distance(a, b) {
    const radians = v => v * Math.PI / 180;
    const dLat = radians(b[0] - a[0]), dLon = radians(b[1] - a[1]);
    const h = Math.sin(dLat / 2) ** 2 + Math.cos(radians(a[0])) * Math.cos(radians(b[0])) * Math.sin(dLon / 2) ** 2;
    return 6371 * 2 * Math.asin(Math.min(1, Math.sqrt(h)));
  }

  function initialiseMap() {
    if (!window.L) {
      byId('fuel-map').textContent = 'Map library could not load. Use the station list below.';
      return;
    }
    map = L.map('fuel-map', {scrollWheelZoom: false, preferCanvas: true});
    if (window.FuelMap) FuelMap.enhance(map, {search: byId('map-search')}); else map.setView([-27.47, 153.02], 10);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap contributors</a>',
    }).on('tileerror', () => {
      byId('tile-status').textContent = 'Some street tiles could not load. Price markers and the station list still work; check your internet connection.';
    }).addTo(map);
    markerLayer = L.layerGroup().addTo(map);
    labelLayer = L.layerGroup().addTo(map);
    deviceTracker=window.FuelLocation?.attach(map,{container:byId('fuel-map').parentElement,
      onPosition(position) {
        deviceFix=position;
        if(position) {
          deviceOrigin=true;
          origin=[position.latitude,position.longitude];
          if(originMarker){originMarker.remove();originMarker=null;}
        } else if(deviceOrigin) {origin=null;deviceOrigin=false;}
        drawStations(Number(byId('map-radius').value) > 0);renderSelected();  // Position only changes the set under a radius filter.
      },
    });
    window.FuelTraffic?.attach(map,{container:byId('fuel-map').parentElement});
    roadRouter=window.FuelRouting?.attach(map,{container:byId('fuel-map').parentElement,
      getPosition:()=>deviceOrigin?deviceFix:origin?{latitude:origin[0],longitude:origin[1]}:null,
      onAdopt:adoptRoute, onSelectStation:id=>selectStation(id),
      // The route inputs changed, so the adopted numbers describe a journey nobody is
      // taking any more. Remove them rather than let them quietly price the wrong trip.
      onInvalidate(){if(adopted)clearAdopted('Route inputs changed, so the adopted extra travel was removed. Calculate and adopt again.');},
    });
    map.on('moveend', () => { drawLabels(); drawNearby(); });
    map.on('click', event => {
      if (!choosingOrigin) return;
      deviceTracker?.stop();
      origin = [event.latlng.lat, event.latlng.lng];
      choosingOrigin = false;
      byId('map-origin').textContent = 'Change starting point';
      byId('map-origin').setAttribute('aria-pressed', 'false');
      if (originMarker) originMarker.remove();
      originMarker = L.circleMarker(origin, {radius: 10, color: '#0b141d', fillColor: '#66b5ff', fillOpacity: 1}).addTo(map).bindTooltip('Your starting point');
      if (byId('map-radius').value === '0') byId('map-radius').value = '5';
      byId('map-instructions').textContent = 'Starting point set. Distances are straight-line estimates; use the detour calculator for actual extra driving.';
      drawStations();
      renderSelected();
    });
    new ResizeObserver(() => map.invalidateSize({pan: false})).observe(byId('fuel-map'));
  }

  function matchingStations() {
    if (!dashboard) return [];
    const query = byId('map-search').value.trim().toLowerCase();
    const filter = byId('map-filter').value;
    const radius = Number(byId('map-radius').value);
    return dashboard.stations.filter(station => {
      if (!Number.isFinite(station.latitude) || !Number.isFinite(station.longitude)) return false;
      if (query && !`${station.name} ${station.region}`.toLowerCase().includes(query)) return false;
      if (filter === 'favourites' && !favourites.has(String(station.site_id))) return false;
      if (filter === 'cheap' && station.price_cpl >= dashboard.summary.median) return false;
      return !(origin && radius) || distance(origin, [station.latitude, station.longitude]) <= radius;
    });
  }

  // Canvas markers keyed by site, so a selection restyles two markers instead of rebuilding
  // ~1,600 (a rebuild also destroyed the popup the click had just opened).
  function styleMarker(marker, id) {
    const selected = id === selectedId;
    marker.setRadius(selected ? 10 : 6);
    marker.setStyle({color: selected ? '#dce6ed' : '#0b141d', weight: selected ? 3 : 1.3});
    if (selected) marker.bringToFront();
  }

  // Permanent price labels only for stations in view, cheapest first, capped: labelling every
  // station in Queensland at street zoom froze the page for seconds.
  function drawLabels() {
    if (!labelLayer) return;
    labelLayer.clearLayers();
    if (map.getZoom() < 12) return;
    const view = map.getBounds().pad(.1);
    let shown = 0;
    for (const marker of markers.values()) {
      if (shown >= MAX_LABELS) break;
      if (!view.contains(marker.getLatLng())) continue;
      // A zero-size divIcon: unlike a tooltip it never measures itself, so no layout per label.
      L.marker(marker.getLatLng(), {interactive: false, keyboard: false,
        icon: L.divIcon({className: 'price-label', iconSize: [0, 0], html: `<span>${fixed(marker.station.price_cpl)}</span>`})}).addTo(labelLayer);
      shown++;
    }
  }

  function drawStations(rebuild = true) {
    if (!dashboard) return;
    const stations = matchingStations();
    byId('map-count').textContent = `${stations.length} matching stations`;
    byId('origin-label').textContent = origin ? 'Distances from your chosen point · straight-line' : 'No starting point set';
    if (markerLayer && rebuild) {
      markerLayer.clearLayers(); markers.clear();
      for (const station of stations) {
        const id = String(station.site_id);
        const marker = L.circleMarker([station.latitude, station.longitude], {
          // Flagged prices (stale or suspect) are grey: an entry error must not look like a bargain.
          fillColor: station.flag ? '#71899c' : station.price_cpl < dashboard.summary.median ? '#58dac7' : '#eab466', fillOpacity: .92,
        }).addTo(markerLayer);
        marker.station = station;
        styleMarker(marker, id);
        marker.bindTooltip(`${fixed(station.price_cpl)} c/L · ${escape(station.name)}`, {direction: 'top'});
        marker.bindPopup(`<strong>${escape(station.name)}</strong><p>${fixed(station.price_cpl)} c/L · U91 ${FLAGS[station.flag] || ''}</p><p>Selected for your buying planner below.</p>`);
        marker.on('click', () => selectStation(station.site_id, false));
        markers.set(id, marker);
      }
      drawLabels();
    } else {
      for (const [id, marker] of markers) styleMarker(marker, id);
    }
    drawNearby(stations);
  }

  // With no starting point the list follows the map, so it shows local prices rather than the
  // cheapest station anywhere in Queensland (collection covers the whole state).
  function drawNearby(stations = matchingStations()) {
    if (!dashboard) return;
    const favouritesOnly = byId('map-filter').value === 'favourites';
    const view = map && !origin && !favouritesOnly ? map.getBounds() : null;
    if (view) stations = stations.filter(s => view.contains([s.latitude, s.longitude]));
    if (!favouritesOnly) stations = [...stations].sort((a, b) => !!a.flag - !!b.flag || a.price_cpl - b.price_cpl);
    byId('nearby-title').textContent = favouritesOnly ? 'Your favourite stations' : origin ? 'Lowest matching prices' : 'Lowest prices on this map';
    byId('nearby-list').innerHTML = stations.slice(0, 8).map(station => {
      const km = origin ? distance(origin, [station.latitude, station.longitude]) : null;
      return `<button type="button" class="nearby-station ${String(station.site_id) === selectedId ? 'chosen' : ''}" data-station="${escape(station.site_id)}">
        <span>${favourites.has(String(station.site_id)) ? '★ ' : ''}${escape(station.name)} ${FLAGS[station.flag] || ''}</span>
        <strong>${fixed(station.price_cpl)} <small>c/L</small></strong>
        <small>${km == null ? escape(station.region) : km.toFixed(1) + ' km straight-line'}</small></button>`;
    }).join('') || '<p class="method-note">No matching stations. Widen the radius or change the filter. Select a station and save it to build your favourites.</p>';
  }

  function renderSelected() {
    const station = dashboard?.stations.find(s => String(s.site_id) === selectedId);
    roadRouter?.selectStation(station);
    if (!station) {
      byId('station-detail').innerHTML = '<div class="eyebrow">YOUR NEXT STOP</div><h3>Select a station</h3><p>Choose a station to use its current price in your buying plan and save it as a favourite.</p>';
      return;
    }
    const difference = dashboard.summary.median - station.price_cpl;
    const km = origin ? distance(origin, [station.latitude, station.longitude]) : null;
    const reported = station.reported_at ? new Date(station.reported_at.replace(/Z$/, '') + 'Z').toLocaleString('en-AU', {timeZone: 'Australia/Brisbane'}) : 'Unavailable';
    byId('station-detail').innerHTML = `<div class="eyebrow">SELECTED FOR YOUR PLAN</div><h3>${escape(station.name)}</h3>
      <div class="station-price">${fixed(station.price_cpl)} <small>c/L</small></div>
      <p>${Math.abs(difference).toFixed(1)} c/L ${difference >= 0 ? 'below' : 'above'} the market median${km == null ? '' : ' · ' + km.toFixed(1) + ' km straight-line from your point'}.</p>
      <p class="quiet">Last price report: ${escape(reported)}. A price can remain unchanged for days. ${dashboard.health.fresh ? 'Snapshot is fresh.' : 'Snapshot is stale; refresh before acting.'}</p>
      <button type="button" class="secondary" id="favourite-toggle">${favourites.has(selectedId) ? '★ Remove favourite' : '☆ Save favourite'}</button>
      <a class="primary map-plan-link" href="#buying-plan">Plan using this price ↓</a>
      <a class="text-link" href="https://www.google.com/maps/dir/?api=1&destination=${station.latitude},${station.longitude}&travelmode=driving" target="_blank" rel="noopener">Open directions in Google Maps ↗</a>
      <div class="station-history" id="station-history"></div>`;
    window.FuelInsights?.history(byId('station-history'), selectedId);
    byId('favourite-toggle').addEventListener('click', () => {
      if (favourites.has(selectedId)) favourites.delete(selectedId); else favourites.add(selectedId);
      save('fuel.favourites', [...favourites]);
      document.dispatchEvent(new CustomEvent('fuel:favourites'));   // the watchlist (insights.js) follows
      renderSelected(); drawStations();
    });
  }

  function selectStation(id, moveMap = true) {
    window.FuelArea?.show('station');
    selectedId = String(id);
    const station = dashboard.stations.find(s => String(s.site_id) === selectedId);
    if (!station) return;
    form.elements.station_id.value = selectedId;
    if (map && moveMap) map.setView([station.latitude, station.longitude], 13);
    renderSelected(); drawStations(false);
    byId('detour-cheap').value = station.price_cpl;
    detourEdited = true;
    calculateDetour();
    compare();
  }

  function getProfile() {
    const inputs = Object.fromEntries(new FormData(form));
    for (const name of Object.keys(inputs)) {
      if (name === 'station_id') inputs[name] = inputs[name] || null;
      else inputs[name] = inputs[name] === '' ? null : Number(inputs[name]);
    }
    if (adopted) inputs.extra_travel_basis = adopted.basis;
    return inputs;
  }

  async function compare(event) {
    if (event) event.preventDefault();
    if (!dashboard || !form.reportValidity()) return;
    const request = ++requestNumber;
    const inputs = getProfile();
    save('fuel.profile', inputs);
    byId('plan-error').hidden = true;
    byId('plan-result').innerHTML = '<p>Comparing the two options…</p>';
    try {
      const response = await fetch('/api/buying-plan', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(inputs), signal: AbortSignal.timeout(20000),
      });
      const result = await response.json();
      if (request !== requestNumber) return;
      if (!response.ok) {
        const detail = Array.isArray(result.detail) ? result.detail.map(d => d.msg).join(' ') : result.detail;
        throw new Error(detail || 'The calculation could not be completed.');
      }
      dirty = false;
      const saving = result.saving;
      byId('plan-result').innerHTML = `<div class="eyebrow">${escape(result.basis)}</div><h3>${escape(result.headline)}</h3><p>${escape(result.explanation)}</p>
        <div class="range-note">${result.range_km} km before your reserve · ${result.days_to_reserve == null ? 'No daily driving entered' : result.days_to_reserve + ' days at your entered usage'}</div>
        ${result.feasible ? `<div class="option-grid">
          <article><span>FILL NOW</span><h4>${money(result.fill_cost)}</h4><p>Buy ${fixed(result.fill_l)} L today.</p><small>${result.fill_within_budget ? 'Within today’s budget' : 'Above today’s budget'}</small></article>
          <article><span>${result.bridge_l > 0 ? 'TOP UP + BUY LATER' : 'BUY LATER'}</span><h4>${money(result.deferred_cost)}</h4><p>${fixed(result.bridge_l)} L now (${money(result.bridge_cost)}), then ${fixed(result.later_l)} L on ${escape(result.target_date)} (${money(result.later_cost)}).</p><small>${result.bridge_within_budget ? 'Today’s purchase fits budget' : 'Today’s purchase exceeds budget'}</small></article>
        </div><div class="saving-callout"><span>${saving > 0 ? 'Conditional saving from waiting' : saving < 0 ? 'Extra cost of waiting' : 'No cost difference'}</span><strong>${money(Math.abs(saving))}</strong></div>
        <p class="break-even">Waiting only pays if the later price is <strong>${result.break_even_cpl == null ? 'not applicable — nothing to defer' : 'below ' + result.break_even_cpl.toFixed(2) + ' c/L'}</strong>.</p>
        <p class="quiet">Uses ${fixed(result.price_now)} c/L now and ${fixed(result.price_later)} c/L later. Both finish with ${fixed(result.end_l)} L. Waiting includes ${fixed(result.extra_fuel_l)} L of extra travel fuel and ${money(result.time_cost)} for time.</p>
        <h4 class="sensitivity-title">What if prices move the other way?</h4><div class="sensitivity">${result.scenarios.map(s => `<div><span>${s.change > 0 ? '+' : ''}${s.change} c/L</span><strong>${s.saving >= 0 ? 'Save ' : 'Costs '}${money(Math.abs(s.saving))}</strong></div>`).join('')}</div><p class="quiet">These ±10 c/L cases are stress tests, not probability bounds.</p>` : ''}
        ${result.timing?.length ? `<div class="timing-options"><h4 class="sensitivity-title">Compare the next seven days</h4>
        <p class="quiet">${result.best_wait_days ? `Day ${result.best_wait_days} has the largest saving among plans that pass the historical-error check.` : 'No date has a sufficiently supported saving to recommend waiting.'} Select a day to compare it in detail.</p>
        <div class="timing-grid">${result.timing.map(t => `<button type="button" class="secondary ${t.selected ? 'chosen' : ''}" data-wait="${t.days}"><b>Day ${t.days}</b><span>${t.feasible ? fixed(t.bridge_l) + ' L now' : 'Too far'}</span><small>${t.feasible ? !t.budget_ok ? 'Over budget' : t.saving > 0 ? 'Save ' + money(t.saving) : t.saving < 0 ? '+' + money(-t.saving) : 'Same cost' : 'Refill sooner'}</small></button>`).join('')}</div></div>` : ''}
        <details><summary>Assumptions behind this comparison</summary><ul>${result.warnings.map(w => `<li>${escape(w)}</li>`).join('')}</ul></details>`;
    } catch (error) {
      if (request !== requestNumber) return;
      byId('plan-error').textContent = error.message;
      byId('plan-error').hidden = false;
      byId('plan-result').innerHTML = '<h3>Check the inputs before comparing.</h3><p>No current recommendation is available.</p>';
    }
  }

  function calculateDetour() {
    const usual = Number(byId('detour-usual').value), cheap = Number(byId('detour-cheap').value);
    const litres = Number(byId('detour-litres').value), km = Number(byId('detour-km').value);
    const consumption = Number(form.elements.consumption.value);
    if (![usual, cheap, litres, consumption].every(n => Number.isFinite(n) && n > 0) || !Number.isFinite(km) || km < 0) {
      byId('detour-result').textContent = 'Enter positive prices, litres and consumption, and a non-negative distance.';
      return;
    }
    const gross = (usual - cheap) * litres / 100;
    const travel = km * consumption / 100 * cheap / 100;
    const net = gross - travel;
    const maximumKm = Math.max(0, gross) / (consumption / 100 * cheap / 100);
    byId('detour-result').innerHTML = `<strong>${money(Math.abs(net))} ${net > 0 ? 'net fuel saving' : net < 0 ? 'more expensive' : 'difference'}</strong><span>${money(gross)} price saving − ${money(travel)} extra fuel.</span><span>Break-even extra driving: <b>${maximumKm.toFixed(1)} km total</b>, before time or tolls.</span>`;
  }

  form.addEventListener('submit', compare);
  byId('plan-result').addEventListener('click', event => {
    const button = event.target.closest('[data-wait]');
    if (button) { form.elements.wait_days.value = button.dataset.wait; compare(); }
  });
  form.addEventListener('input', event => {
    dirty = true;
    ++requestNumber;
    if (ADOPTION_FIELDS.includes(event.target.name)) {
      dropAdoption('Extra travel was edited by hand, so it is no longer backed by a measured road route.');
    }
    byId('plan-result').innerHTML = '<h3>Your inputs have changed.</h3><p>Choose “Compare my options” to update the quantities and costs.</p>';
    calculateDetour();
  });
  form.elements.station_id.addEventListener('change', () => {
    selectedId = form.elements.station_id.value;
    renderSelected(); drawStations(false);
  });
  byId('scenario-clear').addEventListener('click', () => { form.elements.scenario_change_cpl.value = ''; compare(); });
  byId('profile-reset').addEventListener('click', () => {
    dropAdoption('Inputs were reset, so no adopted road route applies.');
    form.reset(); selectedId = ''; save('fuel.profile', {}); dirty = true;
    byId('profile-note').textContent = 'Example values restored. Adjust fuel remaining and driving before acting.';
    drawStations(); renderSelected(); calculateDetour(); compare();
  });
  byId('nearby-list').addEventListener('click', event => {
    const button = event.target.closest('[data-station]');
    if (button) selectStation(button.dataset.station);
  });
  byId('map-search').addEventListener('input', drawStations);
  byId('map-filter').addEventListener('change', drawStations);
  byId('map-radius').addEventListener('change', () => {
    if (!origin && byId('map-radius').value !== '0') {
      byId('map-instructions').textContent = 'First choose “Set starting point”, then click the map. Radius filtering needs a starting point.';
    }
    drawStations();
  });
  byId('map-origin').addEventListener('click', () => {
    choosingOrigin = !choosingOrigin;
    byId('map-origin').setAttribute('aria-pressed', String(choosingOrigin));
    byId('map-instructions').textContent = choosingOrigin ? 'Click anywhere on the map to set your starting point. It stays in this page and is not saved.' : 'Starting-point selection cancelled.';
  });
  byId('map-reset').addEventListener('click', () => {
    deviceTracker?.stop();
    origin = null; choosingOrigin = false;
    if (originMarker) { originMarker.remove(); originMarker = null; }
    byId('map-origin').textContent = 'Set starting point';
    byId('map-origin').setAttribute('aria-pressed', 'false');
    byId('map-radius').value = '0'; byId('map-search').value = ''; byId('map-filter').value = 'all';
    byId('map-instructions').textContent = 'Pick a station to use its price in your plan. Set a starting point to compare nearby stations.';
    if (map) map.setView([-27.47, 153.02], 10);
    drawStations(); renderSelected();
  });
  for (const id of ['detour-usual', 'detour-cheap', 'detour-litres', 'detour-km']) {
    byId(id).addEventListener('input', () => { detourEdited = true; calculateDetour(); });
  }
  initialiseMap();
  window.FuelDecisions = {
    select: id => selectStation(id),
    update(payload) {
      dashboard = payload;
      const newVersion = `${payload.latest}:${payload.health.fresh}`;
      if (newVersion === version) return;
      version = newVersion;
      const previous = selectedId;
      form.elements.station_id.innerHTML = '<option value="">Brisbane market median (not a station)</option>' + payload.stations.map(s => `<option value="${escape(s.site_id)}">${escape(s.name)} · ${fixed(s.price_cpl)} c/L</option>`).join('');
      form.elements.station_id.value = previous;
      if (!form.elements.station_id.value) selectedId = '';
      if (!detourEdited) {
        byId('detour-usual').value = payload.summary.median ?? '';
        byId('detour-cheap').value = payload.summary.minimum ?? '';
      }
      drawStations(); renderSelected(); calculateDetour();
      if (!dirty) compare();
    },
  };
})();
