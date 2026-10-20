/* Area tab beside each map: a searched place's fuel prices, roads, weather, air, bushfires and
   public-interest local news. Opens on a map place search (map-extras.js 'fuel:place'). */
(() => {
  'use strict';
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const num = (v, d = 1) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toFixed(d);
  const signed = v => `${v > 0 ? '+' : v < 0 ? '−' : '±'}${Math.abs(v).toFixed(1)}`;
  // Open-Meteo returns local wall-clock strings for the place (timezone=auto): format them, never re-zone them.
  const time = local => { if (!local) return '—'; const [h, m] = local.slice(11, 16).split(':').map(Number); return `${h % 12 || 12}:${String(m).padStart(2, '0')} ${h < 12 ? 'am' : 'pm'}`; };
  const ago = h => h < 1 ? `${Math.max(1, Math.round(h * 60))} min ago` : h < 48 ? `${Math.round(h)} h ago` : `${Math.round(h / 24)} d ago`;
  const band = (v, steps) => steps.find(([max]) => v <= max)?.[1] ?? steps.at(-1)[1];
  // WMO weather codes as Open-Meteo returns them.
  const SKY = {0: 'Clear', 1: 'Mainly clear', 2: 'Partly cloudy', 3: 'Overcast', 45: 'Fog', 48: 'Fog', 51: 'Light drizzle', 53: 'Drizzle',
    55: 'Heavy drizzle', 61: 'Light rain', 63: 'Rain', 65: 'Heavy rain', 80: 'Showers', 81: 'Heavy showers', 82: 'Violent showers',
    95: 'Thunderstorm', 96: 'Thunderstorm, hail', 99: 'Severe thunderstorm, hail'};
  const UV = [[2, 'low'], [5, 'moderate'], [7, 'high'], [10, 'very high'], [99, 'extreme']];
  const AQI = [[50, 'good'], [100, 'moderate'], [150, 'unhealthy for sensitive groups'], [200, 'unhealthy'], [300, 'very unhealthy'], [999, 'hazardous']];

  const aside = document.querySelector('aside.panel.inspector');
  if (!aside) return;
  const stationBody = aside.querySelector('.panel-body');
  const head = aside.querySelector('.panel-head');
  const heading = head.querySelector('h2'), title = heading.textContent;
  heading.classList.add('visually-hidden');   // Still the panel's heading for screen readers; the tabs show it visually.
  stationBody.id ||= 'side-station';
  stationBody.setAttribute('role', 'tabpanel'); stationBody.setAttribute('aria-labelledby', 'side-tab-station');
  heading.insertAdjacentHTML('afterend', `<div class="area-tabs" role="tablist" aria-label="${esc(title)} or area summary">
    <button type="button" role="tab" id="side-tab-station" aria-controls="${stationBody.id}" aria-selected="true" tabindex="0" data-tab="station">${esc(title)}</button>
    <button type="button" role="tab" id="side-tab-area" aria-controls="side-area" aria-selected="false" tabindex="-1" data-tab="area">Area</button></div>`);
  const body = document.createElement('div');
  body.className = 'panel-body area-body'; body.id = 'side-area'; body.hidden = true;
  body.setAttribute('role', 'tabpanel'); body.setAttribute('aria-labelledby', 'side-tab-area'); body.setAttribute('aria-live', 'polite');
  body.innerHTML = `<p class="kicker">AREA SUMMARY</p><h3>Search a place</h3><p class="muted">Type a city or suburb in the map search and press Enter,
    or summarise whatever the map shows now.</p><button type="button" class="secondary" data-view>Summarise this map view</button>`;
  stationBody.after(body);

  function show(tab, focus = false) {
    head.querySelectorAll('[role=tab]').forEach(b => {
      const on = b.dataset.tab === tab;
      b.setAttribute('aria-selected', String(on)); b.tabIndex = on ? 0 : -1;
      if (on && focus) b.focus();
    });
    stationBody.hidden = tab !== 'station'; body.hidden = tab !== 'area';
  }
  head.addEventListener('click', e => { const b = e.target.closest('[data-tab]'); if (b) show(b.dataset.tab); });
  head.addEventListener('keydown', e => {   // Arrow keys move between the two tabs (WAI-ARIA tabs pattern).
    if (!e.target.closest('[role=tab]') || !['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) return;
    e.preventDefault();
    show(stationBody.hidden ? 'station' : 'area', true);
  });

  const json = async (url, options) => { const r = await fetch(url, options); if (!r.ok) throw new Error(r.status); return r.json(); };
  let request = 0;
  async function load(place) {
    const run = ++request;
    show('area');
    body.scrollTop = 0;   // The panel scrolls inside the map row; a new place starts at its top.
    body.setAttribute('aria-busy', 'true');
    body.innerHTML = `<p class="kicker">AREA SUMMARY</p><h3>${esc(place.name)}</h3><p class="muted">Gathering prices, roads, weather and local news…</p>`;
    const b = place.bbox, at = `latitude=${place.lat}&longitude=${place.lon}&timezone=auto`;
    const wikiTitle = (place.wikipedia || '').replace(/^en:/, '') || (place.population ? place.name : '');
    const [summary, weather, air, wiki] = await Promise.allSettled([
      json(`/api/area?${new URLSearchParams({...b, name: place.news === false ? '' : place.name, region: place.region || ''})}`),
      json(`https://api.open-meteo.com/v1/forecast?${at}&forecast_days=1&current=temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m,wind_gusts_10m&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_max,precipitation_sum,uv_index_max,sunrise,sunset`),
      json(`https://air-quality-api.open-meteo.com/v1/air-quality?${at}&current=us_aqi,pm2_5`),
      wikiTitle ? json(`https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(wikiTitle)}`) : Promise.reject(),
    ]);
    if (run !== request) return;   // A newer search replaced this one.
    try {
      body.innerHTML = render(place, ...[summary, weather, air, wiki].map(r => r.status === 'fulfilled' ? r.value : null),
                              summary.status === 'rejected' ? summary.reason?.message : null);
    } catch {
      body.innerHTML = `<p class="kicker">AREA SUMMARY</p><h3>${esc(place.name)}</h3><p class="muted">This summary could not be shown. Try the search again.</p>`;
    }
    body.removeAttribute('aria-busy');
    offerChip(place);
  }

  // Phones (panel below the map) and full-screen maps: a chip on the map says the summary is ready.
  let chip = null;
  function offerChip(place) {
    const map = window.FuelMap?.map;
    chip?.remove(); chip = null;
    if (!map) return;
    const box = map.getContainer(), full = box.classList.contains('map-full');
    if (!full && aside.getBoundingClientRect().top < box.getBoundingClientRect().bottom - 1) return;   // side by side: already visible
    chip = L.control({position: 'bottomleft'});
    chip.onAdd = () => {
      const b = L.DomUtil.create('button', 'area-chip');
      b.type = 'button'; b.textContent = `📊 ${place.name} summary ↓`;
      L.DomEvent.disableClickPropagation(b);
      b.onclick = () => { window.FuelMap.exitFull?.(); chip?.remove(); chip = null; show('area'); aside.scrollIntoView({behavior: 'smooth', block: 'start'}); };
      return b;
    };
    chip.addTo(map);
  }

  function render(place, s, w, air, wiki, failure) {
    const out = [`<p class="kicker">AREA SUMMARY · ${esc((place.type || 'place').toUpperCase())}</p><h3>${esc(place.name)}</h3>
      <p class="muted">${esc(place.label || '')}</p>`];
    const facts = [];
    if (place.population) facts.push(`Population ${Number(place.population).toLocaleString('en-AU')}${place.populationYear ? ` (${esc(place.populationYear)})` : ''} · OpenStreetMap`);
    if (wiki?.type === 'standard' && wiki.extract) facts.push(`${esc(wiki.extract.split(/(?<=\.)\s/).slice(0, 2).join(' '))} <a href="${esc(wiki.content_urls?.desktop?.page)}" target="_blank" rel="noopener">Wikipedia ↗</a>`);
    if (facts.length) out.push(`<p class="area-facts">${facts.join('<br>')}</p>`);

    if (w?.current) {
      const c = w.current, d = w.daily;
      const uv = d.uv_index_max?.[0], aqi = air?.current?.us_aqi;
      out.push(section('WEATHER · NOW AND TODAY', `<dl class="area-stats">
        <dt>Now</dt><dd>${num(c.temperature_2m)}°C · ${esc(SKY[c.weather_code] || 'Code ' + c.weather_code)}<small>feels ${num(c.apparent_temperature)}°C · humidity ${num(c.relative_humidity_2m, 0)}%</small></dd>
        <dt>Today</dt><dd>${num(d.temperature_2m_min[0])}–${num(d.temperature_2m_max[0])}°C<small>rain chance ${num(d.precipitation_probability_max[0], 0)}% · ${num(d.precipitation_sum[0])} mm expected</small></dd>
        <dt>Wind</dt><dd>${num(c.wind_speed_10m, 0)} km/h<small>gusts ${num(c.wind_gusts_10m, 0)} km/h</small></dd>
        <dt>UV · air</dt><dd>UV ${num(uv, 0)} ${uv == null ? '' : band(uv, UV)}<small>${aqi == null ? 'Air quality unavailable' : `Air quality ${num(aqi, 0)} (US AQI, ${band(aqi, AQI)}) · PM2.5 ${num(air.current.pm2_5)} µg/m³`}</small></dd>
        <dt>Daylight</dt><dd>${time(d.sunrise[0])} – ${time(d.sunset[0])}<small>local time${w.timezone_abbreviation ? ' (' + esc(w.timezone_abbreviation) + ')' : ''}</small></dd></dl>`, 'Open-Meteo model data for the place centre, not an official BOM observation.'));
    }

    if (s) {
      const f = s.fuel, ref = s.reference;
      out.push(section('FUEL · UNLEADED 91', f.median == null
        ? `<p class="muted">${f.stations ? `${f.stations} stations here, all with old or suspect prices.` : 'No stations in this area.'}</p>`
        : `<dl class="area-stats">
          <dt>Median</dt><dd>${num(f.median)} c/L${ref?.median != null ? `<small>${signed(f.median - ref.median)} vs ${esc(ref.area)} · mean ${num(f.mean)}</small>` : ''}</dd>
          <dt>Lowest</dt><dd>${num(f.lowest.price)} c/L<small>${esc(f.lowest.name)} · ${f.within_5c_of_lowest} station${f.within_5c_of_lowest === 1 ? '' : 's'} within 5 c/L of it</small></dd>
          <dt>Highest</dt><dd>${num(f.highest.price)} c/L<small>${esc(f.highest.name)} · spread ${num(f.spread)} c/L = $${num(f.spread * .5, 2)} on 50 L</small></dd>
          <dt>Stations</dt><dd>${f.stations}<small>${f.flagged ? `${f.flagged} with old or suspect prices left out of the ranking` : 'all with current prices'}</small></dd></dl>
          ${s.grades.length ? `<p class="area-grades">${s.grades.filter(g => g.fuel !== 'Unleaded').map(g => `<span>${esc(g.fuel.replace('Premium Unleaded ', 'P').replace('Premium Diesel', 'Prem. diesel').replace('e10', 'E10'))} <b>${num(g.median)}</b></span>`).join('')}</p>` : ''}`,
        'Last reported price per station in this area.'));

      const r = s.roads;
      out.push(section('ROADS · QLDTRAFFIC', `<p>${r.current} current event${r.current === 1 ? '' : 's'}${r.scheduled ? ` · ${r.scheduled} scheduled later` : ''}${Object.keys(r.by_type).length ? `: ${Object.entries(r.by_type).map(([t, n]) => `${n} ${esc(t.toLowerCase())}`).join(', ')}` : ''}.</p>
        ${r.events.length ? `<ul class="area-list">${r.events.map(e => `<li><b>${esc(e.road)}</b>${e.locality ? ` · ${esc(e.locality)}` : ''}<small>${esc(e.type)} · ${esc(e.impact || '')}${e.delay ? ' · ' + esc(e.delay) : ''}</small></li>`).join('')}</ul>` : ''}
        <p>${s.cameras.length} road camera${s.cameras.length === 1 ? '' : 's'} here${s.cameras.length ? ` <button type="button" class="text-button" data-cameras>show them</button>` : ''}</p>`,
        'Closures first. No listed event does not mean a road is clear.'));

      const fires = Object.entries(s.bushfires);
      if (fires.length || s.warnings.length) out.push(section('WARNINGS', `${fires.length ? `<p>🔥 Bushfire incidents: ${fires.map(([l, n]) => `${n} ${esc(l)}`).join(', ')}</p>` : ''}
        ${s.warnings.map(x => `<p>⚠ <a href="${esc(x.link)}" target="_blank" rel="noopener">${esc(x.title)}</a></p>`).join('')}`, 'Queensland Fire Department and Bureau of Meteorology.'));

      if (place.news !== false) out.push(section('LOCAL NEWS · PUBLIC-INTEREST', s.news.length
        ? `<ul class="area-list area-news">${s.news.map(n => `<li><span class="news-tag">${esc(n.tag)}</span>${n.breaking ? '<span class="news-tag breaking">NEW</span>' : ''}
            <a href="${esc(n.link)}" target="_blank" rel="noopener noreferrer">${esc(n.title)}</a>
            <small>${esc(n.source)}${n.outlets.length > 1 ? ` + ${n.outlets.length - 1} other outlet${n.outlets.length > 2 ? 's' : ''}` : ''} · ${ago(n.hours_old)}</small></li>`).join('')}</ul>`
        : '<p class="muted">No public-interest local stories in the last week.</p>',
        'Last 7 days, newest three days first. Only crime, emergencies, health, government, roads and cost of living; one story per event, with how many outlets covered it. Not verified here.'));
      out.push(`<p class="muted area-note">${esc(s.note)}</p>`);
    } else out.push(`<p class="muted">${failure === '404' ? 'This dashboard server predates the area summary. Restart it to load prices, roads and news here.'
      : failure === '422' ? 'That area is outside Australia, so there are no prices, roads or news for it.'
      : `Prices, roads and news are unavailable right now${failure ? ` (HTTP ${esc(failure)})` : ''}. Weather above still works.`}</p>`);
    out.push('<button type="button" class="secondary" data-view>Summarise this map view instead</button>');
    return out.join('');
  }
  const section = (label, html, note) => `<section class="area-section"><p class="kicker">${label}</p>${html}${note ? `<p class="quiet area-source">${esc(note)}</p>` : ''}</section>`;

  body.addEventListener('click', async e => {
    const map = window.FuelMap?.map;
    if (e.target.closest('[data-cameras]') && map && current) {
      const sel = map.getContainer().querySelector('.map-view-control select');
      map.fitBounds([[current.bbox.south, current.bbox.west], [current.bbox.north, current.bbox.east]]);
      if (sel) { sel.value = 'cameras'; sel.dispatchEvent(new Event('change')); map.fitBounds([[current.bbox.south, current.bbox.west], [current.bbox.north, current.bbox.east]]); }
    }
    if (e.target.closest('[data-view]') && map) {
      const token = ++request, v = map.getBounds(), c = map.getCenter();
      // The view centre is sent to OpenStreetMap only on this click, to name the place for its news.
      let name = 'This map view', label = '', news = false, region = '';
      // Only a city-scale view has a meaningful name: zoomed out, the centre lands in whatever state it hits.
      if (map.getZoom() >= 9) try {
        const r = await json(`https://nominatim.openstreetmap.org/reverse?${new URLSearchParams({lat: c.lat, lon: c.lng, zoom: 10, format: 'jsonv2'})}`);
        const a = r.address || {};
        const town = a.city || a.town || a.suburb || a.village || r.name;
        if (town) { name = town; label = r.display_name; news = true; region = a.state || ''; }
      } catch { /* Unnamed view: statistics only. */ }
      if (token !== request) return;   // A search happened while the view was being named.
      current = {name, label, news, region, type: 'map view', lat: c.lat, lon: c.lng,
        bbox: {south: v.getSouth(), west: v.getWest(), north: v.getNorth(), east: v.getEast()}};
      load(current);
    }
  });
  let current = null;
  document.addEventListener('fuel:place', e => { current = e.detail; load(current); });
  window.FuelArea = {show};
})();
