/* Shared map behaviour: pinch zoom, a start view tailored to this browser, place search,
   and official QLDTraffic road cameras. The last map view (not a device fix) is kept in
   this browser's localStorage only. */
(() => {
  'use strict';
  const HOME = [-27.47, 153.03], KEY = 'fuel.mapView';
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

  // Touchpad pinch arrives as ctrl+wheel: zoom on that, let plain two-finger scroll keep scrolling the page.
  function pinchZoom(map) {
    map.scrollWheelZoom.enable();
    map.getContainer().addEventListener('wheel', e => { if (!e.ctrlKey) e.stopImmediatePropagation(); }, {capture: true});
  }

  // Device location if already allowed (never prompts), else the last view here, else Brisbane.
  function startView(map) {
    let saved = null;
    try { saved = JSON.parse(localStorage.getItem(KEY)); } catch {}
    if (saved && [saved.lat, saved.lng, saved.zoom].every(Number.isFinite)) map.setView([saved.lat, saved.lng], saved.zoom);
    else map.setView(HOME, 11);
    map.on('moveend', () => {
      const c = map.getCenter();
      try { localStorage.setItem(KEY, JSON.stringify({lat: c.lat, lng: c.lng, zoom: map.getZoom()})); } catch {}
    });
    navigator.permissions?.query({name: 'geolocation'}).then(p => {
      if (p.state === 'granted') navigator.geolocation.getCurrentPosition(
        pos => map.setView([pos.coords.latitude, pos.coords.longitude], 13), () => {}, {maximumAge: 600000, timeout: 10000});
    }).catch(() => {});
  }

  // Enter looks the text up as an Australian place (OpenStreetMap Nominatim) and frames it.
  function placeSearch(map, input) {
    const hint = input.placeholder;
    let searches = 0;
    input.addEventListener('input', () => { input.setCustomValidity(''); if (input.value) input.placeholder = hint; });
    input.addEventListener('keydown', async e => {
      const q = input.value.trim();
      if (e.key !== 'Enter' || !q) return;
      e.preventDefault();
      const run = ++searches;
      let hit;
      try {
        const r = await fetch('https://nominatim.openstreetmap.org/search?' + new URLSearchParams({q, countrycodes: 'au', format: 'jsonv2', limit: 1, extratags: 1, addressdetails: 1}));
        [hit] = await r.json();
      } catch {}
      if (run !== searches) return;   // Enter pressed again: only the newest search moves the map.
      if (!hit) { input.setCustomValidity(`No Australian place found for “${q}”. Typing still filters stations by name.`); input.reportValidity(); return; }
      const [s, n, w, east] = hit.boundingbox.map(Number);
      map.fitBounds([[s, w], [n, east]], {maxZoom: 14});
      // Clear the name filter so every station in that area shows.
      input.value = '';
      input.dispatchEvent(new Event('input'));
      input.placeholder = `Showing ${hit.name || q} · type to filter stations`;
      // The Area tab (area.js) builds the place's summary from this.
      document.dispatchEvent(new CustomEvent('fuel:place', {detail: {name: hit.name || q, label: hit.display_name,
        bbox: {south: s, west: w, north: n, east}, lat: Number(hit.lat), lon: Number(hit.lon), type: hit.addresstype,
        population: hit.extratags?.population, populationYear: hit.extratags?.['population:date'], wikipedia: hit.extratags?.wikipedia,
        region: hit.address?.state || ''}}));
    });
  }

  // Official TMR road cameras, the public "live footage" for Queensland roads.
  // Cameras sit in their own pane so "Cameras only" can hide every other pane with one class.
  const STYLE = `.camera-icon{display:grid;place-items:center;font-size:13px;background:#0b141d;border:2px solid #b48cff;border-radius:50%}
    .map-view-control select{font:inherit;padding:4px 6px;background:#0b141d;color:#dce6ed;border:1px solid #b48cff;border-radius:4px}
    .cameras-only :is(.leaflet-overlay-pane,.leaflet-shadow-pane,.leaflet-marker-pane,.leaflet-tooltip-pane,.leaflet-heat-pane,.leaflet-supply-pane){display:none}
    .cameras-far .leaflet-cameras-pane{display:none}
    .map-view-control label{display:block;margin-top:4px;padding:3px 6px;background:#0b141d;color:#dce6ed;border:1px solid #f0883e;border-radius:4px;font-size:12px}`;

  // RainViewer's latest radar frame. Its free tiles stop at zoom 7, so Leaflet scales them up.
  function radar(map, box) {
    let layer = null;
    box.insertAdjacentHTML('beforeend', '<label><input type="checkbox"> 🌧 Rain radar</label>');
    const label = box.lastChild, input = label.firstChild;
    input.onchange = async () => {
      if (!input.checked) { layer?.remove(); return; }
      try {
        const d = await (await fetch('https://api.rainviewer.com/public/weather-maps.json')).json();
        const frame = d.radar.past.at(-1);
        layer?.remove();
        layer = L.tileLayer(`${d.host}${frame.path}/256/{z}/{x}/{y}/2/1_1.png`, {maxNativeZoom: 7, maxZoom: 19, opacity: .6, zIndex: 5,
          attribution: `Radar <a href="https://www.rainviewer.com/" target="_blank" rel="noopener">RainViewer</a> ${new Date(frame.time * 1000).toLocaleTimeString('en-AU', {hour: '2-digit', minute: '2-digit'})}`}).addTo(map);
      } catch { input.checked = false; label.lastChild.textContent = ' 🌧 Radar unavailable'; }
    };
    setInterval(() => { if (input.checked) input.onchange(); }, 10 * 60 * 1000);   // New frames every 10 min.
  }

  // Price heat: each grid cell takes the distance-weighted price of stations within ~3 km, coloured
  // against the prices in view (cyan cheaper, red dearer). A low-resolution image that Leaflet
  // stretches over the view, so the browser smooths it; redrawn after every move.
  let stations = [];
  const HEAT_KM = 3, CELL_PX = 8;
  const RAMP = [[88, 218, 199], [234, 180, 102], [246, 126, 131]];
  function rampColour(t) {
    const x = Math.max(0, Math.min(1, t)) * 2, i = Math.min(1, Math.floor(x)), f = x - i;
    return RAMP[i].map((c, k) => Math.round(c + (RAMP[i + 1][k] - c) * f));
  }
  function heat(map, box) {
    let overlay = null, on = false;
    map.createPane('heat').style.zIndex = 350;   // above the street map (200), under station dots (400)
    box.insertAdjacentHTML('beforeend', '<label title="Cyan: cheaper than the rest of this view. Red: dearer. Old or suspect prices are left out."><input type="checkbox"> 🌡 Price heat</label>');
    const input = box.lastChild.firstChild;
    const draw = () => {
      overlay?.remove(); overlay = null;
      if (!on) return;
      const size = map.getSize(), cols = Math.ceil(size.x / CELL_PX), rows = Math.ceil(size.y / CELL_PX);
      const metresPerPx = 40075016.686 * Math.abs(Math.cos(map.getCenter().lat * Math.PI / 180)) / Math.pow(2, map.getZoom() + 8);
      const radius = Math.max(2, Math.round(HEAT_KM * 1000 / metresPerPx / CELL_PX));
      const view = map.getBounds().pad(.3);
      const here = stations.filter(s => !s.flag && Number.isFinite(s.price_cpl) && view.contains([s.latitude, s.longitude]));
      if (here.length < 3) return;
      const weight = new Float32Array(cols * rows), sum = new Float32Array(cols * rows);
      for (const s of here) {
        const p = map.latLngToContainerPoint([s.latitude, s.longitude]);
        const cx = Math.round(p.x / CELL_PX), cy = Math.round(p.y / CELL_PX);
        for (let y = Math.max(0, cy - radius); y < Math.min(rows, cy + radius + 1); y++)
          for (let x = Math.max(0, cx - radius); x < Math.min(cols, cx + radius + 1); x++) {
            const d = Math.hypot(x - cx, y - cy) / radius;
            if (d >= 1) continue;
            const w = (1 - d) * (1 - d);
            weight[y * cols + x] += w; sum[y * cols + x] += w * s.price_cpl;
          }
      }
      const prices = here.map(s => s.price_cpl).sort((a, b) => a - b);
      const lo = prices[Math.floor(prices.length * .1)], hi = prices[Math.floor(prices.length * .9)] || lo + 1;
      const canvas = document.createElement('canvas');
      canvas.width = cols; canvas.height = rows;
      const ctx = canvas.getContext('2d'), image = ctx.createImageData(cols, rows);
      for (let i = 0; i < cols * rows; i++) {
        if (weight[i] < .05) continue;
        const [r, g, b] = rampColour((sum[i] / weight[i] - lo) / Math.max(1, hi - lo));
        image.data.set([r, g, b, Math.round(Math.min(1, weight[i]) * 150)], i * 4);
      }
      ctx.putImageData(image, 0, 0);
      overlay = L.imageOverlay(canvas.toDataURL(), map.getBounds(), {interactive: false, className: 'price-heat', pane: 'heat'}).addTo(map);
      input.parentElement.title = `Cyan ${lo.toFixed(1)} c/L or less · red ${hi.toFixed(1)} or more (10th–90th percentile of stations in view)`;
    };
    input.onchange = () => { on = input.checked; draw(); };
    map.on('moveend', draw);
    redrawHeat = draw;
  }
  let redrawHeat = () => {};

  // Live ships (AIS via this server): an arrow along the course, a dot when stopped.
  const SHIP_COLOUR = {tanker: '#ff9f43', gas: '#d58cff', cargo: '#5aa9ff', passenger: '#f2f2f2', tug: '#f5d547',
                       fishing: '#7dd36b', pleasure: '#9fb4c4', other: '#9fb4c4', unknown: '#71899c'};
  function shipLayers(map, box) {
    const tankers = L.layerGroup(), others = L.layerGroup();
    let timer = null;
    box.insertAdjacentHTML('beforeend', '<label><input type="checkbox" data-ships="tankers" checked> 🛳 Tankers (live)</label><label><input type="checkbox" data-ships="all"> 🚢 Other ships (live)</label>');
    const tankerBox = box.querySelector('[data-ships=tankers]'), allBox = box.querySelector('[data-ships=all]');
    const icon = s => {
      const kind = s.kind === 'tanker' && s.type === 84 ? 'gas' : s.kind, big = s.kind === 'tanker', size = big ? 20 : 13;
      const moving = (s.sog || 0) >= 0.5, angle = s.heading ?? s.cog ?? 0;
      return L.divIcon({className: 'ship-icon', iconSize: [size, size], html: moving
        ? `<svg viewBox="0 0 20 20" style="transform:rotate(${angle}deg)"><path d="M10 1 L17 18 L10 14 L3 18 Z" fill="${SHIP_COLOUR[kind]}" stroke="#0b141d" stroke-width="1.5"/></svg>`
        : `<svg viewBox="0 0 20 20"><circle cx="10" cy="10" r="7" fill="${SHIP_COLOUR[kind]}" stroke="#0b141d" stroke-width="2"/></svg>`});
    };
    const popup = s => `<strong>${s.kind === 'tanker' ? '🛳' : '🚢'} ${esc(s.name || 'MMSI ' + s.mmsi)}</strong><br>
      <small>${s.kind === 'tanker' ? (s.type === 84 ? 'Gas carrier (tanker, hazard cat D)' : 'Tanker') : esc(s.kind)}${s.length_m ? ` · ${s.length_m} m` : ''}${s.draught_m ? ` · draught ${s.draught_m} m` : ''}</small>
      <p>${s.sog != null ? `${s.sog.toFixed(1)} kn` : 'speed unknown'}${s.status ? ` · ${esc(s.status)}` : ''}<br>
      ${s.destination ? `Destination: <b>${esc(s.destination)}</b>` : 'No destination broadcast'}${s.eta ? ` · ETA ${esc(s.eta)} UTC (as broadcast; no year)` : ''}</p>
      <small>Position ${s.minutes_ago < 1 ? 'under a minute' : Math.round(s.minutes_ago) + ' min'} old · MMSI ${s.mmsi}${s.imo ? ` · IMO ${s.imo}` : ''} ·
      <a href="https://www.marinetraffic.com/en/ais/details/ships/mmsi:${encodeURIComponent(s.mmsi)}" target="_blank" rel="noopener">MarineTraffic ↗</a><br>AIS data: aisstream.io</small>`;
    const load = async () => {
      const want = tankerBox.checked || allBox.checked;
      if (!want) return;
      try {
        const r = await (await fetch(`/api/ships?everything=${allBox.checked}`)).json();
        tankers.clearLayers(); others.clearLayers();
        for (const s of r.ships) {
          L.marker([s.lat, s.lon], {icon: icon(s), keyboard: false, title: s.name || String(s.mmsi), zIndexOffset: s.kind === 'tanker' ? 500 : 0})
            .bindTooltip(`${s.kind === 'tanker' ? '🛳' : '🚢'} ${esc(s.name || s.mmsi)}${s.destination ? ' → ' + esc(s.destination) : ''}`)
            .bindPopup(() => popup(s), {maxWidth: 300}).addTo(s.kind === 'tanker' ? tankers : others);
        }
        const count = r.counts?.tanker ?? 0;
        tankerBox.parentElement.lastChild.textContent = r.error && !r.ships.length ? ' 🛳 Ships: ' + (r.connected ? 'waiting' : 'not connected') : ` 🛳 Tankers (live · ${count})`;
        tankerBox.parentElement.title = r.error || `${Object.values(r.counts || {}).reduce((a, b) => a + b, 0)} vessels heard in the last 3 hours. ${r.source || ''}`;
      } catch { tankerBox.parentElement.lastChild.textContent = ' 🛳 Ships unavailable'; }
    };
    const sync = () => {
      if (tankerBox.checked) tankers.addTo(map); else tankers.remove();
      if (allBox.checked) others.addTo(map); else others.remove();
      clearInterval(timer); timer = null;
      if (tankerBox.checked || allBox.checked) { load(); timer = setInterval(load, 30000); }
    };
    tankerBox.onchange = allBox.onchange = sync;
    sync();
  }

  // Fuel supply: refinery, import terminals and depots (static, researched list with sources).
  const SUPPLY = {'refinery': '#f67e83', 'import terminal': '#eab466', 'terminal/depot': '#b48cff'};
  function supply(map, box) {
    // DOM markers, not circle markers: a vector layer in its own pane gets its own full-map canvas,
    // which sat above the station canvas and swallowed every station hover and click.
    const icon = kind => L.divIcon({className: 'supply-icon', iconSize: [16, 16],
      html: `<span style="background:${SUPPLY[kind] || '#71899c'}${kind === 'refinery' ? ';width:18px;height:18px' : ''}"></span>`});
    const layer = L.layerGroup();
    let loaded = false;
    box.insertAdjacentHTML('beforeend', '<label><input type="checkbox"> 🛢 Fuel supply sites</label>');
    const label = box.lastChild;
    label.firstChild.onchange = async e => {
      if (!e.target.checked) { layer.remove(); return; }
      layer.addTo(map);
      if (loaded) return;
      try {
        const data = await (await fetch('/static/supply-sites.json')).json();
        for (const site of data.sites) {
          L.marker([site.lat, site.lng], {icon: icon(site.kind), keyboard: false, title: site.name})
            .bindTooltip(`🛢 ${esc(site.name)} · ${esc(site.kind)}`)
            .bindPopup(`<strong>🛢 ${esc(site.name)}</strong><br><small>${esc(site.operator)} · ${esc(site.kind)} · ${esc(site.locality || '')}</small>
              <p>${esc(site.note)}</p>${site.precision === 'suburb' ? '<small>Location approximate (suburb level).</small><br>' : ''}
              ${site.uncertain ? `<p><small>Uncertain: ${esc(site.uncertain)}</small></p>` : ''}
              ${(site.source_urls || []).filter(u => /^https?:\/\//.test(u)).map(u => `<a href="${esc(u)}" target="_blank" rel="noopener">Source ↗</a>`).join(' ')}`, {maxWidth: 300})
            .addTo(layer);
        }
        loaded = true;
        label.title = data.attribution || '';
      } catch { label.lastChild.textContent = ' 🛢 Supply sites unavailable'; }
    };
  }

  // EV chargers (Open Charge Map, via this server so its key stays private), for the area in view.
  function ev(map, box) {
    const layer = L.layerGroup();
    let on = false, request = 0;
    box.insertAdjacentHTML('beforeend', '<label><input type="checkbox"> ⚡ EV chargers</label>');
    const label = box.lastChild, input = label.firstChild, text = label.lastChild;
    const load = async () => {
      if (!on) return;
      const run = ++request, b = map.getBounds();
      try {
        const r = await (await fetch('/api/ev?' + new URLSearchParams({south: b.getSouth(), west: b.getWest(), north: b.getNorth(), east: b.getEast()}))).json();
        if (run !== request || !on) return;
        layer.clearLayers();
        text.textContent = r.status === 'ok' ? ` ⚡ EV chargers (${r.chargers.length})` : r.status === 'zoom_in' ? ' ⚡ EV chargers (zoom in)' : ' ⚡ EV chargers';
        label.title = r.note || r.source || '';
        if (r.status === 'not_configured') { text.textContent = ' ⚡ EV: add a free key'; return; }
        for (const c of r.chargers) {
          const fast = (c.max_kw || 0) >= 50;
          L.circleMarker([c.lat, c.lng], {radius: fast ? 6 : 4, color: '#0b141d', weight: 1, fillColor: c.operational === false ? '#71899c' : '#7dd36b', fillOpacity: .95})
            .bindTooltip(`⚡ ${esc(c.name)}${c.max_kw ? ` · up to ${c.max_kw} kW` : ''}`)
            .bindPopup(() => `<strong>⚡ ${esc(c.name)}</strong><br><small>${esc(c.address)}</small>
              <p>${esc(c.operator || 'Operator not stated')} · ${c.points ?? '?'} point${c.points === 1 ? '' : 's'}${c.operational === false ? ' · <b>not operational</b>' : ''}</p>
              <p>${c.connections.map(x => `${esc(x.type || 'Connector')}${x.kw ? ` ${x.kw} kW` : ''}${x.count ? ` ×${x.count}` : ''}`).join('<br>')}</p>
              ${c.cost ? `<p>Cost: ${esc(c.cost)}</p>` : ''}<small>Open Charge Map community data: check the operator's app before relying on it.</small>`, {maxWidth: 300})
            .addTo(layer);
        }
      } catch { text.textContent = ' ⚡ EV chargers unavailable'; }
    };
    input.onchange = () => { on = input.checked; if (on) { layer.addTo(map); load(); } else layer.remove(); };
    map.on('moveend', load);
  }

  // Queensland Fire Department incidents: warning areas as outlines, other incidents as dots.
  const FIRE = {'Emergency Warning': '#f14c4c', 'Watch and Act': '#f0883e', 'Advice': '#eab466'};
  function bushfires(map, box) {
    const layer = L.geoJSON(null, {
      style: f => ({color: FIRE[f.properties.level] || '#b0a8a0', weight: 2, fillOpacity: .12}),
      pointToLayer: (f, at) => L.circleMarker(at, {radius: 5, color: '#0b141d', weight: 1, fillColor: FIRE[f.properties.level] || '#ff8a3d', fillOpacity: .9}),
      onEachFeature: (f, l) => {
        const p = f.properties;
        l.bindTooltip(`🔥 ${esc(p.level)}${p.title ? ' · ' + esc(p.title) : ''}`);
        l.bindPopup(`<strong>🔥 ${esc(p.level)}</strong>${p.title ? `<br>${esc(p.title)}` : ''}<p>${esc(p.text)}</p><small>${esc(p.status)} · updated ${esc(p.updated)} · Queensland Fire Department. Check qfd.qld.gov.au for current advice.</small>`, {maxWidth: 320});
      },
    }).addTo(map);
    box.insertAdjacentHTML('beforeend', '<label><input type="checkbox" checked> 🔥 Bushfire incidents</label>');
    const label = box.lastChild;   // captured now: later layers append their own labels after it
    label.firstChild.onchange = e => { if (e.target.checked) layer.addTo(map); else layer.remove(); };
    fetch('/api/bushfires').then(r => r.json()).then(data => layer.addData(data)).catch(() => {
      label.lastChild.textContent = ' 🔥 Bushfires unavailable';
    });
  }
  const CAMERA_ZOOM = 12;
  function cameras(map) {
    if (!document.getElementById('map-extras-style')) document.head.insertAdjacentHTML('beforeend', `<style id="map-extras-style">${STYLE}</style>`);
    map.createPane('cameras').style.zIndex = 640;
    const layer = L.layerGroup().addTo(map), container = map.getContainer();
    // In the mixed view, ~40 Brisbane camera icons buried the stations at city zoom, so they
    // appear from street level; "Cameras only" always shows every camera.
    let mode = 'all';
    const far = () => container.classList.toggle('cameras-far', mode === 'all' && map.getZoom() < CAMERA_ZOOM);
    map.on('zoomend', far); far();
    const icon = L.divIcon({className: 'camera-icon', html: '📷', iconSize: [24, 24]});
    const control = L.control({position: 'topright'});
    control.onAdd = () => {
      const box = L.DomUtil.create('div', 'leaflet-control map-view-control');
      box.innerHTML = '<select aria-label="Map view"><option value="all">Stations + cameras (zoom in)</option><option value="cameras">📷 Cameras only</option><option value="stations">Stations only</option></select>';
      L.DomEvent.disableClickPropagation(box);
      box.firstChild.onchange = e => {
        mode = e.target.value;
        far();
        container.classList.toggle('cameras-only', mode === 'cameras');
        if (mode === 'stations') layer.remove(); else layer.addTo(map);
        const all = layer.getLayers();
        if (mode === 'cameras' && all.length) map.fitBounds(L.featureGroup(all).getBounds(), {padding: [24, 24]});
      };
      // Layer switches fold under one button: expanded on desktop, collapsed on phones so they do not cover the map.
      box.insertAdjacentHTML('beforeend', '<button type="button" class="map-layers-toggle" aria-expanded="true">Layers ▾</button><div class="map-layers"></div>');
      const toggle = box.querySelector('.map-layers-toggle'), layers = box.querySelector('.map-layers');
      const fold = open => { layers.hidden = !open; toggle.setAttribute('aria-expanded', String(open)); toggle.textContent = open ? 'Layers ▴' : 'Layers ▾'; };
      toggle.onclick = () => fold(layers.hidden);
      fold(!matchMedia('(max-width: 760px)').matches);
      L.DomEvent.disableScrollPropagation(box);
      bushfires(map, layers);
      radar(map, layers);
      heat(map, layers);
      ev(map, layers);
      supply(map, layers);
      shipLayers(map, layers);
      return box;
    };
    control.addTo(map);
    const option = () => control.getContainer().querySelector('option[value=cameras]');
    const loadCameras = () => fetch('/api/cameras').then(r => { if (!r.ok) throw new Error(r.status); return r.json(); }).then(({cameras = [], retrieved_at: when, error}) => {
      // QLDTraffic's shared key is often rate limited: say "busy" and retry, never give up for good.
      if (!cameras.length) throw new Error('busy');
      option().textContent = '📷 Cameras only'; option().disabled = false;
      option().title = error && when ? `QLDTraffic is busy; camera list saved ${new Date(when).toLocaleDateString('en-AU')}. Images are live.` : '';
      layer.clearLayers();
      for (const c of cameras) {
        const marker = L.marker([c.lat, c.lng], {icon, pane: 'cameras', title: c.description}).addTo(layer);
        marker.bindTooltip(`📷 ${esc(c.description)}`, {pane: 'cameras'});
        let timer;
        const img = () => `${esc(c.image_url)}?t=${Date.now()}`;
        marker.bindPopup(() => `<strong>${esc(c.description)}</strong><br><small>Facing ${esc(c.direction) || 'unstated'} · QLDTraffic camera · refreshes every minute</small>
          <img src="${img()}" alt="Latest still from ${esc(c.description)}" style="display:block;width:320px;max-width:100%;aspect-ratio:5/4;object-fit:cover;margin-top:6px;background:#111">`, {minWidth: 280, maxWidth: 340});
        marker.on('popupopen', e => { timer = setInterval(() => { const el = e.popup.getElement()?.querySelector('img'); if (el) el.src = img(); }, 60000); });
        marker.on('popupclose', () => clearInterval(timer));
      }
    }).catch(error => {
      option().textContent = error.message === '404' ? '📷 Cameras: restart dashboard' : '📷 Cameras: QLDTraffic busy, retrying';
      option().disabled = true;
      setTimeout(loadCameras, 5 * 60 * 1000);
    });
    loadCameras();
  }

  // Phones: one finger scrolls the page and two fingers move and zoom the map (Leaflet's pinch also
  // pans), so a full-width map cannot trap scrolling. Full screen gives one-finger dragging back.
  const TOUCH = matchMedia('(pointer: coarse)').matches;
  function touch(map) {
    const container = map.getContainer();
    const hint = L.DomUtil.create('div', 'map-hint', container);
    hint.textContent = 'Use two fingers to move the map, or ⤢ for full screen';
    let timer;
    if (TOUCH) map.dragging.disable();
    container.addEventListener('touchstart', e => {
      if (!TOUCH || container.classList.contains('map-full')) return;
      hint.classList.toggle('on', e.touches.length === 1);
      clearTimeout(timer); timer = setTimeout(() => hint.classList.remove('on'), 1400);
    }, {passive: true});
  }

  function fullScreen(map) {
    const container = map.getContainer(), control = L.control({position: 'topleft'});
    const set = on => {
      container.classList.toggle('map-full', on);
      document.documentElement.classList.toggle('map-full-open', on);   // the page underneath stops scrolling
      if (on || !TOUCH) map.dragging.enable(); else map.dragging.disable();
      button.textContent = on ? '✕' : '⤢';
      button.title = on ? 'Exit full screen' : 'Full-screen map';
      button.setAttribute('aria-label', button.title);
      map.invalidateSize({pan: false});
    };
    let button;
    control.onAdd = () => {
      const bar = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
      button = L.DomUtil.create('a', 'map-full-button', bar);
      button.href = '#'; button.setAttribute('role', 'button');
      L.DomEvent.disableClickPropagation(bar);
      L.DomEvent.on(button, 'click', e => { L.DomEvent.preventDefault(e); set(!container.classList.contains('map-full')); });
      return bar;
    };
    control.addTo(map);
    set(false);
    document.addEventListener('keydown', e => { if (e.key === 'Escape' && container.classList.contains('map-full')) set(false); });
    return () => set(false);
  }

  // Station dots share one canvas with traffic lines, fire areas and EV dots, and the canvas hands a
  // hover or click to whichever shape was drawn last. Traffic redraws every minute, so without this a
  // station on a road with roadworks stopped responding. Stations are marked with a .station property.
  function stationsOnTop(map) {
    let queued = false;
    map.on('layeradd', e => {
      if (queued || e.layer.station || !(e.layer instanceof L.Path)) return;
      queued = true;
      requestAnimationFrame(() => { queued = false; map.eachLayer(l => { if (l.station && l.bringToFront) l.bringToFront(); }); });
    });
  }

  window.FuelMap = {setStations(list) { stations = list || []; redrawHeat(); }, enhance(map, {search} = {}) {
    window.FuelMap.map = map; startView(map); pinchZoom(map); touch(map); stationsOnTop(map);
    window.FuelMap.exitFull = fullScreen(map);
    if (search) placeSearch(map, search); cameras(map);
  }};
})();
