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
    input.addEventListener('input', () => { input.setCustomValidity(''); if (input.value) input.placeholder = hint; });
    input.addEventListener('keydown', async e => {
      const q = input.value.trim();
      if (e.key !== 'Enter' || !q) return;
      e.preventDefault();
      let hit;
      try {
        const r = await fetch('https://nominatim.openstreetmap.org/search?' + new URLSearchParams({q, countrycodes: 'au', format: 'jsonv2', limit: 1}));
        [hit] = await r.json();
      } catch {}
      if (!hit) { input.setCustomValidity(`No Australian place found for “${q}”. Typing still filters stations by name.`); input.reportValidity(); return; }
      const [s, n, w, east] = hit.boundingbox.map(Number);
      map.fitBounds([[s, w], [n, east]], {maxZoom: 14});
      // Clear the name filter so every station in that area shows.
      input.value = '';
      input.dispatchEvent(new Event('input'));
      input.placeholder = `Showing ${hit.name || q} · type to filter stations`;
    });
  }

  // Official TMR road cameras, the public "live footage" for Queensland roads.
  // Cameras sit in their own pane so "Cameras only" can hide every other pane with one class.
  const STYLE = `.camera-icon{display:grid;place-items:center;font-size:13px;background:#0b141d;border:2px solid #b48cff;border-radius:50%}
    .map-view-control select{font:inherit;padding:4px 6px;background:#0b141d;color:#dce6ed;border:1px solid #b48cff;border-radius:4px}
    .cameras-only :is(.leaflet-overlay-pane,.leaflet-shadow-pane,.leaflet-marker-pane,.leaflet-tooltip-pane){display:none}
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
    box.lastChild.firstChild.onchange = e => { if (e.target.checked) layer.addTo(map); else layer.remove(); };
    fetch('/api/bushfires').then(r => r.json()).then(data => layer.addData(data)).catch(() => {
      box.lastChild.lastChild.textContent = ' 🔥 Bushfires unavailable';
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
      bushfires(map, box);
      radar(map, box);
      return box;
    };
    control.addTo(map);
    fetch('/api/cameras').then(r => { if (!r.ok) throw new Error(); return r.json(); }).then(({cameras = []}) => {
      if (!cameras.length) throw new Error();
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
    }).catch(() => {
      // Say so rather than showing an empty map: usually the dashboard needs a restart, or QLDTraffic is unreachable.
      const option = control.getContainer().querySelector('option[value=cameras]');
      option.textContent = '📷 Cameras unavailable'; option.disabled = true;
    });
  }

  window.FuelMap = {enhance(map, {search} = {}) { startView(map); pinchZoom(map); if (search) placeSearch(map, search); cameras(map); }};
})();
